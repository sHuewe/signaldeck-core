import json
import logging
import threading
import asyncio
from concurrent.futures import ThreadPoolExecutor
from crontab import CronTab
from signaldeck_sdk import ApplicationContext
from signaldeck_sdk import Processor
from .group import group
from signaldeck_sdk import ValueProvider
from signaldeck_sdk import Cmd, Command
from signaldeck_sdk import PersistData, DataStore
from typing import Dict, List
from .context_impl import build_application_context
import importlib

class WaitForValue(Command):
    def __init__(self,vP:ValueProvider,sleep=20):
        self.vP=vP
        self.sleep=sleep
        self.logger= logging.getLogger("waitForValue")
        super().__init__("waitFor","Waits for condition")

    def matches(self,curVal, operator, targetValue):
        if curVal is None:
            return False
        curVal = float(curVal)
        targetValue= float(targetValue)
    
        if operator == "=":
            return curVal == targetValue
        if operator == ">":
            return curVal > targetValue
        if operator == ">=":
            return curVal >= targetValue
        if operator == "<":
            return curVal < targetValue
        if operator == "<=":
            return curVal <= targetValue

    async def run(self,fieldName: str,operator: str, value: float, cmdRes=None,stopEvent=None):
        self.logger.info(f'Wait for {fieldName} {operator} {value}')
        if cmdRes is not None:
            cmdRes.appendState(self,msg=f'Wait for {fieldName} {operator} {value}')
        curValue=self.vP.getValue(fieldName)
        while not self.matches(curValue,operator,value) and not stopEvent.is_set():
            await asyncio.sleep(self.sleep)
            curValue=self.vP.getValue(fieldName)
            self.logger.debug(f'Check value: {curValue}')
        if stopEvent.is_set():
            cmdRes.appendState(self,msg=f'Stop erkannt')
            return
        if cmdRes is not None:
            cmdRes.appendState(self,msg=f'Matched condition! {fieldName}={curValue}')
        self.logger.info(f'Matched condition! {fieldName}={curValue}')

def classFromName(classPath):
  className=classPath.split(".")[-1]
  path=classPath[:-(len(className)+1)]
  mod=__import__(path,fromlist=[className])
  return getattr(mod,className)

def getProcessorInstances(trans,ctx: ApplicationContext, valueProvider: ValueProvider,cmd: Cmd, dataStores:Dict[str,DataStore],logger, collect_data):
    res = {}
    for t in trans:
        if t.get("skip", False):
            continue
        cl = classFromName(t["class"])
        res[t["name"]]=cl(t["name"],t["config"],valueProvider,collect_data).withClassName(t["class"])
        res[t["name"]].init(ctx)
        res[t["name"]].registerCommands(cmd)
        if  isinstance(res[t["name"]],PersistData):
            logger.info(f'Register data stores for processor {t["name"]}')
            res[t["name"]].registerDataStores(dataStores)
            res[t["name"]].init_current_vals()
    for dataStoreName in dataStores.keys():
        logger.info(f'Fields from {dataStoreName}: {dataStores[dataStoreName].get_fields()}')
    return res

def getGroups(els):
    res = []
    for el in els:
        res.append(group(el))
    return res


class Manager:
    def __init__(self,app,configPath,collect_data=True):
        self.logger=logging.getLogger(__name__)
        data= {}
        self.pending=[]
        self.isProcessing = False
        self.configPath=configPath
        self.initAsync()
        self.parseConfig(collect_data)
        self.use_bluebrints(app)
        

    def use_bluebrints(self,app):
        alreadyHandled = set()
        for name, processor in self.processor.items():
            pluginName = processor.className.split(".")[0]
            if pluginName not in alreadyHandled:
                try:
                    plugin_mod_name = f"{pluginName}.plugin"
                    plugin_mod = importlib.import_module(plugin_mod_name)
                    plugin_mod.register(app)
                except ModuleNotFoundError as e:
                    # Plugin has no plugin.py or isn't installed
                    raise RuntimeError(
                        f"Plugin '{pluginName}' must provide '{plugin_mod_name}' (plugin.py) "
                        f"and be installed in the environment."
                    ) from e
                alreadyHandled.add(pluginName)

    def initAsync(self):
        self._loop = asyncio.new_event_loop()
        self._loop_ready = threading.Event()
        executor = ThreadPoolExecutor(max_workers=1)
        self._loop.set_default_executor(executor)
        self._thread = threading.Thread(
            target=self._run_loop, 
            daemon=True,
            name="AsyncioLoopThread"
        )
        self._thread.start()
        self._loop_ready.wait()


    def _run_loop(self):
        asyncio.set_event_loop(self._loop)
        self._loop.call_soon(self._loop_ready.set)
        self._loop.run_forever()

    def shutdown(self):
        tasks = [t for t in asyncio.all_tasks(self._loop) if not t.done()]
        for t in tasks: t.cancel()
        self.logger.info(f'Cancelled {len(tasks)} tasks')
       # self._loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True))
        # 2) Loop stoppen & schließen
        self._loop.call_soon_threadsafe(self._loop.stop)
        self.logger.info(f'Shutdown {len(self.processor.keys())} processors')
        for p in  self.processor.keys():
            self.logger.info(f'Shutdown {p}')
            self.processor[p].shutdown()

    def parseConfig(self,collect_data):
        with open(self.configPath,"r") as f:
            data = json.load(f)
        self.pageTitle= data.get("page_title","Home Control")
        self.valueProvider= ValueProvider()
        self.valueProvider.loop=self._loop
        self.cmd= Cmd(self._loop)
        self.cmd.registerCmd(WaitForValue(self.valueProvider))
        self.cmd.registerScripts(data.get("cmd",{}).get("script",[]))
        self.cmd.registerAliase(data.get("cmd",{}).get("alias",[]))
        self.dataStore=self._getDataStoresFromConfig(data)
        self.ctx= build_application_context(values=self.valueProvider,logger=self.logger)
        self.processor: Dict[Processor] = getProcessorInstances(data["processors"],self.ctx,self.valueProvider,self.cmd, self.dataStore,self.logger,collect_data)
        self.groups = getGroups(data["groups"])
        #Hash -> Action
        self.hashes = {}
        #Hash -> Group
        self.groupFromHash={}
        self.path={}
        for group in self.groups:
            if group.path not in self.path:
                self.path[group.path]=[]
            self.path[group.path].append(group)
            for action in group.actions:
                hashVal= action.getHash()
                action.processor = self.processor[action.type]
                self.hashes[hashVal]=action
                self.groupFromHash[hashVal]=group
        #Collect async tasks
        self.tasks: List[asyncio.Future]=[]
        for p in  self.processor.keys():
            self.tasks.extend(self.processor[p].get_asyncio_tasks(collect_data))
        for t in self.tasks:
            self._loop.call_soon_threadsafe(self._loop.create_task, t)
            self.logger.info(f'Started task: {t.__name__}')

    def _getDataStoresFromConfig(self,data) -> Dict[str,DataStore]:
        res = {}
        for storeConfig in data.get("data_stores",[]):
            res[storeConfig["name"]]=classFromName(storeConfig["class"])(self._loop,**storeConfig.get("config",{}))
        return res


    def getGroupsForPath(self, p):
        return self.path.get(p,[])  
    
    def getTitleForPath(self, p):
        
        if p == "/":
            return self.pageTitle
        return self.pageTitle + " - " + p.strip("/").replace("/"," - ")
    
    def getAvailablePaths(self):
        res = []
        for p in self.path.keys():
            if p == "/":
                res.append((p,"Home"))
            else:
                res.append(("/"+p,p))
        return res


    def reinit(self):
        self.shutdown()
        self.initAsync()
        self.parseConfig()
    
 
    def send(self,action,value,actionHash,params={},file=None):
        return self.processor[action].process(value,actionHash,file=file,**params)

    def sendHash(self,hashVal,params={},file=None):
        '''
        Entrypoint
        '''
        action = self.hashes[hashVal]
        if not self.processor[action.type].must_be_queued():
            return self.processAction(action,hashVal,params=params, file=file)
        thr = threading.Thread(target= self.processAction, args=(action,hashVal))
        if self.isProcessing:
            self.pending.append(thr)
            return "PENDING"
        self.isProcessing=True
        thr.start()
        thr.join()
        while len(self.pending)>0:
            currentPending = list(self.pending)
            self.pending=[]
            for pendinThr in currentPending:
                pendinThr.start()
                pendinThr.join()
        self.isProcessing=False
        return "OK"

    def processAction(self,action,actionHash,params={},file=None):
        res=None
        for actionVal in action.value:
            res= self.send(action.type,actionVal,actionHash,params=params,file=file)
        if res is None:
            res="OK"
        return res

    def getCronsForActions(self,actions):
        cron = CronTab(user=True)
        res=[]
        for act in actions:
            self.logger.info(f'Search jobs for action {act}')
            jobsI = cron.find_comment(act.getHash())
            jobs=[]
            for j in jobsI:
                jobs.append(j)
            self.logger.info(f'JOBS: {jobs}')
            if len(jobs) > 1:
                self.logger.warning(f'There are multiple cronjobs for action {act.getHash()}')
            if len(jobs) == 0:
                res.append("")
                continue
            sl=[str(s) for s in jobs[0].slices]
            res.append(" ".join(sl))
            
        return res

    def setCronJob(self,action,crondef):
        cron = CronTab(user=True)
        cron.remove_all(comment=action)
        job = cron.new(command=f'curl localhost:5000/run?actionhash={action}',comment=action)
        setTime = True
        try:
            job.setall(crondef)
        except:
            setTime = False

        if crondef is None or crondef=="" or not setTime or not job.is_valid():
            self.logger.info(f'Disable crontab for action {action}')
            cron.remove_all(comment=action)
            cron.write_to_user( user=True )
            return
        self.logger.info(f'Set crontab for action {action}: {crondef}')
        cron.write_to_user( user=True )