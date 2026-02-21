import json
from .action import Action
import logging
import traceback
from typing import Dict, List

class Group:
    def __init__(self,data):
        '''
        Items in group-json are actions.
        Actions contain to an element. (1 element has 1 or more actions, but 1 action only has 1 element)
        The mapping actions to element is done here.
        '''
        self.logger=logging.getLogger(__name__)
        self.filename=data["data"]
        self.name = data["name"]
        self.path = data.get("path","/")
        with open(self.filename,"r") as f:
            self.logger.info("Parse group: "+self.name)
            actions=json.load(f)
            self.actions: List[Action] = []
            for act in actions:
                self.actions.append(Action(act))
                self.logger.info(self.actions[-1].name)
        self.elements=[]
        self.actionsByElement: Dict[str, List[Action]] = {}
        self.elementByAction={}
        for a in self.actions:
            if a.element is None:
                continue
            if a.element not in self.elements:
                self.elements.append(a.element)
                self.actionsByElement[a.element]=[]
            self.actionsByElement[a.element].append(a)
            self.elementByAction[a]=a.element

    def element_supports_cronjob(self, element):
        for action in self.actionsByElement[element]:
            if action.supports_cronjob():
                return True
        return False

    def getStateForElement(self,element):
        res=""
        for action in self.actionsByElement[element]:
            try:
                res+=str(action.processor.getState(action.value,action.getHash()))
            except Exception as e:
                self.logger.error(f'Unable to render {action.processor.name} for value {action.value}')
                self.logger.error(traceback.format_exc())
        return res
