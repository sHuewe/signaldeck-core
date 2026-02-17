from signaldeck_sdk import DataStore
import asyncio, logging
from datetime import datetime, date
from pathlib import Path
import os
import pandas as pd
import numpy as np

DEFAULT_TIME_FORMAT="%Y-%m-%dT%H:%M:%S%z"

def ensure_str(value, config):
    if isinstance(value, str):
        return value
    elif value is None:
        return ''
    elif isinstance(value, (datetime,date)):
        return value.strftime(config.get("date_format",DEFAULT_TIME_FORMAT))
    return str(value)


class CsvStore(DataStore):

#        for t in self.topicConfig:
#            creatDirIfNeeded(self.topicConfig[t].get("persist",None))

    def __init__(self,loop,config=None):
        super().__init__(loop,config)
        self.logger = logging.getLogger(__name__)
        self.cachedFiles = {}

    def getPersistFilename(self,date,config=None):
        datepattern = config.get("filename_datepattern","%Y_%m_%d")
        return date.strftime(datepattern)+".csv"

    def _save_data_sync(self,data, config):
        """
        Reine Sync-Logik: schreibt die CSV-Zeile auf die Platte.
        """
    
        try:
            directory = config.get('dir')
            if not directory:
                logging.getLogger(__name__).warning("No persist config found!")
                return
            path = Path(directory)
            path.mkdir(parents=True, exist_ok=True)
            file_path = path /  self.getPersistFilename(datetime.now(),config=config)
            file_exists = file_path.exists()

            fields = [f["name"] for f in config.get("fields", [])]
            with file_path.open('a', encoding='utf-8', newline='') as f:
                if not file_exists:
                    f.write(','.join(fields) + '\n')
                row = ','.join(ensure_str(data.get(field, ''),config) for field in fields)
                f.write(row + '\n')
        except Exception as e:
            self.logger.warning(e)


    def save(self,processor_name,data,persist_config):
        self._loop.create_task(asyncio.to_thread(self._save_data_sync, data, config=persist_config))

    
    def getDateFormat(self,config=None):
        return config.get("date_format","%d.%m.%Y %H:%M:%S")

    def getDateField(self,config):
        for field in config.get("fields",[]):
            if field.get("dtype","str") == "date":
                return field["name"]
        return "date"

    def getDf_forFile(self,askedDate,config,fileName = None):
        dirName= config["dir"]
        if fileName is None:
            fileName=os.path.join(dirName,self.getPersistFilename(askedDate,config=config)) 
        if fileName in self.cachedFiles:
            return self.cachedFiles[fileName]   
        if Path(fileName).exists():
            dateField= self.getDateField(config)
            dateFormat = self.getDateFormat(config=config)
            df =pd.read_csv(fileName, parse_dates=True)
            df[dateField]=pd.to_datetime(df[dateField],format=dateFormat).apply(lambda x: self.assure_tz_aware(x,config))
        else:
            return None      
        if askedDate.date() < datetime.today().date():
            self.cachedFiles[fileName] = df        
        return df
    
    def get_first_from_day(self,processor_name,fieldName,askedDate,config):
        res = self.getDf_forFile(askedDate,config)
        if res is None:
            return None
        res=res[fieldName].dropna().iloc[0]
        return res
    
    def get_last_from_day(self,processor_name,fieldName,askedDate,config):
        res = self.getDf_forFile(askedDate,config)
        if res is None:
            return None
        res=res[fieldName].dropna().iloc[-1]
        return res

    def get_full_day(self,processor_name,fieldName,askedDate,config):
        res = self.getDf_forFile(askedDate,config)
        if res is None:
            return None
        res=res.set_index(self.getDateField(config))[fieldName]
        return res
    
    def get_best_value(self,processor_name,fieldName,askedDate,config):
        res = self.getDf_forFile(askedDate,config)
        if res is None:
            return None
        dateField= self.getDateField(config)
        res= res[[dateField,fieldName]].dropna()
        res["diff"]=np.abs(res[dateField]-self.assure_tz_aware(askedDate,config))
        return res.loc[res["diff"].idxmin()][fieldName]
    
    def getDir(self,config):
        return config.get("dir","")
    
    def getFullDf(self,filePath,config):
        return self.getDf_forFile(datetime.now(),config,fileName=filePath)
    
    def get_current_vals(self,processor_name,config):
        return None