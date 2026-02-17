import logging.config
import json
with open("logging_config_i.json", 'r') as logging_configuration_file:
    config_dict = json.load(logging_configuration_file)
    logging.config.dictConfig(config_dict)


import numpy as np
import pandas as pd
import glob
import os, time
from datetime import datetime
from models.manager import manager


config_path_csv = "config/haus_csv.json"
config_path_sqlite = "config/haus.json"

logger = logging.getLogger("MigrationScript")

houseManager_csv = manager(config_path_csv,collect_data=False)
houseManager_sqlite = manager(config_path_sqlite,collect_data=False)

topic_strom = "tele/strom/SENSOR"
topic_strom_shelly="shellypro3em-strom/status/em:0"
processors = ["strom","strom_shelly","goodwe","zappi","venus_power","venus_soc"]

csv_store = houseManager_csv.processor["mqtt_subscriber"].dataStores["csv"]
sqlite_store = houseManager_sqlite.processor["mqtt_subscriber"].dataStores["sqlite"]

csv_configs = {} 
sqlite_configs={} 
csv_configs["strom"] =houseManager_csv.processor["mqtt_subscriber"].topicConfig[topic_strom]
sqlite_configs["strom"]= houseManager_sqlite.processor["mqtt_subscriber"].topicConfig[topic_strom]
csv_configs["strom_shelly"] =houseManager_csv.processor["mqtt_subscriber"].topicConfig[topic_strom_shelly]
sqlite_configs["strom_shelly"]= houseManager_sqlite.processor["mqtt_subscriber"].topicConfig[topic_strom_shelly]
for p in processors[2:]:
    csv_configs[p] =houseManager_csv.processor[p].config
    sqlite_configs[p]= houseManager_sqlite.processor[p].config


print("migrate(csv_configs[topic_strom],sqlite_configs[topic_strom])")

def getDateField(config):
    for field in config["persist"][0]["fields"]:
        if field["dtype"] == "date":
            return field["name"]

def removeUnknownFields(record,config):
    fields = [f["name"] for f in config["fields"]]
    return {k: v for k, v in record.items() if k in fields}

def migrate(source_config,dest_config,prefix):
    p = csv_store.getDir(source_config['persist'][0])
    count_files = 0
    count_records = 0
    dateField = getDateField(source_config)
    for filePath in list_csv_files(p,prefix):
        count_files+=1
        data = csv_store.getFullDf(filePath,source_config['persist'][0])
        data_bad = data[pd.isna(data[dateField])].copy()
        if len(data_bad) > 0:
            for record in data_bad.to_dict(orient="records"):
                logger.warning(f'Invalid date in record: {record} from file {filePath}')
        data = data[pd.notna(data[dateField])].copy()
        count_records+=len(data)
        for record in data.to_dict(orient="records"):
            if not isinstance(record[dateField], datetime):
                logger.warning(f'Skip record.. No date available for {record} in {filePath}')
                continue
            sqlite_store.save(dest_config["processor_name"],removeUnknownFields(record,dest_config["persist"][0]),dest_config['persist'][0])
    print(f'Total: {count_files} files with {count_records} records submitted to sqlite-store')
    while sqlite_store.isProcessing:
        time.sleep(10)

    

def list_csv_files(folder_path: str, prefix=""):
    """Gibt eine Liste aller CSV-Dateinamen (ohne Pfad) im angegebenen Ordner zurück."""
    pattern = os.path.join(folder_path, f'{prefix}*.csv')
    return [os.path.join(folder_path,os.path.basename(f)) for f in glob.glob(pattern)]

def close():
    houseManager_csv.shutdown()
    houseManager_sqlite.shutdown()
    print("Shutdown complete")
    exit(1)

file_prefix={}
file_prefix["zappi"]=""

for p in processors[::-1]:
    print(f'Start migration for {p}')
    migrate(csv_configs.get(p),sqlite_configs.get(p),file_prefix.get(p,"2025_10_2"))