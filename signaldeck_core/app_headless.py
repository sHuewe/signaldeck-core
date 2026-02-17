import logging.config
import json
with open("logging_config_i.json", 'r') as logging_configuration_file:
    config_dict = json.load(logging_configuration_file)
    logging.config.dictConfig(config_dict)


import numpy as np
import pandas as pd
import glob
import os
from time import sleep

from models.manager import manager


config_path = "config/strom_detail.json"



m = manager(config_path,collect_data=True)


def close():
    m.shutdown()
    print("Shutdown complete")
    exit(1)


#mqtt.hist(topic_strom,"total_out",days=1,fullDay=True)
#close()