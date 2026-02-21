import hashlib
from signaldeck_sdk import Processor
from typing import Dict, List

class Action:
    def __init__(self,data):
        self.value = data["value"]
        self.type = data["type"]
        self.name = data["name"]
        self.element = data["element"]
        self.cronjob_support = data.get("cronjob",False)
        self.processor : Processor = None
        if "fa" in data.keys():
            self.fa = data["fa"]
        if isinstance(self.value,str):
            self.value=[self.value]


    def supports_cronjob(self):
        return self.cronjob_support


    def getHash(self):
        valueStr = ""
        for actionVal in self.value:
            valueStr+=actionVal
        hash_object = hashlib.sha1((self.type+actionVal).encode())
        return hash_object.hexdigest()

    def isFA(self):
        return hasattr(self,"fa") and self.fa is not None
    