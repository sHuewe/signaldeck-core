# signaldeck_core/services/action_dispatcher.py
from __future__ import annotations
import threading
import logging

class ActionDispatcher:
    def __init__(self, *, logger: logging.Logger) -> None:
        self.logger = logger
        self.pending: list[threading.Thread] = []
        self.is_processing: bool = False

    def send(self, processors: dict, action_type: str, value, action_hash: str, params=None, file=None):
        params = params or {}
        return processors[action_type].process(value, action_hash, file=file, **params)

    def process_action(self, processors: dict, action, action_hash: str, params=None, file=None):
        params = params or {}
        res = None
        for action_val in action.value:
            res = self.send(processors, action.type, action_val, action_hash, params=params, file=file)
        return res if res is not None else "OK"

    def send_hash(self, processors: dict, hashes: dict, hash_val: str, params=None, file=None):
        """
        Main entrypoint for UI: executes/queues an action identified by hash.
        """
        params = params or {}
        action = hashes[hash_val]

        if not processors[action.type].must_be_queued():
            return self.process_action(processors, action, hash_val, params=params, file=file)

        thr = threading.Thread(target=self.process_action, args=(processors, action, hash_val), kwargs={"params": params, "file": file})

        if self.is_processing:
            self.pending.append(thr)
            return "PENDING"

        self.is_processing = True
        thr.start()
        thr.join()

        while self.pending:
            current = list(self.pending)
            self.pending = []
            for t in current:
                t.start()
                t.join()

        self.is_processing = False
        return "OK"