import os
import time
import ujson
from .storage import StreamContainer, SimpleKVContainer

class TimetravelerKV:
    def __init__(self, path):
        self.checkpoints = SimpleKVContainer(os.path.join(path, 'checkpoints'))
        self.log = StreamContainer(path)
    
    def list_versions(self, key, timestamp=None):
        # timestamp = timestamp or time.time()
        # ignore_fields = ignore_fields or []
        # return [t for t, i in self.log.get(key) if t < timestamp 
        #         and not all(['u' in x[0] and all(y in ignore_fields for y in x[1]) for x in i])]
        timestamp = timestamp or time.time()
        
        checkpoints = self.checkpoints.get(key)
        
        checkpoints_timestamps_tuples = [t for t in checkpoints.keys() if t[0] <= timestamp]
        
        if checkpoints_timestamps_tuples:
            checkpoint_versions = ujson.loads(checkpoints.get(checkpoints_timestamps_tuples[-1]).decode())[0]
            offset = len(checkpoints_timestamps_tuples)
            last_checkpoint_timestamp = checkpoints_timestamps_tuples[-1][0]
        else:
            checkpoint_versions = []
            offset = 0
            last_checkpoint_timestamp = 0
        
        new_versions = [t for t, _ in self.log.get(key, offset) if last_checkpoint_timestamp < t <= timestamp]
        return checkpoint_versions + new_versions

    def list(self):
        return list(self.log.keys())

    def get(self, key, timestamp=None):
        timestamp = timestamp or time.time()
        
        # if origin_tuple and timestamp <= origin_tuple[0]:
        #     return origin_tuple[1](timestamp)

        checkpoints = self.checkpoints.get(key)
        # print(time.time()- timestamp)
        
        checkpoints_timestamps_tuples = [t for t in checkpoints.keys() if t[0] <= timestamp]
        # print(time.time()- timestamp)


        
        # if checkpoints or not origin_tuple:
        # last_checkpoint_timestamp = (sorted(c for c in checkpoints if c <= timestamp) or [0])[-1]
        # value = last_checkpoint_timestamp and checkpoints[last_checkpoint_timestamp] or None
        if checkpoints_timestamps_tuples:
            value = ujson.loads(checkpoints.get(checkpoints_timestamps_tuples[-1]).decode())[1]
            offset = len(checkpoints_timestamps_tuples)
            last_checkpoint_timestamp = checkpoints_timestamps_tuples[-1][0]
        else:
            value = None
            offset = 0
            last_checkpoint_timestamp = 0
        # print(time.time()- timestamp)
        # else:
        #     last_checkpoint_timestamp = origin_tuple[0]
        #     value = await origin_tuple[1](last_checkpoint_timestamp)
        #     checkpoints.update({last_checkpoint_timestamp: value})
        #     self.checkpoints[key] = checkpoints


        changes = self.log.get(key, offset)
        for t, change in changes:
            if last_checkpoint_timestamp < t <= timestamp:
                for mode, val in change:
                    if mode == 'd':
                        value = None
                    if mode == 'r':
                        value = val
                    if mode == 'u':
                        value = value or {}
                        value.update(val)
                    if mode == 'l':
                        value = value or []
                    if mode == 'a':
                        value = value or []
                        value.append(val)
                    if mode == 'ua':
                        value = value or {}
                        attr = list(val.keys())[0]
                        value.update({attr: (value.get(attr) or []) + [val[attr]]})
                    if mode == 'ur':
                        value = value or {}
                        attr = list(val.keys())[0]
                        subval = value.get(attr) or []
                        if val[attr] in subval:
                            subval.remove(val[attr])
                        value.update({attr: subval})
                    if mode == 'uu':
                        value = value or {}
                        for attr in val:
                            subvalue = value.get(attr, {})
                            subval = val[attr]
                            for subattr in subval:
                                new_value = subval[subattr]
                                subvalue.update({subattr: new_value})
                            value.update({attr: subvalue})
                    if mode == 'uuu':
                        value = value or {}
                        for attr in val:
                            subvalue = value.get(attr, {})
                            subval = val[attr]
                            for subattr in subval:
                                subsubvalue = subvalue.get(subattr, {})
                                subsubval = subval[subattr]
                                for subsubattr in subsubval:
                                    subsubvalue.update({subsubattr: subsubval[subsubattr]})
                                subvalue.update({subattr: subsubvalue})
                            value.update({attr: subvalue})
                    if mode == 'uup':
                        value = value or {}
                        for attr in val:
                            subvalue = value.get(attr, {})
                            subval = val[attr]
                            for subattr in subval:
                                subsubvalue = subvalue.get(subattr, {})
                                subsubval = subval[subattr]
                                for subsubattr in subsubval:
                                    subsubvalue.pop(subsubattr, None)
                                subvalue.update({subattr: subsubvalue})
                            value.update({attr: subvalue})
                    if mode == 'uu+':
                        value = value or {}
                        for attr in val:
                            subvalue = value.get(attr, {})
                            subval = val[attr]
                            for subattr in subval:
                                new_value = subvalue.get(subattr, 0) + subval[subattr]
                                subvalue.update({subattr: new_value})
                            value.update({attr: subvalue})
                    if mode == 'uu0':
                        value = value or {}
                        for attr in val:
                            subvalue = value.get(attr, {})
                            subval = val[attr]
                            for subattr in subval:
                                new_value = 0
                                subvalue.update({subattr: new_value})
                            value.update({attr: subvalue})
        # print(time.time()- timestamp)
        return value
                
    def set(self, key, changes):
        timestamp = time.time()

        offset = len(self.checkpoints.get(key).keys()) if key in self.checkpoints.keys() else 0
        # print(offset)
        log = self.log.get(key, offset)
        size = log.update({timestamp: changes})

        if size > 1_000_000:
            self.checkpoints.get(key).set((timestamp,), ujson.dumps([self.list_versions(key), self.get(key)]).encode())

        return timestamp
        # self.log[key] = log
        
        # checkpoints = self.checkpoints.get(key) or {}
        # last_checkpoint_timestamp = (sorted(c for c in checkpoints if c <= timestamp) or [0])[-1]
        
        # if len([t for t in checkpoints if t > last_checkpoint_timestamp]) > 100:
        #     checkpoints.update({timestamp: value})
        #     self.checkpoints[key] = checkpoints
