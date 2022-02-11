import time
from .storage import Container

class TimetravelerKV:
    def __init__(self, path):
        # self.checkpoints = {}
        self.log = Container(path)
    
    def list_versions(self, key, timestamp=None, ignore_fields=None):
        timestamp = timestamp or time.time()
        ignore_fields = ignore_fields or []
        return [t for t, i in self.log.get(key) if t < timestamp 
                and not all(['u' in x[0] and all(y in ignore_fields for y in x[1]) for x in i])]

    def list(self):
        return list(self.log.keys())

    def get(self, key, timestamp=None):
        timestamp = timestamp or time.time()
        
        # if origin_tuple and timestamp <= origin_tuple[0]:
        #     return origin_tuple[1](timestamp)

        # checkpoints = self.checkpoints.get(key) or {}
        
        # if checkpoints or not origin_tuple:
        # last_checkpoint_timestamp = (sorted(c for c in checkpoints if c <= timestamp) or [0])[-1]
        # value = last_checkpoint_timestamp and checkpoints[last_checkpoint_timestamp] or None
        value = None
        # else:
        #     last_checkpoint_timestamp = origin_tuple[0]
        #     value = await origin_tuple[1](last_checkpoint_timestamp)
        #     checkpoints.update({last_checkpoint_timestamp: value})
        #     self.checkpoints[key] = checkpoints

        last_checkpoint_timestamp = 0

        changes = self.log.get(key)
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
        return value
                
    def set(self, key, changes):
        timestamp = time.time()
        log = self.log.get(key)
        log.update({timestamp: changes})
        # self.log[key] = log
        
        # checkpoints = self.checkpoints.get(key) or {}
        # last_checkpoint_timestamp = (sorted(c for c in checkpoints if c <= timestamp) or [0])[-1]
        
        # if len([t for t in checkpoints if t > last_checkpoint_timestamp]) > 100:
        #     checkpoints.update({timestamp: value})
        #     self.checkpoints[key] = checkpoints
