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
        
        checkpoint_timestamps = sorted([t for t, in checkpoints.keys() if t <= timestamp])
        # print(time.time()- timestamp)


        
        # if checkpoints or not origin_tuple:
        # last_checkpoint_timestamp = (sorted(c for c in checkpoints if c <= timestamp) or [0])[-1]
        # value = last_checkpoint_timestamp and checkpoints[last_checkpoint_timestamp] or None
        if checkpoint_timestamps:
            value = ujson.loads(checkpoints.get((checkpoint_timestamps[-1],)).decode())[1]
            offset = len(checkpoint_timestamps)
            last_checkpoint_timestamp = checkpoint_timestamps[-1]
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
                for mode, diff in change:
                    value = self._recursive_update(mode, value, diff)
        # print(time.time()- timestamp)
        return value
                
    def set(self, key, changes):
        timestamp = time.time()

        offset = len(self.checkpoints.get(key).keys()) if key in self.checkpoints.keys() else 0
        # print(offset)
        log = self.log.get(key, offset)
        size = log.update({timestamp: changes})

        if size > 1_000_000:
            checkpoint = self.get(key)
            self.checkpoints.get(key).set((timestamp,), ujson.dumps([self.list_versions(key), checkpoint]).encode())

        return timestamp
        # self.log[key] = log
        
        # checkpoints = self.checkpoints.get(key) or {}
        # last_checkpoint_timestamp = (sorted(c for c in checkpoints if c <= timestamp) or [0])[-1]
        
        # if len([t for t in checkpoints if t > last_checkpoint_timestamp]) > 100:
        #     checkpoints.update({timestamp: value})
        #     self.checkpoints[key] = checkpoints
    
    def _recursive_update(self, mode, old_value, diff):
        if mode[0] == 'u':
            value = old_value or {}
            if len(mode) > 1:
                diff = {key: self._recursive_update(mode[1:], value.get(key), sub_diff) 
                        for key, sub_diff in diff.items()}
            value.update(diff)
        elif mode[0] == 'r':
            value = diff
        elif mode[0] == '+':
            value = (old_value or 0) + diff
        elif mode[0] == 'l':
            value = old_value or []
        elif mode[0] == 'a':
            value = old_value or []
            value.append(diff)
        elif mode[0] == 'e':
            value = old_value or []
            value.extend(diff)
        elif mode[0] == 'm':
            value = old_value or []
            if diff in value:
                value.remove(diff)
        elif mode[0] == 'p':
            value = old_value or {}
            for key in diff:
                if key in value:
                    value.pop(key, None)
        else:
            raise Exception('update mode', mode, 'not implemented')
            
        return value

