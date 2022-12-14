import ujson
import base64
import hashlib
import bson
import os

class SimpleFileContainer:
    def __init__(self, path):
        self._path = path
        self._keys = Stream(os.path.join(path, 'plainkeys'))
        if not os.path.exists(path):
            os.makedirs(path)
        
    def get(self, key):
        with open(os.path.join(self._path, key), 'rb') as f:
            return f.read()
    def set(self, key, value):
        self._keys.update({key: None})
        with open(os.path.join(self._path, key), 'wb') as f:
            return f.write(value)
    def keys(self):
        return list(k for k, _ in self._keys)
    
    def __contains__(self, key):
        return os.path.exists(os.path.join(self._path, key))
    
class SimpleKV:
    def __init__(self, path):
        self._keyencs = Stream(os.path.join(path, 'keys'))
        self._data = SimpleFileContainer(path)
    def set(self, key, value):
        if not isinstance(key, str):
            keyenc = self._hash(ujson.dumps(key, escape_forward_slashes=False).encode())
        else:
            keyenc = key
        
        self._keyencs.update({keyenc: key})
        return self._data.set(keyenc, bson.dumps({'value': value}))

    def get(self, key):
        if not isinstance(key, str):
            keyenc = self._hash(ujson.dumps(key, escape_forward_slashes=False).encode())
        else:
            keyenc = key
        if keyenc in self._data:
            return bson.loads(self._data.get(keyenc))['value']
    
    def keys(self):
        return list(set(tuple(v) for _, v in self._keyencs))
        
    def _hash(self, data):
        return base64.b64encode(hashlib.blake2s(data).digest(), b'_-')[:-1].decode()

class SimpleKVContainer:
    def __init__(self, path):
        self._path = path
        self._data = {}
        self._keyencs = Stream(os.path.join(path, 'keys1'))
        if not os.path.exists(path):
            os.makedirs(path)

    def get(self, key):
        if not isinstance(key, str):
            keyenc = self._hash(ujson.dumps(key, escape_forward_slashes=False).encode())
        else:
            keyenc = key
        
        if item := self._data.get(keyenc):
            return item

        item = SimpleKV(os.path.join(self._path, keyenc))
        self._keyencs.update({keyenc: key})
        self._data[keyenc] = item
        return item
    
    def keys(self):
        return list(tuple(v) for _, v in self._keyencs)
        
    def _hash(self, data):
        return base64.b64encode(hashlib.blake2s(data).digest(), b'_-')[:-1].decode()
class StreamContainer:
    def __init__(self, path):
        self._path = path
        # self._data = {}
        self._keyencs = Stream(os.path.join(path, 'keys'))
        if not os.path.exists(path):
            os.makedirs(path)

    def get(self, key, offset=0):
        if not isinstance(key, str):
            keyenc = self._hash(ujson.dumps(key, escape_forward_slashes=False).encode())
        else:
            keyenc = key
        
        # if item := self._data.get(keyenc):
        #     return item
        item = Stream(os.path.join(self._path, keyenc), offset)
        self._keyencs.update({keyenc: key})
        # self._data[keyenc] = item
        return item
    
    def keys(self):
        return list(v for _, v in self._keyencs)
        
    def _hash(self, data):
        return base64.b64encode(hashlib.blake2s(data).digest(), b'_-')[:-1].decode()

class Stream:
    def __init__(self, path, offset=0):
        self._path = path
        self._offset = offset
        # print('', offset)

    def update(self, data):
        path = self._path + (f'_{self._offset}' if self._offset else '')
        # print(path)
        with open(path, 'a') as f:
            for k, v in data.items():
                f.write(ujson.dumps([k, v], escape_forward_slashes=False) + '\n')
        return os.path.getsize(path)
    
    def _open_file(self):
        path = self._path + (f'_{self._offset}' if self._offset else '')
        self._file = os.path.exists(path) and open(path, 'r')
        return self

    def __iter__(self):
        self._open_file()
        return self

    def __next__(self):
        if content := self._file and self._file.readline():
            return tuple(ujson.loads(content))
        else:
            self._file and self._file.close()
            self._offset += 1
            self._open_file()
            if content := self._file and self._file.readline():
                return tuple(ujson.loads(content))
            self._offset -= 1
            raise StopIteration
        