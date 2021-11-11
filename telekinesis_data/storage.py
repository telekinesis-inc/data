import bson
import os
import telekinesis as tk
from glob import glob
import shutil
import re

class Storage:
    def __init__(self, session, prefix=''):
        if len(prefix) < 2 or prefix[:2] != './':
            prefix = './' + prefix
        if re.findall(r'^\.\.\/|\/\.\.\/', prefix):
            raise PermissionError
        
        self._prefix = prefix
        
        self._session = session
        if not os.path.exists(prefix):
            os.makedirs(prefix)
        
    async def get(self, key, default=None):
        try:
            if await self.is_path(key):
                return await self.create_child(key)
            with open(os.path.join(self._prefix, key), 'rb') as f:
                return tk.Telekinesis(None, self._session)._decode(bson.loads(f.read()))
        except FileNotFoundError:
            return default

    @tk.block_arg_evaluation
    async def set(self, key, value):
        if re.findall(r'^\.\.\/|\/\.\.\/', key):
            raise PermissionError
        if isinstance(value, tk.Telekinesis):
            value._block_gc = True
        if isinstance(value, dict):
            for val in value.values():
                if isinstance(val, tk.Telekinesis):
                    val._block_gc = True
        path = os.path.join(self._prefix, key)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'wb') as f:
            f.write(bson.dumps(tk.Telekinesis(None, self._session, block_gc=True)._encode(value)))

    @tk.block_arg_evaluation
    async def set_attribute(self, key, attr, value):
        if re.findall(r'^\.\.\/|\/\.\.\/', key):
            raise PermissionError
        if isinstance(value, tk.Telekinesis):
            value._block_gc = True
        obj = await self.get(key) or {}
        obj[attr] = value
        with open(os.path.join(self._prefix, key), 'wb') as f:
            f.write(bson.dumps(tk.Telekinesis(None, self._session, block_gc=True)._encode(obj)))

    @tk.block_arg_evaluation
    async def set_append(self, key, value):
        if re.findall(r'^\.\.\/|\/\.\.\/', key):
            raise PermissionError
        if isinstance(value, tk.Telekinesis):
            value._block_gc = True
        lst = await self.get(key) or []
        lst.append(value)
        with open(os.path.join(self._prefix, key), 'wb') as f:
            f.write(bson.dumps(tk.Telekinesis(None, self._session, block_gc=True)._encode(lst)))

    async def list(self, pattern='*'):
        if re.findall(r'^\.\.\/|\/\.\.\/', pattern):
            raise PermissionError
        return [os.path.join(*(p.split('/')[len(self._prefix.split('/')):])) 
                for p in glob(os.path.join(self._prefix, pattern))]

    async def remove(self, pattern):
        [os.remove(os.path.join(self._prefix, p)) if os.path.isfile(os.path.join(self._prefix, p))
         else shutil.rmtree(os.path.join(self._prefix, p)) for p in await self.list(pattern)]
        
    async def is_path(self, key):
        if re.findall(r'^\.\.\/|\/\.\.\/', key):
            raise PermissionError
        return os.path.isdir(os.path.join(self._prefix, key))

    async def create_child(self, subprefix):
        return Storage(self._session, os.path.join(self._prefix, subprefix))
    
    async def getmtime(self, key):
        if re.findall(r'^\.\.\/|\/\.\.\/', key):
            raise PermissionError
        try:
            return os.path.getmtime(os.path.join(self._prefix, key))
        except FileNotFoundError:
            return FileNotFoundError