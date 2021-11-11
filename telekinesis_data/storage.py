import os
import hashlib
import base64
import time

import bson
import ujson
import telekinesis as tk

class Storage:
    def __init__(self, session, path='', root=None, branch='main'):
        if len(path) < 2 or path[:2] != './':
            path = './' + path
        
        self._path = path
        self._root = root or ''
        self.branch = branch
        
        self._session = session
        if not os.path.exists(path+'/data'):
            os.makedirs(path+'/data')
            os.makedirs(path+'/meta')
        
        root_path = path+'/meta/'+self._hash(root.encode())
        if not os.path.exists(root_path):
            os.makedirs(root_path)

    async def get_metadata(self, key, branch=None, timestamp=None):
        return (await self._get_all_metadata(key, branch, timestamp)).get('user_metadata')

    async def get_tuple(self, key, default=None, branch=None, timestamp=None):
        metadata = None
        data = default
        try:
            if all_metadata := await self._get_all_metadata(key, branch, timestamp):
                metadata = all_metadata.get('user_metadata')
                if data_key := all_metadata.get('data'):
                    with open(os.path.join(self._path, 'data', data_key), 'rb') as f:
                        data = tk.Telekinesis(None, self._session)._decode(bson.loads(f.read()))
        except FileNotFoundError:
            pass

        return metadata, data

    async def get(self, key, default=None, branch=None, timestamp=None):
        return (await self.get_tuple(key, default, branch, timestamp))[1]

    async def get_child(self, root, branch=None):
        return Storage(self._session, self._path, self._root + (root and '/') + root, branch or self.branch)

    async def exists(self, key):
        return os.path.exists(os.path.join(self._path, 'meta', self._hash(self.root + (key and '/')+key))) 

    async def getmtime(self, key, branch=None, timestamp=None):
        return self._get_all_metadata(key, branch, timestamp)['timestamp']

    @tk.block_arg_evaluation
    async def set(self, key, value=None, metadata=None, branch=None, clear=False):
        if isinstance(value, tk.Telekinesis):
            value._block_gc = True
        hsh = self._hash((self._root+'/'+key).encode())

        old_all_metadata = await self._get_all_metadata(key, branch)
        old_user_metadata = ((not clear and (old_all_metadata or {})) or {}).get('user_metadata') or {}
        combined_user_metadata = old_user_metadata.copy()
        combined_user_metadata.update(metadata or {})

        path_meta = os.path.join(self._path, 'meta', hsh, branch or self.branch)
        os.makedirs(os.path.dirname(path_meta), exist_ok=True)

        all_metadata = {
            'user_metadata': combined_user_metadata,
            'timestamp': time.time()
        }
        
        if value is not None or clear or not old_all_metadata:
            encoded_data = bson.dumps(tk.Telekinesis(None, self._session, block_gc=True)._encode(value))
            value_hsh = self._hash(encoded_data)
            path_data = os.path.join(self._path, 'data', value_hsh)
            if not os.path.exists(path_data):
                with open(path_data+'_', 'wb') as f:
                    f.write(encoded_data)
                os.rename(path_data+'_', path_data)
            all_metadata['data'] = value_hsh
        else:
            all_metadata['data'] = old_all_metadata['data']

        with open(path_meta + '_', 'w') as f:
            ujson.dump(all_metadata, f, escape_forward_slashes=False)
        os.rename(path_meta+'_', path_meta)

    async def _get_all_metadata(self, key, branch=None, timestamp=None):
        hsh = self._hash((self._root+(key and '/')+key).encode())
        metadata = None
        try:
            path_meta = os.path.join(self._path, 'meta', hsh, branch or self.branch)
            with open(path_meta, 'rb') as f:
                return ujson.load(f)

        except FileNotFoundError:
            return metadata

    def _hash(self, data):
        return base64.b64encode(hashlib.blake2s(data).digest(), b'_-')[:-1].decode()

    # @tk.block_arg_evaluation
    # async def set_attribute(self, key, attr, value):
    #     if isinstance(value, tk.Telekinesis):
    #         value._block_gc = True
    #     obj = await self.get(key) or {}
    #     obj[attr] = value
    #     with open(os.path.join(self._path, key), 'wb') as f:
    #         f.write(bson.dumps(tk.Telekinesis(None, self._session, block_gc=True)._encode(obj)))

    # @tk.block_arg_evaluation
    # async def set_append(self, key, value):
    #     if isinstance(value, tk.Telekinesis):
    #         value._block_gc = True
    #     lst = await self.get(key) or []
    #     lst.append(value)
    #     with open(os.path.join(self._path, key), 'wb') as f:
    #         f.write(bson.dumps(tk.Telekinesis(None, self._session, block_gc=True)._encode(lst)))

    # async def list(self, key):

    # async def getmtime(self, key):
    #     try:
    #         return os.path.getmtime(os.path.join(self._path, key))
    #     except FileNotFoundError:
    #         return FileNotFoundError