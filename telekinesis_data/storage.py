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
        
        if not self._raw_exists(self._root):
            self._set_all_metadata(self._root, {
                'key': self._root,
                'user_metadata': {},
                'timestamp': time.time(),
                'children': []}, branch)

    async def get_metadata(self, key, branch=None, timestamp=None):
        return (self._get_all_metadata(self._root+(key and '/')+key, branch, timestamp) or {}).get('user_metadata')

    async def get_tuple(self, key, default=None, branch=None, timestamp=None):
        metadata = None
        data = default
        try:
            if all_metadata := self._get_all_metadata(self._root+(key and '/')+key, branch, timestamp):
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

    def _raw_exists(self, fullkey):
        return os.path.exists(os.path.join(self._path, 'meta', self._hash(fullkey.encode()))) 
    
    async def exists(self, key):
        return self._raw_exists(self._root + (key and '/')+key)

    async def getmtime(self, key, branch=None, timestamp=None):
        return (self._get_all_metadata(self._root+(key and '/')+key, branch, timestamp))['timestamp']

    async def list(self, root='', metadata_query=None, metadata=False, details=False, data=False, branch=None, timestamp=None):
        raw_keys = (self._get_all_metadata(self._root+(root and '/')+root, branch, timestamp) or {}).get('children') or []
        keys = [k[len(self._root)+1:] for k in raw_keys]
        if metadata_query:
            raise NotImplemented
        if not any([details, data, metadata, metadata_query]):
            return keys
        out = {k: {} for k in keys}
        for k, rk in zip(keys, raw_keys):
            if metadata:
                out[k]['metadata'] = await self.get_metadata(rk, branch, timestamp)
            if details:
                am = self._get_all_metadata(rk, branch, timestamp)
                out[k]['details'] = {'timestamp': am['timestamp'], 'n_children': len(am['children'])}
            if data:
                out[k]['data'] = await self.get(k, None, branch, timestamp)
        return out
            
    @tk.block_arg_evaluation
    async def set(self, key, value=None, metadata=None, branch=None, clear=False):
        if isinstance(value, tk.Telekinesis):
            value._block_gc = True

        fullkey = self._root+(key and '/')+key
        old_all_metadata = self._get_all_metadata(fullkey, branch)
        old_user_metadata = ((not clear and (old_all_metadata or {})) or {}).get('user_metadata') or {}
        combined_user_metadata = old_user_metadata.copy()
        combined_user_metadata.update(metadata or {})

        all_metadata = {
            'key': fullkey,
            'user_metadata': combined_user_metadata,
            'timestamp': time.time(),
            'children': (old_all_metadata or {}).get('children') or []
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
        
        self._set_all_metadata(fullkey, all_metadata, branch)

    def _set_all_metadata(self, fullkey, all_metadata, branch):
        hsh = self._hash(fullkey.encode())
        
        if fullkey:
            parent = '/'.join(fullkey.split('/')[:-1])
            if not self._raw_exists(parent):
                self._set_all_metadata(parent, {
                    'key': parent,
                    'user_metadata': {},
                    'timestamp': time.time(),
                    'children': [fullkey]
                }, branch)
            else:
                if not self._raw_exists(fullkey):
                    old_all_metadata = self._get_all_metadata(parent, branch)
                    old_all_metadata['children'].append(fullkey)
                    self._set_all_metadata(parent, old_all_metadata, branch)

        path_meta = os.path.join(self._path, 'meta', hsh, branch or self.branch)
        os.makedirs(os.path.dirname(path_meta), exist_ok=True)

        with open(path_meta + '_', 'w') as f:
            ujson.dump(all_metadata, f, escape_forward_slashes=False)
        os.rename(path_meta+'_', path_meta)

    def _get_all_metadata(self, fullkey, branch, timestamp=None):
        hsh = self._hash((fullkey).encode())
        metadata = None
        try:
            path_meta = os.path.join(self._path, 'meta', hsh, branch or self.branch)
            with open(path_meta, 'rb') as f:
                return ujson.load(f)

        except FileNotFoundError:
            return metadata

    def _hash(self, data):
        return base64.b64encode(hashlib.blake2s(data).digest(), b'_-')[:-1].decode()
