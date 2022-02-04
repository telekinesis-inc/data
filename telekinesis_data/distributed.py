from operator import is_
import os
import time
import base64
import hashlib

import bson
import telekinesis as tk

from .storage import SimpleKV, SimpleFileContainer
from .timetravel import TimetravelerKV
from .const import REGIONS

class TelekinesisData:
    def __init__(self, session, path, region='AAAA'):
        if region in REGIONS:
            region = REGIONS[region]
        self._region = region
        self.id = region + session.instance_id
        self._session = session
        self._registry = SimpleKV(os.path.join(path, 'registry'))
        self._local = TimetravelerKV(os.path.join(path, 'meta'))
        self._data = SimpleFileContainer(os.path.join(path, 'data'))

        self._default_branch_id = None
        self._branches = {}#Container(os.path.join(path, 'branches'))
        
        self.client = tk.Telekinesis(self, session)
        self._peers = {self.id: self.client}

    def begin(self, branch_id=None):
        if branch_id is None:
            branch_id = self.id + base64.b64encode(os.urandom(3)).decode()
        self._registry.set((branch_id,), self.id)
        self._default_branch_id = branch_id
        self._branches[branch_id] = {
            'branch_id': branch_id, 'origin_id': None, 'origin_timestamp': None, 'origin_key': None
        }
        
    @tk.inject_first_arg
    @tk.block_arg_evaluation
    async def set(
        self, context, key, value=None, metadata=None, clear=False, value_getter=None, branch=None
    ):
        peer_id, branch_id, branch = await self._overhead(context, branch)
        
        if isinstance(value, tk.Telekinesis):
            value._block_gc = True

        if not value_getter:
            if isinstance(value, bytes):
                value_enc = value
                prefix = '0'
            elif isinstance(value, str):
                value_enc = value.encode()
                prefix = '1'
            else:
                value_enc = bson.dumps(tk.Telekinesis(None, self._session, block_gc=True)._encode(value))
                prefix = ''

            value_hash = prefix + self._hash(value_enc)
        else:
            value_hash = value

        if peer_id:
            for i in range(len(key)+1):
                k = key[:-i] or (i == 0 and key) or ()
                ck = key[:-(i or 1)+1] or key
                if owner_id := self._registry.get((branch_id, *k)):
                    if owner_id == self.id:
                        if k == key:
                            self._registry.set((branch_id, *key), self.id)
                            self._local.set((branch_id, *key), [
                                ('u' if clear else 'uu', {'metadata': metadata or {}}),
                                ('u', {'value': value_hash} if value is not None or clear else {})
                            ])
                            if value is not None or clear and value_hash not in self._data:
                                if value_getter:
                                    value_enc = await value_getter()
                                    assert self._hash(value_enc) == value_hash[1:]
                                self._data.set(value_hash, value_enc)
                        else:
                            self._registry.set((branch_id, *ck), peer_id) 
                            
                            self._local.set((branch_id, *k), [('ua', {'children': ck[-1]})])
                        return k, self.id
                    else:
                        if peer := self._peers.get(owner_id):
                            return peer.set(key, value, metadata, clear, value_getter, branch)
                        else:
                            self._registry.set((branch_id, *k), None)
        else:
            if value is not None or clear and value_hash not in self._data:
                if value_getter:
                    value_enc = await value_getter()
                    assert value_hash == self._hash(value_enc)[1:]
                self._data.set(value_hash, value_enc)
            for i in range(len(key)+1):
                k = key[:-i] or (i == 0 and key) or ()
                if owner_id := self._registry.get((branch_id, *k)):
                    if owner_id == self.id:
                        root = k
                        root_owner_id = self.id
                    else:
                        if peer := self._peers.get(owner_id):
                            if not value_getter and len(value_enc) > 2**18:
                                val = value_hash
                                val_getter = lambda: value_enc
                            else:
                                val = value
                                val_getter = value_getter
                            root, root_owner_id = await peer.set(key, val, metadata, clear, val_getter, branch)
                        else:
                            self._registry.set((branch_id, *k), None)
                            continue
                    
                    self._registry.set((branch_id, *root), root_owner_id)
                    if root != key:
                        self._local.set((branch_id, *key), [
                            ('u' if clear else 'uu', {'metadata': metadata or {}}),
                            ('u', {'value': value_hash} if value is not None or clear else {})
                        ])
                        self._registry.set((branch_id, *key), self.id)
                        for j in range(len(key)):
                            kk = key[:-j-1]
                            ck = key[:-j] or key
                            if kk == root:
                                if root_owner_id == self.id:
                                    self._local.set((branch_id, *kk), [('ua', {'children': ck[-1]})])
                                return
                            else:
                                self._registry.set((branch_id, *kk), self.id)
                                self._local.set((branch_id, *kk), [('ua', {'children': ck[-1]})])
                    elif root_owner_id == self.id:
                        self._local.set((branch_id, *key), [
                            ('u' if clear else 'uu', {'metadata': metadata or {}}),
                            ('u', {'value': value_hash} if value is not None or clear else {})
                        ])
                        return

    @tk.inject_first_arg
    async def get(self, context, key, metadata=False, timestamp=None, branch=None):
        is_peer, branch_id, branch = await self._overhead(context, branch)

        for i in range(len(key)+1):
            k = key[:-i] or (i==0 and key) or ()
            if owner_id := self._registry.get((branch_id, *k)):
                if owner_id == self.id:
                    if self._local.list_versions((branch_id, *key), timestamp, ['origin']):
                        obj = self._local.get((branch_id, *key), timestamp) or {}
                        if metadata:
                            out = obj.get('metadata')
                            if is_peer:
                                return ('data', out)
                            return out
                        if value_hash := obj.get('value'):
                            if value_enc := self._data.get(value_hash):
                                if len(value_hash) == 43:
                                    data = tk.Telekinesis(None, self._session)\
                                        ._decode(bson.loads(value_enc))
                                elif value_hash[0] == '0':
                                    data = value_enc
                                
                                elif value_hash[0] == '1':
                                    data = value_enc.decode()

                                if is_peer:
                                    if len(value_enc) > 2**18:
                                        return ('getter', value_hash, lambda: value_enc)
                                    return ('data', data)
                                return data
                        return
                    elif branch['origin_id']:
                        out = self.client.get(
                            (*branch['origin_key'], *key), 
                            metadata, 
                            min(timestamp or time.time(), branch['origin_timestamp']), 
                            branch['origin_id'])
                        if is_peer:
                            return out
                        return await out
                else:
                    if owner := self._peers.get(owner_id):
                        if is_peer:
                            return owner.get(key, metadata, timestamp, branch)
                        else:
                            out = await owner.get(key, metadata, timestamp, branch)
                            if not out:
                                return
                            if out[0] == 'data':
                                return out[1]
                            if out[0] == 'getter':
                                value_hash = out[1]
                                if data := self._data.get(value_hash):
                                    pass
                                else:
                                    value_enc = await out[2]()
                                    self._data.set(value_hash, data)
                                    if len(value_hash) == 43:
                                        return tk.Telekinesis(None, self._session)\
                                            ._decode(bson.loads(value_enc))
                                    elif value_hash[0] == '0':
                                        return value_enc
                                    
                                    elif value_hash[0] == '1':
                                        return value_enc.decode()
                    else:
                        self._registry.set(k, None)

    @tk.inject_first_arg
    async def remove(self, context, key, branch=None):
        _, branch_id, branch = await self._overhead(context, branch)

        for i in range(0, len(key)+1):
            k = key[:-i] or (i == 0 and key) or ()
            ck = key[:-(i or 1)+1] or key
            if owner_id := self._registry.get((branch_id, *k)):
                if i == 0:
                    self._registry.set((branch_id, *k), None)
                if owner_id == self.id:
                    if i == 1:
                        self._registry.set((branch_id, *ck), None)
                        self._local.set((branch_id, *k), [
                            ('ur', {'children': ck[-1]})
                        ])
                        return
                else:
                    if peer := self._peers.get(owner_id):
                        return peer.remove(key, branch)
                    else:
                        self._registry.set((branch_id, *k), None)


    async def list(self, key, timestamp=None, branch=None):
        _, branch_id, branch = await self._overhead(None, branch)

        for i in range(len(key)+1):
            k = key[:-i] or (i==0 and key) or ()
            if owner_id := self._registry.get((branch_id, *k)):
                if owner_id == self.id:
                    return (self._local.get((branch_id, *key), timestamp) or {}).get('children') or []
                else:
                    if owner := self._peers.get(owner_id):
                        return owner.list(key, timestamp, branch)
                    else:
                        self._registry.set((branch_id, *k), None)

    async def exists(self, key, timestamp=None, branch=None):
        return (key[-1] in await self.list(key[:-1], timestamp, branch)) if len(key) else True

    @tk.inject_first_arg
    async def list_versions(self, context, key, timestamp=None, branch=None):
        is_peer, branch_id, branch = await self._overhead(context, branch)

        for i in range(len(key)+1):
            k = key[:-i] or (i==0 and key) or ()
            if owner_id := self._registry.get((branch_id, *k)):
                if owner_id == self.id:
                    return self._local.list_versions((branch_id, *key), timestamp, ['origin'])
                else:
                    if owner := self._peers.get(owner_id):
                        # if is_peer:
                        return owner.list_versions(key, timestamp, branch)
                        # else:
                        #     out = await owner.list_versions(key, timestamp, branch)
                        #     return out
                    else:
                        self._registry.set((branch_id, *k), None)

    @tk.inject_first_arg
    async def list_branches(self, context, key, timestamp=None, branch=None):
        is_peer, branch_id, branch = await self._overhead(context, branch)

        for i in range(len(key)+1):
            k = key[:-i] or (i==0 and key) or ()
            if owner_id := self._registry.get((branch_id, *k)):
                if owner_id == self.id:
                    return [b for b in 
                            (self._local.get((branch_id, *key), timestamp) or {}).get('branches') or []]
                else:
                    if owner := self._peers.get(owner_id):
                        return owner.list_branches(key, timestamp, branch)
                    else:
                        self._registry.set((branch_id, *k), None)

    @tk.inject_first_arg
    async def create_branch(self, context, new_branch, origin_branch=None, origin_timestamp=None):
        key, name = new_branch
        peer_id, origin_branch_id, origin_branch = await self._overhead(context, origin_branch)
        
        for i in range(len(key)+1):
            k = key[:-i] or (i==0 and key) or ()
            if owner_id := self._registry.get((origin_branch_id, *k)):
                if owner_id == self.id:
                    # happy path first... TODO: Figure out non happy path(s) :/
                    if k == key:
                        branch_id = (peer_id or self.id) + base64.b64encode(os.urandom(3)).decode()
                        new_branch = {
                            'branch_id': branch_id,
                            'origin_id': origin_branch_id,
                            'origin_timestamp': origin_timestamp or time.time(),
                            'origin_key': key
                        }
                        self._local.set((origin_branch_id, *key), [
                            ('uu', {'branches': {name: new_branch}})
                        ])
                        self._registry.set((branch_id,), peer_id)
                        self._branches[branch_id] = new_branch
                        if peer_id:
                            return new_branch
                        self._local.set((branch_id,), [('u', {'origin': new_branch})])
                        return tk.Telekinesis(Branch(self, branch_id), self._session)
                else:
                    if owner := self._peers.get(owner_id):
                        new_branch = await owner.create_branch(new_branch, origin_branch, origin_timestamp)
                        self._registry.set((new_branch['branch_id'],), self.id)
                        self._branches[new_branch['branch_id']] = new_branch
                        self._local.set((new_branch['branch_id'],), [('u', {'origin': new_branch})])
                        return tk.Telekinesis(Branch(self, new_branch['branch_id']), self._session)
                    else:
                        self._registry.set((origin_branch_id, *k), None)

    async def get_branch(self, branch_tup, timestamp=None):
        if branch_tup[1] is None:
            return tk.Telekinesis(Branch(self, self._default_branch_id, branch_tup[0]), self._session)

        branch = await self.get_branch_info(branch_tup, timestamp)
        if branch['branch_id'] not in self._branches:
            self._branches['branch_id'] = branch
        return tk.Telekinesis(Branch(self, branch['branch_id']), self._session)

    async def get_branch_info(self, branch_tup, timestamp=None):
        _, branch_id, branch = await self._overhead(None, None)
        if isinstance(branch_tup, str) and branch_tup in self._branches:
            return self._branches[branch_tup]
        else:
            key, name = branch_tup

            for i in range(len(key)+1):
                k = key[:-i] or (i==0 and key) or ()
                if owner_id := self._registry.get((branch_id, *k)):
                    if owner_id == self.id:
                        return ((self._local.get((branch_id, *key), timestamp) or {}).get('branches') \
                                or {})[name]
                    else:
                        if owner := self._peers.get(owner_id):
                            return owner.get_branch_info(branch_tup, timestamp)
                        else:
                            self._registry.set((branch_id, *k), None)
        
    @tk.inject_first_arg
    async def add_peer(self, context, peer, pull_branch=True, branch=None, expand=True):
        peer_instance_id, branch_id, branch_ = await self._overhead(context, branch, True)
        
        if peer_instance_id:
            if expand:
                for peer_2_id in self._peers:
                    if peer_2_id is not self.id:
                        await self._peers[peer_2_id].add_peer(peer, pull_branch=False, expand=False)
            
            if peer_instance_id != peer._target.session[1] or expand:
                await peer.add_peer(self, False, pull_branch and branch_ or None, False)
            
            peer_id = await peer.id
            if branch:
                branch_id = branch_['branch_id']
                self._default_branch_id = branch_id
                self._branches[branch_id] = branch_
                self._registry.set((branch_id, ), peer_id)

            self._peers[peer_id] = peer
            
            return len(self._peers)

        raise PermissionError

    @tk.inject_first_arg
    async def close(self, metadata):
        if metadata.caller.session[0] == self._session.session_key.public_serial():
            if metadata.caller.session[1] == self._session.instance_id:
                for peer in self._peers:
                    await peer.close()
                return
            else:
                self._peers.pop(metadata.caller.session[1], None)
                return None

        raise PermissionError
    
        return sorted(self._peers)[0]

    def _hash(self, data):
        return base64.b64encode(hashlib.blake2s(data).digest(), b'_-')[:-1].decode()

    async def _overhead(self, context, branch, new_peer=False):
        caller = context and context.caller.session or ['','']
        is_peer = caller[0] == self._session.session_key.public_serial() \
            and caller[1] in [p[4:] for p in self._peers] and caller[1] != self.id[4:]
        
        peer_id = is_peer and [p for p in self._peers if caller[1] == p[4:]][0] or \
            new_peer and caller[0] == self._session.session_key.public_serial() and caller[1]
        
        if is_peer and context.reply_to:
            reply_to = context.reply_to.session
            reply_to_is_peer = reply_to[0] == self._session.session_key.public_serial() \
                and reply_to[1] in [p[4:] for p in self._peers] and reply_to[1] != self.id[4:]
        
            reply_to_peer_id = reply_to_is_peer and [p for p in self._peers if reply_to[1] == p[4:]][0] or \
                new_peer and reply_to[0] == self._session.session_key.public_serial() and reply_to[1]
            
            if reply_to_is_peer:
                peer_id = reply_to_peer_id
        
        
        if branch is None:
            if self._default_branch_id:
                branch = self._branches[self._default_branch_id]
        elif not isinstance(branch, dict):
            branch = await self.get_branch_info(branch)
        branch_id = branch and branch['branch_id']

        return peer_id, branch_id, branch

class Branch:
    def __init__(self, parent, branch_id, root=None):
        self._root = root or ()
        self._parent = parent
        self._branch_id = branch_id

    @tk.inject_first_arg
    @tk.block_arg_evaluation
    async def set(
        self, context, key, value=None, metadata=None, clear=False, value_getter=None, branch=None
    ):
        peer_id, _, _ = await self._parent._overhead(context, None)
        out = self._parent.set(
            context, self._root+tuple(key), value, metadata, clear, value_getter, branch or self._branch_id)
        if peer_id:
            return out
        return await out

    @tk.inject_first_arg
    async def remove(self, context, key, branch=None):
        peer_id, _, _ = await self._parent._overhead(context, None)
        out = self._parent.remove(context, self._root + tuple(key), branch or self._branch_id)
        if peer_id:
            return out
        return await out

    @tk.inject_first_arg
    async def get(self, context, key, metadata=False, timestamp=None, branch=None):
        peer_id, _, _ = await self._parent._overhead(context, None)
        out = self._parent.get(context, self._root + tuple(key), metadata, timestamp, branch or self._branch_id)
        if peer_id:
            return out
        return await out

    @tk.inject_first_arg
    async def list(self, context, key, timestamp=None, branch=None):
        peer_id, _, _ = await self._parent._overhead(context, None)
        out = self._parent.list(self._root + tuple(key), timestamp, branch or self._branch_id)
        if peer_id:
            return out
        return await out
        
    @tk.inject_first_arg
    async def exists(self, context, key, timestamp=None, branch=None):
        peer_id, _, _ = await self._parent._overhead(context, None)
        out = self._parent.exists(self._root + tuple(key), timestamp, branch or self._branch_id)
        if peer_id:
            return out
        return await out
        
    @tk.inject_first_arg
    async def list_versions(self, context, key, timestamp=None, branch=None):
        peer_id, _, _ = await self._parent._overhead(context, None)
        out = self._parent.list_versions(context, self._root + tuple(key), timestamp, branch or self._branch_id)
        if peer_id:
            return out
        return await out
    
    @tk.inject_first_arg
    async def list_branches(self, context, key, timestamp=None, branch=None):
        peer_id, _, _ = await self._parent._overhead(context, None)
        out = self._parent.list_branches(self._root+ tuple(key), timestamp, branch or self._branch_id)
        if peer_id:
            return out
        return await out

    @tk.inject_first_arg
    async def create_branch(self, context, new_branch, origin_branch=None, origin_timestamp=None):
        peer_id, _, _ = await self._parent._overhead(context, None)

        if not isinstance(origin_branch, str):
            if origin_branch:
                origin_branch = ((*self._root, *origin_branch[0]), origin_branch[1])
            else:
                origin_branch = self._branch_id
        out = self._parent.create_branch(((*self._root, *new_branch[0]), new_branch[1]), origin_branch, origin_timestamp)
        if peer_id:
            return out
        return await out

    @tk.inject_first_arg
    async def get_branch(self, context, branch_tup, timestamp=None):
        peer_id, _, _ = await self._parent._overhead(context, None)
        out = self._parent.get_branch(((*self._root, *branch_tup[0]), branch_tup[1]), timestamp)
        if peer_id:
            return out
        return await out

    @tk.inject_first_arg
    async def get_branch_info(self, context, branch_tup, timestamp=None):
        peer_id, _, _ = await self._parent._overhead(context, None)
        out = self._parent.get_branch_info(((*self._root, *branch_tup[0]), branch_tup[1]), timestamp)
        if peer_id:
            return out
        return await out