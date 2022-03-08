from glob import glob
import os
import asyncio
import shutil
import time
from .storage import SimpleKV

class FileSync:
    def __init__(self, data_branch, target_dir, support_dir):
        self.data = data_branch
        self.target_dir = target_dir
        self.tracker = SimpleKV(support_dir)
        self.task = asyncio.create_task(self.keep_sync())
        
        self.ftk = lambda f: tuple(f.strip('/').split('/')[len(self.target_dir.strip('/').split('/')):])
        self.ktf = lambda k: os.path.join(self.target_dir, '/'.join(k))

    async def keep_sync(self):
        while True:
            await self.sync()
            await asyncio.sleep(5)

    async def sync(self):
        tree = await self.data.tree(())
        files = glob(os.path.join(self.target_dir, '**'), recursive=True)
        for t in tree:
            if t not in self.tracker.keys() or tree[t] > self.tracker.get(t)[1]:
                d = await self.data.get(t)
                if d:
                    print('downloading', t)
                    with open(self.ktf(t), 'wb') as f:
                        data = await self.data.get(t)
                        f.write(data if isinstance(data, bytes) else str(data).encode())
                    self.tracker.set(t, (os.path.getmtime(self.ktf(t)), tree[t], False))
                elif await self.data.list(t):
                    if not os.path.exists(self.ktf(t)):
                        print('making dir', t)
                        os.mkdir(self.ktf(t))
                        self.tracker.set(t, (os.path.getmtime(self.ktf(t)), tree[t], False))

        files = glob(os.path.join(self.target_dir, '**'), recursive=True)

        for f in files:
            if self.ftk(f) not in self.tracker.keys() \
            or (os.path.getmtime(f) > self.tracker.get(self.ftk(f))[0]):
                print('uploading', f)
                mt = os.path.getmtime(f)
                if os.path.isfile(f):
                    with open(f, 'rb') as ff:
                        ts = await self.data.set(self.ftk(f), ff.read())
                else:
                    ts = await self.data.set(self.ftk(f), None)
                self.tracker.set(self.ftk(f), (mt, ts, False))
        tree = await self.data.tree(())

        for k in self.tracker.keys():
            if k not in tree and not self.tracker.get(k)[2]:
                print('deleting (down)', k)
                ts = await self.data.list_versions(k)[-1]
                self.tracker.set(k, (time.time(), ts, True))
                if os.path.exists(self.ktf(k)):
                    if os.path.isdir(self.ktf(k)):
                        shutil.rmtree(self.ktf(k))
                    else:
                        os.remove(self.ktf(k))

            if self.ktf(k) not in files and not self.tracker.get(k)[2]:
                print('deleting (up)', k)
                ts = await self.data.remove(k)
                self.tracker.set(k, (ts, ts, True))
