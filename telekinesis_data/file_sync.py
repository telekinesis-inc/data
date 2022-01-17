class FileSync:
    def __init__(self, remote_telekinesis_data, location):
        self._remote_telekinesis_data = remote_telekinesis_data
        self._location = location
    async def sync(self):
        ...
