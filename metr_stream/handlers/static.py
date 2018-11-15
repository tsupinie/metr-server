
import json

from metr_stream.handlers.handler import DataHandler

class StaticHandler(DataHandler):
    def __init__(self, static):
        self._static = static
        self.id = "gui"

    async def fetch(self, first_time=True):
        with open(f'static/{self._static}.json', 'rb') as fstat:
            static_data = json.loads(fstat.read().decode('utf-8'))

        static_msg = {'handler': self.id, self._static:static_data}
        return static_msg
