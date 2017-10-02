
import json

from metr_stream.handlers.handler import DataHandler

class ShapefileHandler(DataHandler):
    def __init__(self, domain, name):
        self._domain = domain
        self._name = name

    async def fetch(self):
        fname = f"data/{self._domain}/{self._name}.json"
        shp_str = open(fname, 'rb').read().decode('utf-8')
        shp_json = json.loads(shp_str)
        shp_json['handler'] = f"shapefile.{self._domain}.{self._name}"
        return shp_json
