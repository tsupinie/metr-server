
from .handler import DataHandler

class Level2Handler(DataHandler):
    def __init__(self, site):
        self._site = site

    def fetch(self):
        pass

