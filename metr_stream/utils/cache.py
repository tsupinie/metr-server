
import json
from datetime import datetime, timedelta
import os

class Cache(object):
    def __init__(self, fname_func, timeout=timedelta(minutes=5)):
        self._timeout = timeout
        self._fname = fname_func

    def load_cache(self, dt):
        fname = self._fname(dt)
        if not os.path.exists(fname):
            return None

        if datetime.utcfromtimestamp(os.path.getmtime(fname)) < datetime.utcnow() - self._timeout:
            return None

        json_str = json.loads(open(fname, 'rb').read().decode('utf-8'))
        return json_str

    def cache(self, data, dt):
        json_str = json.dumps(data).encode('utf-8')
        fname = self._fname(dt)
        open(fname, 'wb').write(json_str)
