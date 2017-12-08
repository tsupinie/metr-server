
import json
from datetime import datetime, timedelta
import os

class Cache(object):
    def __init__(self, fname_func, timeout=timedelta(minutes=5)):
        self._timeout = timeout
        self._fname = fname_func

    def load_cache(self, dt):
        if self.is_expired(dt) or not self.is_cached(dt):
            return None

        fname = self._fname(dt)
        json_str = json.loads(open(fname, 'rb').read().decode('utf-8'))
        return json_str

    def cache(self, data, dt):
        json_str = json.dumps(data).encode('utf-8')
        fname = self._fname(dt)
        open(fname, 'wb').write(json_str)

    def is_cached(self, dt):
        fname = self._fname(dt)
        return os.path.exists(fname)

    def is_expired(self, dt):
        if not self.is_cached(dt):
            return False

        fname = self._fname(dt)
        if datetime.utcfromtimestamp(os.path.getmtime(fname)) < datetime.utcnow() - self._timeout:
            return True
