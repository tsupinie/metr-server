
import os
import json

def _get_static_path():
    static_path = os.path.join(__file__, '..', '..', '..', 'static')
    return os.path.normpath(static_path)

def get_static(fname):
    static_path = _get_static_path()
    static_fname = os.path.join(static_path, fname)

    with open(static_fname, 'rb') as statf:
        static_json = json.loads(statf.read().decode('utf-8'))

    return static_json
