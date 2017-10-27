
from bs4 import BeautifulSoup

from datetime import datetime, timedelta
import pytz
import zipfile
import zlib
import base64
import struct
import json
import urllib.request as urlreq
import os
import warnings
from collections import defaultdict
from math import exp, log

from metr_stream.handlers.handler import DataHandler
from metr_stream.utils.download import download
from metr_stream.utils.static import get_static
from metr_stream.utils.obs.mdf import MDF

def _cache_fname(source, dt):
    return f"data/sfc/{source}_{dt.strftime('%Y%m%d%H')}.json"

def _parse_metar_mdf(mdf_txt):
    mdf = MDF.from_string(mdf_txt)

    obs = []
    for ob in mdf:
        for param in ob.keys():
            try:
                if ob[param] < -990:
                    ob[param] = float('nan')
            except TypeError:
                pass

        ob['STID'] = ob['STID'].encode('utf-8')
        ob_time = mdf.base_time + timedelta(minutes=ob['TIME'])
        ob['TIME'] = ob_time.strftime("%Y%m%d_%H%M").encode('utf-8')
        ob['WSPD'] *= 1.94  # Convert m/s to kts
        obs.append(ob)

    return obs

def _parse_meso_mdf(mdf_txt):
    static = get_static('okmesonet.json')

    obs = _parse_metar_mdf(mdf_txt)
    for ob in obs:
        relh = float(ob['RELH']) / 100
        tair = float(ob['TAIR']) + 273.15
        sat_vapr = 611 * exp(2.5e6 / 461.5 * (1 / 273.15 - 1 / tair))
        tdew = 1. / (1. / 273.15 - 461.5 / 2.5e6 * log((sat_vapr * relh) / 611))
        ob['TDEW'] = tdew - 273.15

        ob['PALT'] = ob['PRES'] # Set PMSL to be the station presssure for now

        ob['LAT'] = float(static[ob['STID'].decode('utf-8')]['LAT'])
        ob['LON'] = float(static[ob['STID'].decode('utf-8')]['LON'])
    return obs


class ObsNetworkConfig(object):
    def __init__(self, url_fmt, parser):
        self.url_fmt = url_fmt
        self.parser = parser

_configs = {
    'metar': [
        ObsNetworkConfig(
            "http://www.mesonet.org/data/public/noaa/metar/archive/mdf/conus/%Y/%m/%d/%Y%m%d%H%M.mdf",
            _parse_metar_mdf,
        )
    ],
    'mesonet': [
        ObsNetworkConfig(
            "http://www.mesonet.org/data/public/mesonet/mdf/%Y/%m/%d/%Y%m%d%H%M.mdf",
            _parse_meso_mdf,
        )
    ]
}


class ObsHandler(DataHandler):
    def __init__(self, source):
        self._source = source

    async def fetch(self):
        def pack_ob(param_order, ob):
            pack_fmt = '5sff13sfffff'
            return struct.pack(pack_fmt, *[ob[p] for p in param_order])

        now = datetime.utcnow()
        sfc_hr = now.replace(minute=10, second=0, microsecond=0)
        if sfc_hr > now:
            sfc_hr -= timedelta(hours=1) 
        sfc_hr = sfc_hr.replace(minute=0)

        obs_json = self._load_cache(sfc_hr)
        if obs_json is None:
            obs = []
            for config in _configs[self._source]:
                url = sfc_hr.strftime(config.url_fmt)

                txt = (await download(url)).decode('utf-8')
                network_obs = config.parser(txt)

                obs.extend(network_obs)

            params = ['STID', 'LAT', 'LON', 'TIME', 'PALT', 'TAIR', 'TDEW', 'WDIR', 'WSPD']
            obs_str = b"".join(pack_ob(params, ob) for ob in obs)

            base64_data = base64.encodebytes(zlib.compress(obs_str)).decode('ascii')
            obs_json = {'source':'METAR', 'params':params, 'data':"".join(base64_data.split("\n"))}
            obs_json['nominal_time'] = sfc_hr.replace(minute=0).strftime("%Y%m%d_%H%M")
            obs_json['handler'] = f"obs.{self._source}"

        self._obs = obs_json

        return obs_json

    def post_fetch(self):
        json_str = json.dumps(self._obs).encode('utf-8')
        dt = datetime.strptime(self._obs['nominal_time'], '%Y%m%d_%H%M')
        cache_fname = _cache_fname(self._source, dt)
        open(cache_fname, 'wb').write(json_str)

    def _load_cache(self, dt):
        cache_fname = _cache_fname(self._source, dt)
        if not os.path.exists(cache_fname):
            return None

        json_str = json.loads(open(cache_fname, 'rb').read().decode('utf-8'))
        return json_str


