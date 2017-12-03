
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
from math import exp, log, floor

from metr_stream.handlers.handler import DataHandler
from metr_stream.utils.download import download
from metr_stream.utils.static import get_static
from metr_stream.utils.errors import StaleDataError
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
    def __init__(self, url_fmt, parser, cycle, delay, stale):
        self.url_fmt = url_fmt
        self.parser = parser
        self._cycle = cycle
        self._delay = delay
        self._stale = stale

    def get_time(self, dcycle=0):
        now = (datetime.utcnow() - timedelta(seconds=self._delay)).timestamp()
        recent = floor(now / self._cycle) * self._cycle
        obs_dt = datetime.fromtimestamp(recent) - timedelta(seconds=(dcycle * self._cycle))
        if (datetime.utcnow() - obs_dt).total_seconds() > self._stale:
            raise StaleDataError('')
        return obs_dt


_configs = {
    'metar': [
        ObsNetworkConfig(
            "http://www.mesonet.org/data/public/noaa/metar/archive/mdf/conus/%Y/%m/%d/%Y%m%d%H%M.mdf",
            _parse_metar_mdf, 3600, 600, 7200
        )
    ],
    'mesonet': [
        ObsNetworkConfig(
            "http://www.mesonet.org/data/public/mesonet/mdf/%Y/%m/%d/%Y%m%d%H%M.mdf",
            _parse_meso_mdf, 300, 360, 1200
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

        obs_dt = max(cfg.get_time() for cfg in _configs[self._source])

        obs_json = self._load_cache(sfc_hr)
        if obs_json is None:
            obs = []
            obs_dts = []
            for config in _configs[self._source]:
                network_obs = None
                network_error = False
                dcycle = 0
                while network_obs is None:
                    try:
                        cfg_dt = config.get_time(dcycle=dcycle)
                    except StaleDataError:
                        network_error = True
                        break

                    url = cfg_dt.strftime(config.url_fmt)

                    txt = (await download(url)).decode('utf-8')
                    try:
                        network_obs = config.parser(txt)
                    except:
                        dcycle += 1

                if not network_error:
                    obs.extend(network_obs)
                    obs_dts.append(cfg_dt)

            if len(obs) == 0:
                self._obs = None
                raise StaleDataError(f"obs.{self._source}")

            obs_dt = max(obs_dts)

            params = ['STID', 'LAT', 'LON', 'TIME', 'PALT', 'TAIR', 'TDEW', 'WDIR', 'WSPD']
            obs_str = b"".join(pack_ob(params, ob) for ob in obs)

            base64_data = base64.encodebytes(zlib.compress(obs_str)).decode('ascii')
            obs_json = {'source':'METAR', 'params':params, 'data':"".join(base64_data.split("\n"))}
            obs_json['nominal_time'] = obs_dt.strftime("%Y%m%d_%H%M")
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


