
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
import logging
_logger = logging.getLogger(__name__)
_logger.setLevel(logging.DEBUG)

from metr_stream.handlers.handler import DataHandler
from metr_stream.utils.download import download
from metr_stream.utils.static import get_static
from metr_stream.utils.errors import StaleDataError
from metr_stream.utils.obs.mdf import MDF
from metr_stream.utils.cache import Cache


def _cache_fname(source, network):
    def fname(dt):
        return f"data/sfc/{source}_{network}_{dt.strftime('%Y%m%d%H%M')}.json"
    return fname


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
    def __init__(self, name, url_fmt, parser, cycle, data_check_intv, delay, stale):
        self.name = name
        self.url_fmt = url_fmt
        self.parser = parser
        self._cycle = cycle
        self._delay = delay
        self._check_intv = data_check_intv
        self.stale = stale

    def get_time(self, dcycle=0):
        now = (datetime.utcnow() - timedelta(seconds=self._delay)).timestamp()
        recent = floor(now / self._cycle) * self._cycle
        obs_dt = datetime.fromtimestamp(recent) - timedelta(seconds=(dcycle * self._cycle))
        if (datetime.utcnow() - obs_dt).total_seconds() > self.stale:
            raise StaleDataError('')
        return obs_dt

    def get_expected(self):
        now = datetime.utcnow().replace(tzinfo=pytz.utc)
        recent = datetime.utcfromtimestamp(floor(now.timestamp() / self._check_intv) * self._check_intv)
        expected = recent + timedelta(seconds=self._delay)
        if expected > now.replace(tzinfo=None) + timedelta(seconds=self._check_intv):
            expected -= timedelta(seconds=self._check_intv)
        return expected


_configs = {
    'metar': [
        ObsNetworkConfig(
            "metar",
            "http://www.mesonet.org/data/public/noaa/metar/archive/mdf/conus/%Y/%m/%d/%Y%m%d%H%M.mdf",
            _parse_metar_mdf, 3600, 300, 600, 7200
        )
    ],
    'mesonet': [
        ObsNetworkConfig(
            "okmesonet",
            "http://www.mesonet.org/data/public/mesonet/mdf/%Y/%m/%d/%Y%m%d%H%M.mdf",
            _parse_meso_mdf, 300, 300, 420, 1200
        )
    ]
}


class ObsHandler(DataHandler):
    def __init__(self, source):
        self._source = source
        self._obs = None
        self._cache = {cfg.name: Cache(_cache_fname(self._source, cfg.name)) for cfg in _configs[source]}

        self.id = f"obs.{self._source}"

    async def fetch(self, first_time=True):
        params = ['STID', 'LAT', 'LON', 'PALT', 'TAIR', 'TDEW', 'WDIR', 'WSPD']
        def pack_ob(param_order, ob):
            pack_fmt = '5sfffffff'
            return struct.pack(pack_fmt, *[ob[p] for p in param_order])

        obs_dt = max(cfg.get_time() for cfg in _configs[self._source])

        entities = []
        for config in _configs[self._source]:
            obs_entity = self._cache[config.name].load_cache(obs_dt)

            if obs_entity is None:
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
                    obs_str = b"".join(pack_ob(params, ob) for ob in network_obs)
                    base64_data = base64.encodebytes(obs_str).decode('ascii')

                    obs_entity = {
                        'network': config.name,
                        'valid': obs_dt.strftime("%Y-%m-%d %H:%M:%S UTC"),
                        'expires': (obs_dt + timedelta(seconds=config.stale)).strftime("%Y-%m-%d %H:%M:%S UTC"),
                        'params': params,
                        'data': "".join(base64_data.split("\n")),
                    }

            if obs_entity is not None:
                entities.append(obs_entity)

        if len(entities) == 0:
            self._obs = None
            raise StaleDataError(f"obs.{self._source}")

        obs_json = {
            'source': self._source,
            'handler': self.id,
            'entities': entities,
        }

        self._obs = obs_json

        return obs_json

    def data_check_intv(self):
        expected_times = [cfg.get_expected() for cfg in _configs[self._source]] 
        next_time = (min(expected_times) - datetime.utcnow()).total_seconds()
        return next_time + 1

    def post_fetch(self):
        if self._obs is None:
            return

        for entity in self._obs['entities']:
            dt = datetime.strptime(entity['valid'], '%Y-%m-%d %H:%M:%S UTC')
            self._cache[entity['network']].cache(entity, dt)
