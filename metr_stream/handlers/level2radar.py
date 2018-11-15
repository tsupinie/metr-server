
import numpy as np

import os
import logging
_logger = logging.getLogger(__name__)
_logger.setLevel(logging.INFO)

# Get PyART to shut up when you import it
os.environ['PYART_QUIET'] = "1"
from pyart.io import read_nexrad_archive
from pyart.correct import dealias_unwrap_phase

import pyproj

from io import BytesIO
from datetime import datetime, timedelta
import pytz
import json
import zlib
import base64
import struct
import re

from metr_stream.handlers.handler import DataHandler
from metr_stream.utils.download import download
from metr_stream.utils.static import get_static
from metr_stream.utils.cache import Cache
from metr_stream.utils.errors import NoNewDataError

_url_base = "http://mesonet-nexrad.agron.iastate.edu/level2/raw"
_wsr_88ds = None
_recent_td = timedelta(hours=1)
_remote_tz = pytz.timezone('America/Chicago')

def radar_info():
    global _wsr_88ds
    if _wsr_88ds is None:
        _wsr_88ds = get_static('wsr88ds.json')

    return _wsr_88ds


async def check_recent():
    def parse_dt(dt_str):
        return datetime.strptime(dt_str, "%Y-%m-%d %H:%M").replace(tzinfo=_remote_tz).astimezone(pytz.utc).replace(tzinfo=None)

    radar_ids = [ st['id'] for st in radar_info() ]

    html = (await download(_url_base)).decode('utf-8')

    rem_sites = re.findall("href=\"([\w\d]{4})/\".*?([\d]{4}-[\d]{2}-[\d]{2} [\d]{2}:[\d]{2})", html)
    rem_sites = [ (site, parse_dt(dt)) for site, dt in rem_sites if site in radar_ids ]

    rem_recent = dict((site, dt > datetime.utcnow() - _recent_td) for site, dt in rem_sites)
    return rem_recent


async def check_recent_site(site):
    def get_recent_dt(line):
        if line == "":
            return None

        dt = datetime.strptime(line[-13:], '%Y%m%d_%H%M')
        return dt if dt >= datetime.utcnow() - _recent_td else None

    url = f"{_url_base}/{site}/dir.list"
    txt = await download(url)

    recent = [ get_recent_dt(line) for line in txt.decode('utf-8').split("\n") ]
    return [ dt for dt in recent if dt is not None ]    


def _cache_fname(cache_dir, site, field, elev):
    def fname(dt):
        return f"{cache_dir}/{site}_{field}_{elev:04.1f}_{dt.strftime('%Y%m%d_%H%M')}.json"
    return fname


class Level2Handler(DataHandler):
    _cache_dir = "data/l2"

    def __init__(self, site, field, elev):
        self._site = site
        self._field = field
        self._elev = elev
        self._radar_vols = []

        self._cache = Cache(_cache_fname(Level2Handler._cache_dir, self._site, self._field, self._elev),
                            timeout=timedelta(minutes=15))

        self._last_dt_sent = None

        int_deg = int(np.floor(self._elev))
        frc_deg = int((self._elev - int_deg) * 10)
        self.id = f"level2radar.{self._site}.{self._field}.{int_deg:02d}p{frc_deg:1d}"

    async def fetch(self, first_time=True):
        self._radar_vols = [ rv for rv in self._radar_vols if rv.timestamp > (datetime.utcnow() - timedelta(hours=2)) ]

        dts = await check_recent_site(self._site)
        dts.sort(reverse=True)

        sweep = None
        idt = 0

        while sweep is None:
            fetch_dt = dts[idt]
            if first_time:
                sweep = self._load_cache(fetch_dt)
                if sweep is not None:
                    break

            try:
                rv = await RadarVolume.fetch(self._site, fetch_dt)
            except (ValueError, KeyError) as exc:
                print(exc)
            else:
                sweep_obj = rv.get_sweep(self._field, self._elev)

                try:
                    sweep = sweep_obj.to_json()
                except AttributeError:
                    _logger.info("Rejecting volume: sweep not present")
                    sweep = None
                else:
                    if not sweep_obj.is_complete():
                        dazim = round(sweep_obj._dazim, 1)
                        _logger.info(f"Rejecting volume: sweep incomplete (dazim = {dazim}, n_rays = {sweep_obj._data.shape[0]})")
                        sweep = None
                    else:
                        self._radar_vols.append(rv)

            idt += 1

        if self._last_dt_sent is not None and sweep['entities'][0]['valid'] <= self._last_dt_sent:
            raise NoNewDataError(self.id)

        self._last_dt_sent = sweep['entities'][0]['valid']

        _logger.debug(f"Number of radar volumes: {len(self._radar_vols)}")
        sweep['handler'] = self.id
        return sweep
       
    def post_fetch(self):
        self._cache_volumes()

    def data_check_intv(self):
        return 60

    def _cache_volumes(self):
        for rv in self._radar_vols:
            rv.cache()

    def _load_cache(self, dt):
        sweep = self._cache.load_cache(dt)
        return sweep


class RadarVolume(object):
    def __init__(self, sweeps):
        self._sweeps = sweeps

    def get_sweep(self, field, elev):
        sweep = None
        for swp in self._sweeps:
            swp_field = RadarSweep._cache_fields[swp.field]
            swp_elev = round(swp.elevation, 1)
            if field == swp_field and elev == swp_elev and swp.has_data():
                sweep = swp
        return sweep

    def cache(self):
        for swp in self._sweeps:
            if swp.is_complete() and swp.has_data():
                swp.cache()

    @property
    def timestamp(self):
        return min(swp.timestamp for swp in self._sweeps)

    @classmethod
    async def fetch(cls, site, dt, local=False):
        if local:
            url = f"http://127.0.0.1:8000/data/l2raw/{site}{dt.strftime('%Y%m%d_%H%M%S')}_V06"
        else:
            url = f"{_url_base}/{site}/{site}_{dt.strftime('%Y%m%d_%H%M')}"

        _logger.debug(f"Downloading radar volume for {site} at {dt.strftime('%d %b %Y %H%M UTC')}")
        bio = BytesIO()
        bio.write(await download(url))
        bio.seek(0)

        rfile = read_nexrad_archive(bio)
        rfile_dealias = dealias_unwrap_phase(rfile)
        dt = datetime.strptime(rfile.time['units'], 'seconds since %Y-%m-%dT%H:%M:%SZ')

        sweeps = []
        for field in rfile.fields.keys():
            for ie, elv in enumerate(rfile.fixed_angle['data']):
                istart, iend = rfile.get_start_end(ie)
                azimuths = rfile.get_azimuth(ie)
                ranges = rfile.range['data']

                nyquist = rfile.get_nyquist_vel(ie)
                if field == 'velocity' and nyquist < 10:
                    continue
                elif field != 'velocity' and len(sweeps) > 0 and sweeps[-1].elevation == elv and sweeps[-1].field == field:
                    # Check to see if this is a "duplicate" sweep
                    if nyquist > 10:
                        # Assume this is the short-range sweep and ignore it
                        continue
                    else:
                        # Assume that somehow the short-range sweep got put in the file
                        # first and take it out. I don't think this should ever happen.
                        sweeps.pop()

                saz = azimuths[0]
                eaz = azimuths[-1] if azimuths[-1] > azimuths[0] else azimuths[-1] + 360
                dazim = round((eaz - saz) / len(azimuths), 1)

                dt_sweep = dt + timedelta(seconds=rfile.time['data'][istart])

                if field == 'velocity':
                    field_data = rfile_dealias['data'][istart:(iend + 1)]
                else:
                    field_data = rfile.get_field(ie, field)

                rs = RadarSweep(site, dt_sweep, field, elv, 
                                azimuths[0], float(ranges[0]), dazim, 250, field_data)
                sweeps.append(rs)
        return cls(sweeps)


class RadarSweep(object):
    _cache_fields = {'reflectivity': 'REF', 'velocity': 'VEL', 'spectrum_width': 'SPW', 
                     'cross_correlation_ratio': 'CCR', 'differential_phase': 'KDP', 'differential_reflectivity': 'ZDR'}

    def __init__(self, site, dt, field, elevation, start_azimuth, start_range, dazim, drng, data):
        ctr_azim = dazim / 2

        self.site = site
        self.timestamp = dt
        self.field = field
        self.elevation = elevation
        self._st_az = round((start_azimuth - ctr_azim) / dazim) * dazim + ctr_azim
        self._st_rn = start_range
        self._dazim = dazim
        self._drng = drng
        self._data = data

        field_str = RadarSweep._cache_fields[self.field]
        self._cache = Cache(_cache_fname(Level2Handler._cache_dir, self.site, RadarSweep._cache_fields[self.field], self.elevation))

    def is_complete(self):
        n_rays = self._data.shape[0]
        return (self._dazim == 0.5 and n_rays == 720) or (self._dazim == 1.0 and n_rays == 360)

    def has_data(self):
        return (~self._data.mask).sum() > 10

    def to_json(self):
        data_filled = list(np.ma.filled(self._data, -99.).ravel())
        data_packed = struct.pack("%df" % len(data_filled), *data_filled)

        for st in radar_info():
            if st['id'] == self.site:
                site = st
                break

        rs_json = {'site': self.site, 'field': self.field.title(), 'elevation': "%f" % self.elevation}

        radar_entity = {'st_azimuth': self._st_az, 'st_range': self._st_rn, 'dazim':self._dazim, 'drng':self._drng}
        radar_entity['site_latitude'] = site['latitude']
        radar_entity['site_longitude'] = site['longitude']

        radar_entity['valid'] = self.timestamp.strftime("%Y-%m-%d %H:%M:%S UTC")
        radar_entity['expires'] = (self.timestamp + timedelta(minutes=15)).strftime("%Y-%m-%d %H:%M:%S UTC")
        radar_entity['n_rays'], radar_entity['n_gates'] = self._data.shape
        base64_data = base64.encodebytes(data_packed).decode('ascii')
        radar_entity['data'] = "".join(base64_data.split("\n"))

        rs_json['entities'] = [radar_entity]
        return rs_json
        
    def cache(self):
        if not self._cache.is_cached(self.timestamp):
            self._cache.cache(self.to_json(), self.timestamp)

        self._data = None

if __name__ == "__main__":
    check_recent()
