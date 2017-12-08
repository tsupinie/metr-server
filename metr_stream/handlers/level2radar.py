
import numpy as np

import os
import logging
_logger = logging.getLogger(__name__)
_logger.setLevel(logging.INFO)

# Get PyART to shut up when you import it
os.environ['PYART_QUIET'] = "1"
from pyart.io import read_nexrad_archive

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


def _cache_fname(cache_dir, site, field, elev, dt):
    fname = f"{cache_dir}/{site}_{field}_{elev:04.1f}_{dt.strftime('%Y%m%d_%H%M')}.json"
    return fname


class Level2Handler(DataHandler):
    _cache_dir = "data/l2"

    def __init__(self, site, field, elev):
        self._site = site
        self._field = field
        self._elev = elev
        self._radar_vols = []

        self.data_check_intv = 60

        int_deg = int(np.floor(self._elev))
        frc_deg = int((self._elev - int_deg) * 10)
        self.id = f"level2radar.{self._site}.{self._field}.{int_deg:02d}p{frc_deg:1d}"

    async def fetch(self):
        dts = await check_recent_site(self._site)
        dts.sort(reverse=True)

        sweep = None
        idt = 0

        while sweep is None:
            fetch_dt = dts[idt]
            sweep = self._load_cache(self._site, self._field, self._elev, fetch_dt)
            if sweep is not None:
                break

            try:
                rv = await RadarVolume.fetch(self._site, fetch_dt)
            except ValueError:
                pass
            else:
                sweep_obj = rv.get_sweep(self._field, self._elev)
                self._radar_vols.append(rv)

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

            idt += 1

        sweep['handler'] = self.id
        return sweep
       
    def post_fetch(self):
        for rv in self._radar_vols:
            rv.cache(cache_dir=Level2Handler._cache_dir)

    def _load_cache(self, site, field, elev, dt):
        cache_name = _cache_fname(Level2Handler._cache_dir, site, field, elev, dt)
        if os.path.exists(cache_name):
            sweep_str = open(cache_name, 'rb').read().decode('utf-8')
            sweep = json.loads(sweep_str)
        else:
            sweep = None

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

    def cache(self, cache_dir='.'):
        for swp in self._sweeps:
            if swp.is_complete() and swp.has_data():
                swp.cache(cache_dir=cache_dir)

    @classmethod
    async def fetch(cls, site, dt, local=False):
        if local:
            url = f"http://127.0.0.1:8000/data/l2raw/{site}{dt.strftime('%Y%m%d_%H%M%S')}_V06"
        else:
            url = f"{_url_base}/{site}/{site}_{dt.strftime('%Y%m%d_%H%M')}"

        bio = BytesIO()
        bio.write(await download(url))
        bio.seek(0)

        rfile = read_nexrad_archive(bio)
        dt = datetime.strptime(rfile.time['units'], 'seconds since %Y-%m-%dT%H:%M:%SZ')

        sweeps = []
        for field in rfile.fields.keys():
            for ie, elv in enumerate(rfile.fixed_angle['data']):
                istart, iend = rfile.get_start_end(ie)
                azimuths = rfile.get_azimuth(ie)
                ranges = rfile.range['data']

                saz = azimuths[0]
                eaz = azimuths[-1] if azimuths[-1] > azimuths[0] else azimuths[-1] + 360
                dazim = round((eaz - saz) / len(azimuths), 1)

                dt_sweep = dt + timedelta(seconds=rfile.time['data'][istart])
                rs = RadarSweep(site, dt_sweep, field, elv, 
                                azimuths[0], float(ranges[0]), dazim, 250, rfile.get_field(ie, field))
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

        proj = pyproj.Proj(proj='lcc', lon_0=-97.5, lat_1=30, lat_2=45)
        site_x, site_y = proj(site['longitude'], site['latitude'])
        first_x = site_x + self._st_rn * np.sin(np.radians(self._st_az))
        first_y = site_y + self._st_rn * np.cos(np.radians(self._st_az))

        rs_json = {'site': self.site, 'field': self.field.title(), 'elevation': "%f" % self.elevation, 
                   'st_azimuth': self._st_az, 'st_range': self._st_rn, 'dazim':self._dazim, 'drng':self._drng}
        rs_json['site_latitude'] = site['latitude']
        rs_json['site_longitude'] = site['longitude']
        rs_json['first_longitude'], rs_json['first_latitude'] = proj(first_x, first_y, inverse=True)
        rs_json['timestamp'] = self.timestamp.strftime("%Y%m%d_%H%M")
        rs_json['n_rays'], rs_json['n_gates'] = self._data.shape
        base64_data = base64.encodebytes(zlib.compress(data_packed)).decode('ascii')
        rs_json['data'] = "".join(base64_data.split("\n"))
        return rs_json
        
    def cache(self, cache_dir='.'):
        rs_json = self.to_json()

        field_str = RadarSweep._cache_fields[self.field]
        fname = _cache_fname(cache_dir, self.site, field_str, self.elevation, self.timestamp)
        with open(fname, 'wb') as fcache:
            fcache.write(json.dumps(rs_json).encode('utf-8'))

if __name__ == "__main__":
    check_recent()
