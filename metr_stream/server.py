
import logging
import asyncio
import signal
import os
from datetime import datetime, timedelta

from metr_stream.protocols.hollaback import HollaBackProtocol
from metr_stream.protocols.metr_stream import MetrStreamProtocol

from aiohttp import web

class Cleaner(object):
    def __init__(self, interval, max_age, data_dir):
        self._intv = interval
        self._max_age = timedelta(seconds=max_age)
        self._path = data_dir

        self._logger = logging.getLogger(__name__)
        self._logger.setLevel(logging.INFO)

    async def run_cleaner(self):
        self._logger.info("Starting cleaner process")
        while True:
            await asyncio.sleep(self._intv)

            try:
                self._cleanup()
            except Exception as exc:
                self._logger.error(str(exc))

    def _cleanup(self):
        now = datetime.utcnow()
        self._logger.info(f"Cleaning '{self._path}'")
        for root, dnames, fnames in os.walk(self._path):
            if os.path.basename(root) in ['geo', 'l2raw']:
                continue

            self._logger.debug(f"Cleaning '{root}'")

            for fname in fnames:
                if not fname.endswith('.json'):
                    continue

                full_fname = os.path.join(root, fname)

                dt = datetime.utcfromtimestamp(os.path.getmtime(full_fname))
                if dt < now - self._max_age:
                    os.unlink(full_fname)


def main():
    host = "127.0.0.1"
    port = 8002
    data_path = "data"
    logging.basicConfig(format="%(levelname)s|%(name)s|%(asctime)-15s: %(message)s")
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    protocol = HollaBackProtocol()

    app = web.Application()
    app.add_routes([web.get('/', protocol)])
    app.on_shutdown.append(type(protocol).on_shutdown)
    web.run_app(app, host=host, port=port)

if __name__ == "__main__":
    main()
