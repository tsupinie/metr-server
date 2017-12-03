
import logging
import json
import multiprocessing

from autobahn.asyncio.websocket import WebSocketServerProtocol

from metr_stream.handlers.handler import get_data_handler
from metr_stream.utils.errors import StaleDataError


class MetrStreamProtocol(WebSocketServerProtocol):
    def __init__(self, *args, **kwargs):
        super(MetrStreamProtocol, self).__init__(*args, **kwargs)

        self._logger = logging.getLogger(__name__)
        self._logger.setLevel(logging.INFO)

    def onConnect(self, request):
        self._source = request.peer
        self._logger.info(f"Connection from {self._source} opened")

    async def onMessage(self, payload, is_binary):
        msg_json = json.loads(payload)

        req_type = msg_json.pop('type')
        req_handler = get_data_handler(req_type)(**msg_json)

        success = await self.fetchData(req_handler, is_binary)
        if success:
            self._logger.debug(f"Activating {handler_id} for {self._source}")


    def sendMessage(self, payload, is_binary):
        self._logger.info(f"Sending {len(payload)} bytes to {self._source}")
        super(MetrStreamProtocol, self).sendMessage(payload, is_binary)

    def onClose(self, was_clean, code, reason):
        if was_clean:
            self._logger.info(f"Connection from {self._source} closed")
        else:
            self._logger.info(f"Connection from {self._source} terminated ({reason})")

    async def fetchData(self, handler, is_binary):
        success = True
        try:
            req_data = await handler.fetch()
        except StaleDataError as exc:
            self._logger.error(f"Stale data in {exc.handler}")
            req_data = {'handler': exc.handler, 'error':'stale data'}
            success = False
   
        data_json = json.dumps(req_data).encode('utf-8')
        self.sendMessage(data_json, is_binary)

        proc = multiprocessing.Process(target=handler.post_fetch)
        proc.start()

        return success
