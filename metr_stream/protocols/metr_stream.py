
import logging
import json

from autobahn.asyncio.websocket import WebSocketServerProtocol

from metr_stream.handlers.handler import get_data_handler


class MetrStreamProtocol(WebSocketServerProtocol):
    def __init__(self, *args, **kwargs):
        super(MetrStreamProtocol, self).__init__(*args, **kwargs)

        self._logger = logging.getLogger(__name__)
        self._logger.setLevel(logging.INFO)

    def onConnect(self, request):
        self._source = request.peer
        self._logger.info(f"Connection from {self._source} opened")

    def onMessage(self, payload, is_binary):
        msg_json = json.loads(payload)

        req_type = msg_json.pop('type')
        req_data = get_data_handler(req_type)(**req)

        data_json = json.dumps(req_data)
        self.sendMessage(data_json, is_binary)

    def onClose(self, was_clean, code, reason):
        if was_clean:
            self._logger.info(f"Connection from {self._source} closed")
        else:
            self._logger.info(f"Connection from {self._source} terminated ({reason})")
