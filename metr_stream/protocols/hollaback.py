
import logging

from metr_stream.protocols.websocket import WebSocketProtocol

class HollaBackProtocol(WebSocketProtocol):
    def __init__(self, *args, **kwargs):
        super(HollaBackProtocol, self).__init__(*args, **kwargs)

        self._logger = logging.getLogger(__name__)
        self._logger.setLevel(logging.INFO)

    async def on_connect(self, request):
        self._remote = request.remote
        self._logger.info(f"Connection from {self._remote} accepted")

    async def on_message(self, payload):
        self._logger.info(f"Message received: {payload}")
        await self.send_message(payload)

    async def on_close(self):
        self._logger.info(f"Connection from {self._remote} closed")
