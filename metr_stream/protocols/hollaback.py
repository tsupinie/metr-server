
import logging

from autobahn.asyncio.websocket import WebSocketServerProtocol


class HollaBackProtocol(WebSocketServerProtocol):
    def __init__(self, *args, **kwargs):
        super(HollaBackProtocol, self).__init__(*args, **kwargs)

        self._logger = logging.getLogger(__name__)
        self._logger.setLevel(logging.INFO)

    def onConnect(self, request):
        self._logger.info(f"Connection from {request.peer} accepted")

    def onMessage(self, payload, isBinary):
        self._logger.info(f"Message received: {payload}")
        self.sendMessage(payload, isBinary)

    def onClose(self, wasClean, code, reason):
        if wasClean:
            self._logger.info(f"Connection closed cleanly")
        else:
            self._logger.info(f"Connection terminated (reason: {reason})")
