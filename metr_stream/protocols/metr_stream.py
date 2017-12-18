
import logging
import json
import multiprocessing

from autobahn.asyncio.websocket import WebSocketServerProtocol

from metr_stream.handlers.handler import get_data_handler
from metr_stream.utils.errors import StaleDataError
from metr_stream.utils.timer import Timer


class MetrStreamProtocol(WebSocketServerProtocol):
    def __init__(self, *args, **kwargs):
        super(MetrStreamProtocol, self).__init__(*args, **kwargs)

        self._logger = logging.getLogger(__name__)
        self._logger.setLevel(logging.DEBUG)
        self._active_timers = {}

    def onConnect(self, request):
        self._source = request.peer
        self._logger.info(f"Connection from {self._source} opened")

    async def onMessage(self, payload, is_binary):
        msg_json = json.loads(payload)

        req_action = msg_json.pop('action')
        if req_action == 'activate':
            req_type = msg_json.pop('type')
            req_handler = get_data_handler(req_type)(**msg_json)

            success = await self.fetchData(req_handler, is_binary)
            if success:
                handler_id = req_handler.id
                self._logger.debug(f"Activating {handler_id} for {self._source}")

        elif req_action == 'deactivate':
            self._logger.debug(f"Deactivating {msg_json['handler']} for {self._source}")
            handler_timer = self._active_timers.pop(msg_json['handler'])
            handler_timer.stop()
        else:
            self._logger.error(f"Unknown request action: {req_action}")

    def sendMessage(self, payload, is_binary):
        self._logger.info(f"Sending {len(payload)} bytes to {self._source}")
        super(MetrStreamProtocol, self).sendMessage(payload, is_binary)

    def onClose(self, was_clean, code, reason):
        for timer in self._active_timers.values():
            timer.stop()

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
        except Exception as exc:
            self._logger.error(f"Error in {handler.id}: {exc}")
            req_data = {'handler': handler.id, 'error':'internal server error'}
            success = False   

        async def doFetch():
            await self.fetchData(handler, is_binary)

        handler_timer = Timer(doFetch, handler.data_check_intv(), single_shot=True)
        handler_timer.start()
        self._active_timers[handler.id] = handler_timer

        data_json = json.dumps(req_data).encode('utf-8')
        self.sendMessage(data_json, is_binary)

        proc = multiprocessing.Process(target=handler.post_fetch)
        proc.start()

        return success
