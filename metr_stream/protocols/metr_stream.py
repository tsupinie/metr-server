
import logging
import json
import multiprocessing

from metr_stream.protocols.websocket import WebSocketProtocol
from metr_stream.handlers.handler import get_data_handler
from metr_stream.utils.errors import StaleDataError
from metr_stream.utils.timer import Timer


class MetrStreamProtocol(WebSocketProtocol):
    def __init__(self, *args, **kwargs):
        super(MetrStreamProtocol, self).__init__(*args, **kwargs)

        self._logger = logging.getLogger(__name__)
        self._logger.setLevel(logging.DEBUG)
        self._active_timers = {}

    async def on_connect(self, request):
        self._source = request.peer
        self._logger.info(f"Connection from {self._source} opened")

    async def on_message(self, payload):
        msg_json = json.loads(payload)

        req_action = msg_json.pop('action')
        if req_action == 'activate':
            req_type = msg_json.pop('type')
            req_handler = get_data_handler(req_type)(**msg_json)

            success = await self.fetch_data(req_handler)
            if success:
                handler_id = req_handler.id
                self._logger.debug(f"Activating {handler_id} for {self._source}")

        elif req_action == 'deactivate':
            self._logger.debug(f"Deactivating {msg_json['handler']} for {self._source}")
            handler_timer = self._active_timers.pop(msg_json['handler'])
            handler_timer.stop()
        else:
            self._logger.error(f"Unknown request action: {req_action}")

    async def send_message(self, payload, is_binary=False):
        self._logger.info(f"Sending {len(payload)} bytes to {self._source}")
        super(MetrStreamProtocol, self).send_message(payload, is_binary=is_binary)

    async def on_close(self):
        for timer in self._active_timers.values():
            timer.stop()

        self._logger.info(f"Connection from {self._source} closed")

    async def fetch_data(self, handler, is_binary=False):
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

        async def do_fetch():
            await self.fetch_data(handler, is_binary=is_binary)

        handler_timer = Timer(do_fetch, handler.data_check_intv(), single_shot=True)
        handler_timer.start()
        self._active_timers[handler.id] = handler_timer

        data_json = json.dumps(req_data).encode('utf-8')
        self.send_message(data_json, is_binary=is_binary)

        proc = multiprocessing.Process(target=handler.post_fetch)
        proc.start()

        return success
