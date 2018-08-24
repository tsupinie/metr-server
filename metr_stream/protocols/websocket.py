
import aiohttp

class WebSocketProtocol(object):
    _connections = []

    def __init__(self):
        self._ws = None

    async def on_connect(self, req):
        pass

    async def on_message(self, msg):
        pass

    async def send_message(self, msg, is_binary=False):
        if self._ws is None:
            raise ValueError("Open a connection before sending a message!")

        if is_binary:
            await self._ws.send_bytes(msg)
        else:
            await self._ws.send_str(msg)

    async def on_close(self):
        pass

    @staticmethod
    async def on_shutdown(app):
        for ws in WebSocketProtocol._connections:
            await ws.close(code=aiohttp.WSCloseCode.GOING_AWAY, 
                           message='Server is shutting down')

    async def __call__(self, request):
        self._ws = aiohttp.web.WebSocketResponse()
        await self._ws.prepare(request)

        WebSocketProtocol._connections.append(self._ws)

        await self.on_connect(request)

        async for msg in self._ws:
            if msg.type != aiohttp.WSMsgType.TEXT:
                continue

            if msg.data == 'close':
                await ws.close()
            else:
                await self.on_message(msg.data)

        await self.on_close()

        ws = self._ws
        self._ws = None
        WebSocketProtocol._connections.remove(ws)

        return ws
