
import logging
import asyncio

from autobahn.asyncio.websocket import WebSocketServerFactory

from protocols.hollaback import HollaBackProtocol


def main():
    port = 8001
    logging.basicConfig(format="%(levelname)s|%(name)s|%(asctime)-15s: %(message)s")
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    factory = WebSocketServerFactory()
    factory.protocol = HollaBackProtocol

    loop = asyncio.get_event_loop()
    coro = loop.create_server(factory, '127.0.0.1', port)
    server = loop.run_until_complete(coro)

    try:
        logger.info(f"Listening on port {port}")
        loop.run_forever()
    except KeyboardInterrupt:
        print()
    finally:
        server.close()
        loop.close()


if __name__ == "__main__":
    main()
