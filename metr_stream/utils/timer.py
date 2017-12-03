
import asyncio
from contextlib import suppress

class Timer(object):
    def __init__(self, callback, period, single_shot=False):
        self._cb = callback
        self._per = period
        self.is_started = False
        self._single = single_shot

    def start(self):
        if not self.is_started:
            self.is_started = True
            self._task = asyncio.ensure_future(self._run())

    def stop(self):
        if self.is_started:
            self.is_started = False
            self._task.cancel()

    async def _run(self):
        if self._single:
            await asyncio.sleep(self._per)
            await self._cb()
            self.stop()
        else:
            while True:
                await asyncio.sleep(self._per)
                await self._cb()


async def main():
    async def t1fire():
        print("Timer 1 called!")
        await asyncio.sleep(0.5)
        print("Timer 1 long task finished!")

    async def t2fire():
        print("Timer 2 called!")

    t1 = Timer(t1fire, 1)

    t1.start()
    await asyncio.sleep(5.1)

    t1.stop()

    t2 = Timer(t2fire, 2, single_shot=True)
    t2.start()
    await asyncio.sleep(4.1)

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
