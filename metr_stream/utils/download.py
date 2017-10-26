
import aiohttp

async def download(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            return await resp.read()

if __name__ == "__main__":
    import asyncio

    async def test_download(url):
        print(await download(url))

    loop = asyncio.get_event_loop()
    task = asyncio.ensure_future(test_download('http://www.autumnsky.us/almanac/OKC_almanac.csv'))
    loop.run_until_complete(asyncio.wait([task]))
