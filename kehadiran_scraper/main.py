import asyncio
import datetime as dt
import logging
import os
import typing as t

import aiofiles
import aiohttp

# TODO: Read from env var for some of these
SCRAPE_CHUNK_SIZE = 50
SLEEP_S = 1
FIRST_DATE = dt.date(1959, 9, 11)
URL_PATTERN = "https://www.parlimen.gov.my/files/hindex/pdf/DR-{day}{month}{year}.pdf"
TEST_OUTPUT_DIR = ".test.out/"

T = t.TypeVar("T")

logger = logging.getLogger(__name__)


def _chunker(it: t.Iterator[T], size: int) -> t.Iterator[tuple[T]]:
    chunk = []
    for item in it:
        chunk.append(item)
        if len(chunk) == size:
            yield tuple(chunk)
            chunk = []
    if chunk:
        yield tuple(chunk)


def _url_generator(start: dt.date) -> t.Iterator[str]:
    today = dt.date.today()
    days = (today - start).days
    for i in range(days):
        cur_date: dt.date = start + dt.timedelta(days=i)
        url: str = URL_PATTERN.format(
            day=str(cur_date.day).zfill(2),
            month=str(cur_date.month).zfill(2),
            year=cur_date.year,
        )
        yield url


async def download(
    url: str,
    session: aiohttp.ClientSession,
) -> tuple[t.Optional[bytes], t.Optional[str]]:
    logger.info("Downloading from %s", url)
    async with session.get(url) as response:
        data: bytes = await response.read()
        if b"File does not exist" in data:
            # TODO: Collect url in a blacklist set. Later we can dump into file
            logger.error("Url %s does not contain file. Need to blacklist", url)
            return (None, None)
        return data, url


async def save(data: bytes, url: str):
    # TODO: Write to S3
    filename: str = TEST_OUTPUT_DIR + url.split("/")[-1]
    logger.info("Found file from %s, dumping to file %s", url, filename)
    async with aiofiles.open(filename, mode="wb") as fo:
        await fo.write(data)


async def scrape():
    async with aiohttp.ClientSession() as session:
        # TODO: Skip url found in blacklist file (Or use whitelist instead)
        for urls in _chunker(_url_generator(start=FIRST_DATE), size=SCRAPE_CHUNK_SIZE):
            download_tasks = [download(url=url, session=session) for url in urls]
            files = await asyncio.gather(*download_tasks)

            save_tasks = [
                save(data=data, url=url)
                for data, url in files
                if data is not None and url is not None
            ]
            await asyncio.gather(*save_tasks)
            await asyncio.sleep(SLEEP_S)


def main():
    os.makedirs(TEST_OUTPUT_DIR, exist_ok=True)
    asyncio.run(scrape())


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.DEBUG,
    )
    main()
