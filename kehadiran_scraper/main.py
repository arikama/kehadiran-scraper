import asyncio
import datetime as dt
import logging
from os import environ
import typing as t

import aiofiles
import aiohttp

SCRAPE_CHUNK_SIZE = int(environ.get("SCRAPE_CHUNK_SIZE", 50))
SLEEP_S = float(environ.get("SLEEP_S", 0.5))
MIN_DATE = dt.datetime.strptime(environ.get("MIN_DATE", "1959-09-11"), "%Y-%m-%d").date()
MAX_DATE = dt.datetime.strptime(environ.get("MAX_DATE", dt.datetime.today()), "%Y-%m-%d").date()
USE_WHITELIST = bool(int(environ.get("USE_WHITELIST", "0")))
URL_PATTERN = "https://www.parlimen.gov.my/files/hindex/pdf/DR-{day}{month}{year}.pdf"
OUTPUT_DIR = "output/"
WHITE_LIST_FILENAME = "whitelist-dates"
LOG_LEVEL = environ.get("LOG_LEVEL", "DEBUG")

T = t.TypeVar("T")

logger = logging.getLogger("kehadiran_scraper.main")


def _chunker(it: t.Iterator[T], size: int) -> t.Iterator[t.Iterator[T]]:
    chunk = []
    for item in it:
        chunk.append(item)
        if len(chunk) == size:
            yield iter(chunk)
            chunk = []
    if chunk:
        yield iter(chunk)


def _url_generator(
    min_date: dt.date,
    max_date: dt.date,
    whitelist: set[dt.date],
    use_whitelist: bool,
) -> t.Iterator[tuple[str, dt.date]]:
    days = (max_date - min_date).days
    for i in range(days):
        cur_date: dt.date = max_date - dt.timedelta(days=i)
        if use_whitelist:
            if cur_date not in whitelist:
                continue
        url: str = URL_PATTERN.format(
            day=str(cur_date.day).zfill(2),
            month=str(cur_date.month).zfill(2),
            year=cur_date.year,
        )
        yield url, cur_date


async def _get_whitelist_dates_from_file() -> set[dt.date]:
    s: set[dt.date] = set()
    async with aiofiles.open(WHITE_LIST_FILENAME, mode="r") as fi:
        async for line in fi:
            date = dt.date.fromisoformat(line.strip("\n"))
            s.add(date)
    return s


async def _update_whitelist_to_file(whitelist: set[dt.date]) -> None:
    async with aiofiles.open(WHITE_LIST_FILENAME, mode="w") as fo:
        whitelist_sorted = "\n".join(sorted(str(d) for d in whitelist))
        await fo.write(whitelist_sorted)


async def download(
    url: str,
    session: aiohttp.ClientSession,
) -> tuple[t.Optional[bytes], t.Optional[str]]:
    logger.debug("Downloading from %s", url)
    async with session.get(url) as response:
        data: bytes = await response.read()
        if b"File does not exist" in data:
            logger.debug("Url %s does not contain file", url)
            return (None, url)
        return data, url


async def save(data: bytes, url: str, date: dt.date):
    filename: str = OUTPUT_DIR + dt.date.strftime(date, "%Y-%m-%d") + ".pdf"
    logger.info("Found file from %s, dumping to file %s", url, filename)
    async with aiofiles.open(filename, mode="wb") as fo:
        await fo.write(data)


async def scrape(min_date: dt.date, max_date: dt.date, visited_dates: list[dt.date]):
    whitelist: set[dt.date] = await _get_whitelist_dates_from_file()

    async with aiohttp.ClientSession() as session:
        urls_iter: t.Iterator[tuple[str, dt.date]] = _url_generator(
            min_date=min_date,
            max_date=max_date,
            whitelist=whitelist,
            use_whitelist=USE_WHITELIST,
        )

        for urls in _chunker(urls_iter, size=SCRAPE_CHUNK_SIZE):
            url_to_date: dict[str, dt.date] = {}
            download_tasks = []
            for url, cur_date in urls:
                task = download(url=url, session=session)
                download_tasks.append(task)
                url_to_date[url] = cur_date
            files = await asyncio.gather(*download_tasks)

            save_tasks = []
            for data, url in files:
                if data is None:
                    continue
                cur_date = url_to_date[url]
                save_tasks.append(save(data=data, url=url, date=cur_date))
                whitelist.add(cur_date)
            await asyncio.gather(*save_tasks, asyncio.sleep(SLEEP_S))
            await _update_whitelist_to_file(whitelist=whitelist)
            for cur_date in url_to_date.values():
                visited_dates.append(cur_date)


def main():

    async def _keep_retrying():

        visited_dates = []
        min_date = MIN_DATE
        max_date = MAX_DATE

        while True:
            try:
                await scrape(
                    min_date=min_date,
                    max_date=max_date,
                    visited_dates=visited_dates,
                )
            except aiohttp.ServerConnectionError:
                t_s = 5
                logger.info("Server disconnected error. Trying again after %s seconds", t_s)
                await asyncio.sleep(t_s)
                max_date = min(visited_dates)
            except Exception as exception:
                logger.error("Unexpected error occured, exiting. Error: %s", exception)
                return
            else:
                logger.info("All dates have been scraped. Exiting with success.")
                return

    asyncio.run(_keep_retrying())


if __name__ == "__main__":
    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL),
    )
    main()
