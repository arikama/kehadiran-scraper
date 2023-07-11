import asyncio
import datetime as dt
import logging
from os import environ
import os
from random import random
import typing as t

import aiofiles
import aiohttp

# SCRAPE_CHUNK_SIZE = int(environ.get("SCRAPE_CHUNK_SIZE", 50))
SCRAPE_CHUNK_SIZE = int(environ.get("SCRAPE_CHUNK_SIZE", 5))
SLEEP_S = float(environ.get("SLEEP_S", 0.5))
DISCONNECT_BACKOFF_S = float(environ.get("DISCONNECT_BACKOFF_S", 10))
MIN_DATE = dt.datetime.strptime(
    environ.get("MIN_DATE", "1959-09-11"), "%Y-%m-%d"
).date()
MAX_DATE_DEFAULT = dt.datetime.strftime(dt.datetime.today(), "%Y-%m-%d")
MAX_DATE = dt.datetime.strptime(
    environ.get("MAX_DATE", MAX_DATE_DEFAULT), "%Y-%m-%d"
).date()
URL_PATTERN = "https://www.parlimen.gov.my/files/hindex/pdf/DR-{day}{month}{year}.pdf"
OUTPUT_DIR = "output/"
BLACKLIST_FILENAME = ".blacklist"
LOG_LEVEL = environ.get("LOG_LEVEL", "DEBUG")

T = t.TypeVar("T")

logger = logging.getLogger("kehadiran_scraper.main")


async def download(
    url: str,
    session: aiohttp.ClientSession,
) -> tuple[t.Optional[bytes], t.Optional[str]]:
    async with session.get(url) as response:
        data: bytes = await response.read()
        if b"File does not exist" in data:
            return (None, url)
        return data, url


async def save(data: bytes, url: str, date: dt.date):
    filename: str = OUTPUT_DIR + dt.date.strftime(date, "%Y-%m-%d") + ".pdf"
    async with aiofiles.open(filename, mode="wb") as fo:
        await fo.write(data)


async def scrape(min_date: dt.date, max_date: dt.date, visited_dates: list[dt.date]):
    blacklist: set[dt.date] = await _get_blacklist_dates_from_file(
        create_if_not_exist=True
    )

    async with aiohttp.ClientSession() as session:
        urls_iter: t.Iterator[tuple[str, dt.date]] = _url_generator(
            min_date=min_date,
            max_date=max_date,
            blacklist=blacklist,
        )

        for urls in _chunker(urls_iter, size=SCRAPE_CHUNK_SIZE):
            url_to_date: dict[str, dt.date] = {}
            download_tasks = []
            for i, (url, cur_date) in enumerate(urls):
                if i != 0:
                    random_delay_s: float = random() * SLEEP_S
                    task = download(url=url, session=session)
                    task = _stagger(task, t_s=random_delay_s)
                else:
                    task = download(url=url, session=session)
                download_tasks.append(task)
                url_to_date[url] = cur_date

            logger.info(
                "Downloading files from %s urls, from %s to %s",
                *(
                    len(download_tasks),
                    url_to_date[urls[0][0]],
                    url_to_date[urls[-1][0]],
                ),
            )
            files = await asyncio.gather(*download_tasks)

            save_tasks = []
            blacklist_new = set()
            for data, url in files:
                cur_date = url_to_date[url]
                if data is None:
                    blacklist_new.add(cur_date)
                    continue
                save_tasks.append(save(data=data, url=url, date=cur_date))
            blacklist.update(blacklist_new)

            logger.info(
                "Saving %s downloaded files & adding %s dates to blacklist",
                *(len(save_tasks), len(blacklist_new)),
            )
            await asyncio.gather(
                *save_tasks,
                _update_blacklist_to_file(blacklist=blacklist),
            )
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
                logger.warning(
                    "Server disconnected error. Trying again after %s seconds",
                    DISCONNECT_BACKOFF_S,
                )
                await asyncio.sleep(DISCONNECT_BACKOFF_S)
                max_date = min(visited_dates)
            else:
                logger.info("All dates have been scraped. Exiting with success.")
                return

    asyncio.run(_keep_retrying())


def _chunker(it: t.Iterator[T], size: int) -> t.Iterator[list[T]]:
    chunk = []
    for item in it:
        chunk.append(item)
        if len(chunk) == size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def _url_generator(
    min_date: dt.date,
    max_date: dt.date,
    blacklist: set[dt.date],
) -> t.Iterator[tuple[str, dt.date]]:
    days = (max_date - min_date).days
    for i in range(days):
        cur_date: dt.date = max_date - dt.timedelta(days=i)
        if cur_date in blacklist:
            logger.debug("%s is blacklisted, skipping", cur_date)
            continue
        url: str = URL_PATTERN.format(
            day=str(cur_date.day).zfill(2),
            month=str(cur_date.month).zfill(2),
            year=cur_date.year,
        )
        yield url, cur_date


async def _get_blacklist_dates_from_file(
    create_if_not_exist: bool = False,
) -> set[dt.date]:
    s: set[dt.date] = set()
    if create_if_not_exist:
        if not os.path.exists(BLACKLIST_FILENAME):
            # Open and close immediately to create new empty file
            f = await aiofiles.open(BLACKLIST_FILENAME, mode="w")
            await f.close()

    async with aiofiles.open(BLACKLIST_FILENAME, mode="r") as fi:
        async for line in fi:
            line = line.strip("\n")
            date = dt.date.fromisoformat(line.strip("\n"))
            s.add(date)
    return s


async def _update_blacklist_to_file(blacklist: set[dt.date]) -> None:
    blacklist_existing: set[dt.date] = await _get_blacklist_dates_from_file()
    blacklist = blacklist - blacklist_existing
    async with aiofiles.open(BLACKLIST_FILENAME, mode="a") as fo:
        blacklist_sorted = "\n".join(sorted((str(d) for d in blacklist), reverse=True))
        if blacklist_existing and blacklist_sorted:
            blacklist_sorted = "\n" + blacklist_sorted
        await fo.write(blacklist_sorted)


async def _stagger(task: t.Awaitable[T], t_s: float) -> T:
    await asyncio.sleep(t_s)
    ret: T = await task
    return ret


if __name__ == "__main__":
    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL),
    )
    main()
