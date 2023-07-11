# kehadiran-scraper

Scrape the www.parlimen.gov.my website to download the hansard pdf files.

## Development

Make sure that Python is installed correctly.

```bash
python3 --version
python3 -m pip --version
```

Install required packages.

```bash
python3 -m pip install -r requirements.txt
```
Run

```bash
python kehadiran_scraper/main.py
```

If you want, you can also specify these environment variables to run with specific parameters:

```bash
LOG_LEVEL=INFO \
SCRAPE_CHUNK_SIZE=15 \
SLEEP_S=0.25 \
MIN_DATE=2015-01-01 \
MAX_DATE=2023-07-09 \
DISCONNECT_BACKOFF_S=5 \
python kehadiran_scraper/main.py

# LOG_LEVEL: Set the log level (DEBUG, INFO, ERROR)
#
# SLEEP_S: Set the time in seconds to sleep before each chunk
#
# MIN_DATE: Set the min date to scrape. We start scraping
# from MAX_DATE down to MIN_DATE (latest to earliest)
#
# MAX_DATE: Set the max date to scrape. We start scraping
# from MAX_DATE down to MIN_DATE (latest to earliest)
#
# DISCONNECT_BACKOFF_S: Set the time in seconds to backoff and wait before
# trying again  after getting a "Server disconnected" error. On each next try,
# we try again from the last date that failed so we're not going
# to do redundant work
```

The hansard pdf files can found out at `output/` directory. The blacklisted dates will be generated at `.blacklist` file and the file will be populated as the script is running, so that if you run the script again with the same dates, it will run faster. If you ran the script and exited successful, and found out that the files in `output/` have changed, please create a branch, commit these two files and file a PR. This might mean that they have uploaded new or updated files.