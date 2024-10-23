import asyncio
import logging
import re
import sys
from typing import IO
import urllib.parse
import urllib.error

import aiofiles
import aiohttp
from aiohttp import ClientSession
import aiohttp.http_exceptions

logging.basicConfig(
    format="%(asctime)s %(levelname)s:%(name)s: %(message)s",
    level=logging.DEBUG,
    datefmt="%H:%M:%S",
    stream=sys.stderr
)

logger = logging.getLogger("areq")
logging.getLogger("chardet.charsetprober").disabled = True

HREF_RE = re.compile(r'href="(.*?)"')


# define the coroutine to fetch content from the urls
async def fetch_html(url:str, session: ClientSession, **kwargs):

    resp = await session.request(method="GET", url=url, **kwargs)
    resp.raise_for_status()
    logger.info("Got response [%s] for URL: %s", resp.status, url)
    html = await resp.text()
    return html 

# define the coroutine that parse the content from the given html
async def parse(url: str, session: ClientSession, **kwargs) -> set:
    """Find HREFs in the HTML of `url`."""

    print(f"Parsing HTML for url: {url}")
    found = set()  # Creates an empty set to store unique links
    try:
        html = await fetch_html(url=url, session=session, **kwargs)  # Fetches the HTML content
    except (aiohttp.ClientError, aiohttp.http_exceptions.HttpProcessingError) as e:
        logger.error(
            "aiohttp exception for %s [%s]: %s",
            url,
            getattr(e, "status", None),
            getattr(e, "message", None),
        )  # Logs HTTP-related exceptions
        return found  # Returns an empty set on error
    except Exception as e:
        logger.exception(
            "Non-aiohttp exception occured:  %s", getattr(e, "__dict__", {})
        )  # Logs non-HTTP exceptions
        return found  # Returns an empty set on error
    else:
        for link in HREF_RE.findall(html):  # Extracts all links using the regular expression
            try:
                abslink = urllib.parse.urljoin(url, link)  # Converts relative URLs to absolute URLs
            except (urllib.error.URLError, ValueError):
                logger.exception("Error parsing URL: %s", link)  # Logs parsing errors
                pass
            else:
                found.add(abslink)  # Adds the link to the set of found links
        logger.info("Found %d links for %s", len(found), url)  # Logs the number of links found
        return found  # Returns the set of links

async def write_one(file:IO, url:str, **kwargs):
    res = await parse(url,**kwargs)
    if not res:
        return None
    async with aiofiles.open(file, "a") as f:
        for p in res:
            await f.write(f"{url}\t{p}\n")
        logger.info("Wrote results for source URL: %s", url)

async def bulk_crawl_and_write(file: IO, urls: set, **kwargs) -> None:
    """Crawl & write concurrently to `file` for multiple `urls`."""
    async with ClientSession() as session:
        tasks = []
        for url in urls:
            tasks.append(
                write_one(file=file, url=url, session=session, **kwargs)
            )
        await asyncio.gather(*tasks)

if __name__ == "__main__":
    import pathlib
    import sys

    assert sys.version_info >= (3, 7), "Script requires Python 3.7+."
    here = pathlib.Path(__file__).parent

    with open(here.joinpath("urls.txt")) as infile:
        urls = set(map(str.strip, infile))

    outpath = here.joinpath("foundurls.txt")
    with open(outpath, "w") as outfile:
        outfile.write("source_url\tparsed_url\n")

    asyncio.run(bulk_crawl_and_write(file=outpath, urls=urls))

    
    
