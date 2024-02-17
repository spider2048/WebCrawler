import logging
import os
from urllib.parse import urldefrag, urljoin, urlparse
from typing import Optional, List, Set, Tuple
import aiofiles
import aiohttp
from lxml import html
import zlib
import hashlib

from models import CrawlConfig, ProfileConfig

logger = logging.getLogger("PageUtils")


class Page:
    @staticmethod
    def links(src: str, document: str) -> Set[str]:
        links: Set[str] = set()
        for tag in html.fromstring(document).xpath("//a"):
            href: Optional[str] = tag.get("href")
            if not href:
                continue
            link, _ = urldefrag(urljoin(src, href))
            links.add(link)
        return links

    @staticmethod
    async def get(websession: aiohttp.ClientSession, url: str) -> str:
        logger.debug("Fetch page %s", url)
        async with websession.get(url) as response:
            return await response.text()

    @staticmethod
    def compress(content: str) -> bytes:
        return zlib.compress(content.encode())

    @staticmethod
    def hash(content: bytes) -> str:
        return hashlib.sha1(content).hexdigest()

    @staticmethod
    async def cache(dest, data):
        if os.path.exists(dest):
            return

        async with aiofiles.open(dest, "wb+") as fd:
            await fd.write(data)

    @staticmethod
    async def parse(
        src: str,
        websession: aiohttp.ClientSession,
        cache_dir: str,
        profile: ProfileConfig,
    ) -> Tuple[Set[str], str]:
        content: str = await Page.get(websession, src)
        links: Set[str] = Page.links(src, content)

        compressed_data: bytes = Page.compress(content)
        hash_str = Page.hash(compressed_data)

        profile.tasks.append(
            Page.cache(os.path.join(cache_dir, hash_str), compressed_data)
        )

        Page.filter(links, profile)
        return links, hash_str

    @staticmethod
    def filter(links: Set[str], profile: ProfileConfig):
        filter_links: List[str] = [link for link in links if not profile.filter(link)]
        for link in filter_links:
            links.remove(link)
