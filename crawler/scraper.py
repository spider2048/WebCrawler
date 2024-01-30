import asyncio
import aiohttp
import hashlib
import logging
import os
import sys
import zlib
from typing import Coroutine, List, Set, Optional
from urllib.parse import urldefrag, urljoin, urlparse

from sqlalchemy.ext.asyncio import AsyncSession

sys.path.extend([os.getcwd()])
from models import *

import aiofiles
import networkx as nx
import ujson as json  # Faster JSON
from lxml import html


class Scraper:
    def __init__(
        self,
        profile_config: ProfileConfig,
        crawl_config: Config,
        session: AsyncSession
    ) -> None:
        # Logging
        self.logger = logging.getLogger(__name__)
        self.crawlopts = crawl_config

        if self.crawlopts.debug:
            self.logger.setLevel(logging.DEBUG)
        else:
            self.logger.setLevel(logging.INFO)

        # Parse/Get options
        self.profile: ProfileConfig = profile_config

        # Graphing
        self.graph: nx.DiGraph = nx.DiGraph()
        self.tasks: List[Coroutine] = []

        # URL Queues (sets)
        self.queue: Set[str] = set(self.profile.locations)
        self.new_queue: Set[str] = set()
        self.visited_urls: Set[str] = set()
        self.url_table: List[str] = []

        # Database (SQLAlchemy)
        self.session: AsyncSession = session

    async def fetch_links(self, root_url: str, resp_text: str) -> Set[str]:
        links: Set[str] = set()
        root_domain: str = urlparse(root_url).netloc
        for a_tag in html.fromstring(resp_text).xpath("//a"):
            href: Optional[str] = a_tag.get("href")
            if not href:
                continue
            link: str = urljoin(root_url, href)

            # Filter cross-site links
            if self.profile.same_domain and urlparse(link).netloc != root_domain:
                continue
            link, _ = urldefrag(link)  # Remove fragments
            links.add(link)
        return links

    async def store_graph(self) -> None:
        try:
            self.logger.info("stored data in graph")
            with open(
                os.path.join(self.crawlopts.graph_ts_dir, f"{self.profile.profile_name}.json"), "w+"
            ) as fd:
                json.dump(nx.node_link_data(self.graph), fd)
        except Exception as err:
            self.logger.error("failed to store graph: %s", err)

    async def cache_page(self, hash_str: str, data: bytes):
        async with aiofiles.open(
            os.path.join(self.crawlopts.cache_dir, hash_str), "wb+"
        ) as fd:
            await fd.write(data)

    async def crawl_worker(self, url: str, websession: aiohttp.ClientSession):
        self.visited_urls.add(url)
        self.logger.debug("visiting: %s", url)

        try:
            # Fetch page and links
            content: str = await self.fetch_page(websession, url)
            links: Set[str] = await self.fetch_links(url, content)

            # Compress page and calculate hash
            cdata: bytes = zlib.compress(content.encode())
            hash_str: str = hashlib.sha1(cdata).hexdigest()

            # Schedule to save files
            self.profile.file_save_tasks.append(self.cache_page(hash_str, cdata))

            # Add page data to database
            self.session.add(
                URLData(
                    url=url,
                    profile_name=self.profile.profile_name,
                    time=self.crawlopts.unix_time,
                    hash=hash_str,
                )
            )

            # Add links to graph
            self.graph.add_edges_from((url, l) for l in links)

            # Update next queue
            self.new_queue.update(links)
        except Exception as err:
            self.logger.error(err)  
            self.graph.add_edge(url, f"ERROR {err}")

    async def crawl(self):
        # Start crawling
        websession = aiohttp.ClientSession()
        for _ in range(self.profile.depth):
            self.tasks.extend(self.crawl_worker(url, websession) for url in self.queue)

            await asyncio.gather(*self.tasks)
            self.tasks.clear()

            self.queue = self.new_queue - self.visited_urls
            self.new_queue.clear()

        self.logger.info("[%s] Crawled: %d URLs", self.profile.profile_name, len(self.visited_urls))
        self.logger.info("[%s] queue size: %d", self.profile.profile_name, len(self.queue))

        # Store graph
        await self.store_graph()
        
        # Cleanup Web session
        await websession.close()

    async def fetch_page(self, websession: aiohttp.ClientSession, root_url: str):
        self.logger.debug("fetch page: %s", root_url)
        async with websession.get(root_url) as resp:
            return await resp.text()
