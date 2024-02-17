import asyncio
import aiohttp
import hashlib
import logging
import os
import sys
import zlib
from typing import Coroutine, List, Set

from sqlalchemy.ext.asyncio import AsyncSession

sys.path.extend([os.getcwd()])
from models import *
from page_utils import *
from graphing import Graph


class Scraper:
    def __init__(
        self,
        profile: ProfileConfig,
        crawlopts: CrawlConfig,
        session: AsyncSession,
    ) -> None:
        # Logging
        self.logger = logging.getLogger(__name__)
        self.crawlopts = crawlopts

        if self.crawlopts.debug:
            self.logger.setLevel(logging.DEBUG)
        else:
            self.logger.setLevel(logging.INFO)

        # Parse/Get options
        self.profile: ProfileConfig = profile

        # Graphing
        self.graph: Graph = Graph()
        self.tasks: List[Coroutine] = []

        # URL Queues (sets)
        self.queue: Set[str] = set(self.profile.locations)
        self.new_queue: Set[str] = set()
        self.visited_urls: Set[str] = set()

        # Database (SQLAlchemy)
        self.session: AsyncSession = session

    async def crawl_worker(self, url: str, websession: aiohttp.ClientSession):
        self.visited_urls.add(url)
        self.logger.debug("Visiting %s", url)

        try:  # TODO: Find a better alternative
            # Fetch page and links
            links, hash_str = await Page.parse(
                url, websession, self.crawlopts.cache_dir, self.profile
            )

            # Add page data to database
            self.session.add(
                URLData(
                    url=url,
                    profile_name=self.profile.profile_name,
                    time=self.crawlopts.unix_time,
                    hash=hash_str,
                )
            )

            self.graph.update_edges(url, links)
            self.new_queue.update(links)
        except Exception as err:
            self.logger.error("Error (%s) crawling url %s", err, url)
            self.graph.update_edges(url, [f"ERROR {err}"])

    async def crawl(self):
        # Start crawling
        websession = aiohttp.ClientSession()
        for _ in range(self.profile.depth):
            self.tasks.extend(self.crawl_worker(url, websession) for url in self.queue)

            await asyncio.gather(*self.tasks)
            self.tasks.clear()

            self.queue = self.new_queue - self.visited_urls
            self.new_queue.clear()

        self.logger.info(
            "[%s] Crawled: %d URLs", self.profile.profile_name, len(self.visited_urls)
        )
        self.logger.info(
            "[%s] queue size: %d", self.profile.profile_name, len(self.queue)
        )

        # Store graph
        await self.graph.save(
            os.path.join(
                self.crawlopts.graph_ts_dir, f"{self.profile.profile_name}.json"
            )
        )

        # Cleanup Web session
        await websession.close()
