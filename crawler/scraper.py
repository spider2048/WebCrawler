import asyncio
import aiohttp
import logging
import os
import sys
from typing import Coroutine, List, Set, Tuple
import traceback
from sqlalchemy.ext.asyncio import AsyncSession
import itertools

sys.path.extend([os.getcwd()])
from models import *
from page_utils import *
from graphing import Graph

logger: logging.Logger = logging.getLogger("Scraper")


class Scraper:
    def __init__(
        self,
        profile: ProfileConfig,
        crawlopts: CrawlConfig,
        session: AsyncSession,
    ) -> None:
        # Logging
        self.crawlopts = crawlopts

        if self.crawlopts.debug:
            logger.setLevel(logging.DEBUG)
        else:
            logger.setLevel(logging.INFO)

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

    async def crawl_worker(self, url: str, content: str):
        self.visited_urls.add(url)

        # TODO: Find a better alternative
        try:
            # Parse
            links, hash_str, title = await Page.parse(
                url, content, self.crawlopts.cache_dir, self.profile
            )

            # Add page data to database
            self.session.add(
                URLData(
                    url=url,
                    profile_name=self.profile.profile_name,
                    time=self.crawlopts.unix_time,
                    hash=hash_str,
                    title=title,
                )
            )

            self.graph.update_edges(url, links, title)
            self.new_queue.update(links)
        except Exception as err:
            logger.error("Error (%s) crawling url %s", err, url)
            logger.error(traceback.format_exc())
            self.graph.update_edges(url, [f"ERROR {err}"], "<error-title>")

    async def crawl(self):
        websession = aiohttp.ClientSession()

        # Start crawling
        for _ in range(self.profile.depth):
            contents: List[str] = await asyncio.gather(*[
                Page.get(websession, url)
                for url in self.queue
            ])

            self.tasks.extend(
                self.crawl_worker(url, content)
                for (url, content) in zip(self.queue, contents)
            )

            await asyncio.gather(*self.tasks)
            self.tasks.clear()

            self.queue = self.new_queue - self.visited_urls
            self.new_queue.clear()

        logger.info(
            "[%s] Crawled: %d URLs", self.profile.profile_name, len(self.visited_urls)
        )
        logger.info("[%s] queue size: %d", self.profile.profile_name, len(self.queue))

        # Store graph
        await self.graph.save(
            os.path.join(
                self.crawlopts.graph_ts_dir,
                f"{self.profile.profile_name}.json"
            )
        )

        # Cleanup Web session
        await websession.close()
