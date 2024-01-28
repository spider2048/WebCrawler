import asyncio
import aiohttp
import hashlib
import logging
import os
import time
import zlib
from typing import Coroutine, Dict, List, Set, Type, Union, Any, Optional
from urllib.parse import urldefrag, urljoin, urlparse

import aiofiles
import networkx as nx
import ujson as json  # Faster JSON
from lxml import html
from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.asyncio import AsyncSession, AsyncEngine, create_async_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


class Scraper:
    def __init__(
        self,
        profile_name: str,
        profile: Dict[str, Union[str, bool, int]],
        crawl_options,
    ) -> None:
        # Logging
        self.logger = logging.getLogger(__name__)
        if crawl_options["debug"]:
            self.logger.setLevel(logging.DEBUG)
        else:
            self.logger.setLevel(logging.INFO)

        # Crawl options
        self.options: Dict[str, Union[str, bool, int]] = crawl_options
        self.graph_dir: str = os.path.join(
            crawl_options["graph_dir"], crawl_options["timestamp"]
        )

        # Timestamps
        self.current_time = int(time.time())

        # Parse/Get options
        self.profile: str = profile_name
        assert profile.keys() == {"locations", "depth", "method", "same_domain"}
        (
            (_, self.loc),
            (_, self.max_depth),
            (_, self.method),
            (_, self.same_domain),
        ) = profile.items()

        # Graphing
        self.graph: nx.DiGraph = nx.DiGraph()
        self.tasks: List[Coroutine] = []

        # URL Queues (sets)
        self.queue: Set[str] = set(self.loc)
        self.new_queue: Set[str] = set()
        self.visited_urls: Set[str] = set()
        self.url_table: List[str] = []

        # Database (SQLAlchemy)
        self.Base: Any = declarative_base()
        self.engine: AsyncEngine = create_async_engine(
            os.path.join("sqlite+aiosqlite:///", crawl_options["database_location"]),
            echo=crawl_options["debug"],
        )

        self.Ty_UrlData: Type = type(
            "UrlData",
            (self.Base,),
            {
                "__tablename__": self.profile,
                "id": Column(Integer, primary_key=True, autoincrement=True),
                "url": Column(String, nullable=False),
                "time": Column(Integer, nullable=False),
                "hash": Column(String(64), nullable=False),
            },
        )

        session_maker = sessionmaker(bind=self.engine, class_=AsyncSession)
        self.session = session_maker()

        # File cache tasks
        self.file_cache_tasks: List[Coroutine] = []

    async def fetch_links(self, root_url: str, resp_text: str) -> Set[str]:
        links: Set[str] = set()
        root_domain: str = urlparse(root_url).netloc
        for a_tag in html.fromstring(resp_text).xpath("//a"):
            href: Optional[str] = a_tag.get("href")
            if not href:
                continue
            link: str = urljoin(root_url, href)

            # Filter cross-site links
            if self.same_domain and urlparse(link).netloc != root_domain:
                continue
            link, _ = urldefrag(link)  # Remove fragments
            links.add(link)
        return links

    async def store_graph(self) -> None:
        try:
            self.logger.info("stored data in graph")
            with open(os.path.join(self.graph_dir, f"{self.profile}.json"), "w+") as fd:
                json.dump(nx.node_link_data(self.graph), fd)
        except Exception as err:
            self.logger.error("failed to store graph: %s", err)

    async def cache_page(self, hash, data):
        async with aiofiles.open(
            os.path.join(self.options["cache_dir"], hash.hexdigest()), "wb+"
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
            hash: hashlib._Hash = hashlib.sha1(cdata)

            # Schedule to save files
            self.file_cache_tasks.append(self.cache_page(hash, cdata))

            # Add page data to database
            d = self.Ty_UrlData()
            d.url = url
            d.time = self.current_time
            d.hash = hash.hexdigest()
            self.session.add(d)

            # Add links to graph
            self.graph.add_edges_from((url, l) for l in links)

            # Update next queue
            self.new_queue.update(links)
        except Exception as err:
            self.logger.error(err)
            self.graph.add_edge(url, f"ERROR {err}")

    async def crawl(self):
        # Initialize database
        async with self.engine.begin() as conn:
            await conn.run_sync(self.Base.metadata.drop_all)
            await conn.run_sync(self.Base.metadata.create_all)

        # Start crawling
        websession = aiohttp.ClientSession()
        for _ in range(self.max_depth):
            self.tasks.extend(self.crawl_worker(url, websession) for url in self.queue)

            await asyncio.gather(*self.tasks)
            self.tasks.clear()

            self.queue = self.new_queue - self.visited_urls
            self.new_queue.clear()

        self.logger.info("[%s] Crawled: %d URLs", self.profile, len(self.visited_urls))
        self.logger.info("[%s] queue size: %d", self.profile, len(self.queue))

        # Store graph
        await self.store_graph()

        # Commit SQL data
        await self.session.commit()

        # Cleanup SQL session
        await self.session.close()

        # Cleanup Web session
        await websession.close()

        # Save all pending files
        await asyncio.gather(*self.file_cache_tasks)

        self.logger.info("Saved %d files to disk", len(self.file_cache_tasks))

    async def fetch_page(self, session: aiohttp.ClientSession, root_url: str):
        self.logger.debug("fetch page: %s", root_url)
        async with session.get(root_url) as resp:
            return await resp.text()
