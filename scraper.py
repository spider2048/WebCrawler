import asyncio
import aiohttp
import hashlib
import logging
import time
import zlib
from typing import Coroutine, List, Set
from urllib.parse import urldefrag, urljoin, urlparse

import networkx as nx
import ujson as json  # Faster JSON
from lxml import html
from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


class Scraper:
    def __init__(self, profile_name: str, options, db, timestamp, debug=False) -> None:
        # Logging
        self.logger = logging.getLogger(__name__)
        if debug:
            self.logger.setLevel(logging.DEBUG)
        else:
            self.logger.setLevel(logging.INFO)

        # Parse/Get options
        assert options.keys() == {"locations", "depth", "method", "same_domain"}
        (
            (_, self.loc),
            (_, self.max_depth),
            (_, self.method),
            (_, self.same_domain),
        ) = options.items()

        self.profile: str = profile_name

        # Graphing
        self.graph: nx.DiGraph = nx.DiGraph()
        self.tasks: List[Coroutine] = []

        # URL Queues (sets)
        self.queue = set(self.loc)
        self.new_queue = set()
        self.visited_urls: set = set()
        self.url_table = []

        # Database (SQLAlchemy)
        self.Base = declarative_base()
        self.engine = create_async_engine(f"sqlite+aiosqlite:///{db}")

        self.Ty_UrlData = type(
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

        self.session_maker = sessionmaker(bind=self.engine, class_=AsyncSession)
        self.db_initialized = False

        # Timestamps
        self.timestamp = timestamp  # For graph's parent folder
        self.current_time = int(time.time())

    def fetch_links(self, root_url: str, resp_text: str) -> Set:
        links = set()
        root_domain = urlparse(root_url).netloc
        for a_tag in html.fromstring(resp_text).xpath("//a"):
            href = a_tag.get("href")
            if not href:
                continue
            link = urljoin(root_url, href)

            # Filter cross-site links
            if self.same_domain and urlparse(link).netloc != root_domain:
                continue
            link, _ = urldefrag(link)  # Remove fragments
            links.add(link)
        return links

    async def store_graph(self):
        try:
            with open(f"graphs/{self.timestamp}/{self.profile}.json", "w+") as fd:
                json.dump(nx.node_link_data(self.graph), fd)
        except Exception as err:
            self.logger.error("failed to store_graph: %s", err)

    async def init_db(self):
        if self.db_initialized:
            return
        async with self.engine.begin() as conn:
            await conn.run_sync(self.Base.metadata.drop_all)
            await conn.run_sync(self.Base.metadata.create_all)
        self.db_initialized = True

    async def store_db(self):
        self.logger.debug("%s store_db", self.profile)
        await self.init_db()
        async with self.session_maker() as session:
            for data in self.url_table:
                d = self.Ty_UrlData()
                d.url, d.time, d.hash = data
                session.add(d)
            await session.commit()

    async def crawl_worker(self, url: str, session: aiohttp.ClientSession):
        self.visited_urls.add(url)
        self.logger.debug("visiting: %s", url)
        try:
            content: str = await self.fetch_page(session, url)
            links: Set[str] = self.fetch_links(url, content)

            cdata = zlib.compress(content.encode())
            hash = hashlib.sha1(cdata)
            self.url_table.append([url, self.current_time, hash.hexdigest()])

            with open(f"data/{hash.hexdigest()}", "wb+") as fd:
                fd.write(cdata)

            self.graph.add_edges_from((url, l) for l in links)
            self.new_queue.update(links)
        except Exception as err:
            self.logger.error(err)
            self.graph.add_edge(url, f"ERROR {err}")

    async def crawl(self):
        session = aiohttp.ClientSession()

        for _ in range(self.max_depth):
            for url in self.queue:
                self.tasks.append(self.crawl_worker(url, session))

            await asyncio.gather(*self.tasks)
            self.tasks.clear()

            self.queue = self.new_queue - self.visited_urls
            self.new_queue.clear()

        print(f"[{self.profile}] Crawled: {len(self.visited_urls)} URLs")
        print(f"[{self.profile}] queue size: {len(self.queue)}")

        await asyncio.gather(self.store_db(), self.store_graph())
        await session.close()

    async def fetch_page(self, session: aiohttp.ClientSession, root_url: str):
        self.logger.debug("fetch page: %s", root_url)
        async with session.get(root_url) as resp:
            return await resp.text()
