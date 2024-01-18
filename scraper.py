from typing import Coroutine, List, Set
import asyncio
from urllib.parse import urldefrag, urljoin, urlparse
import networkx as nx
import aiosqlite
import zlib
from lxml import html
import hashlib
import logging
import time
import grequests
import sqlalchemy


class Scraper:
    def __init__(self, profile_name: str, options, db, timestamp, debug=False) -> None:
        assert options.keys() == {"locations", "depth", "method", "same_domain"}
        self.logger = logging.getLogger(__name__)

        if debug:
            self.logger.setLevel(logging.DEBUG)
        else:
            self.logger.setLevel(logging.INFO)

        (
            (_, self.loc),
            (_, self.max_depth),
            (_, self.method),
            (_, self.same_domain),
        ) = options.items()

        self.profile: str      = profile_name
        self.visited_urls: set = set()
        self.graph: nx.DiGraph = nx.DiGraph()
        self.tasks: List[Coroutine] = []

        self.queue = set(self.loc)
        self.new_queue = set()

        self.url_table = []

        self.conn = None
        self.db = db

        self.timestamp = timestamp
        self.current_time = int(time.time())

    def fetch_links(self, root_url: str, resp_text: str) -> Set:
        links = set()

        root_domain = urlparse(root_url).netloc

        for a_tag in html.fromstring(resp_text).xpath("//a"):
            href = a_tag.get("href")
            if not href:
                continue

            link = href
            link = urljoin(root_url, href)

            if self.same_domain and urlparse(link).netloc != root_domain:
                continue

            link, _ = urldefrag(link)
            links.add(link)

        return links

    async def store_graph(self):
        try:
            import pygraphviz
            nx.nx_agraph.write_dot(
                self.graph, f"graphs/{self.timestamp}/{self.profile}.dot"
            )
        except Exception as err:
            self.logger.error("failed to store_graph: %s", err)

    async def store_db(self):
        # TODO: replace with sql alchemy
        self.logger.debug("%s store_db", self.profile)
        self.conn = await aiosqlite.connect(self.db)
        await self.conn.execute(
            f"CREATE TABLE IF NOT EXISTS {self.profile} (url TEXT, time INT, hash VARCHAR(64))",
        )

        for data in self.url_table:
            await self.conn.execute(
                f"INSERT INTO {self.profile} VALUES (?, ?, ?)", data
            )
        await self.conn.commit()
        await self.conn.close()

    async def crawl_worker(self, url: str, session: grequests.Session):
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
        t_start = time.time()

        session = grequests.Session()

        for _ in range(self.max_depth):
            for url in self.queue:
                self.tasks.append(self.crawl_worker(url, session))

            await asyncio.gather(*self.tasks)
            self.tasks.clear()

            self.queue = self.new_queue - self.visited_urls
            self.new_queue.clear()

        print(f"[{self.profile}] Crawled: {len(self.visited_urls)} URLs in {time.time() - t_start:.2f} seconds")
        print(f"[{self.profile}] queue size: {len(self.queue)}")

        await asyncio.gather(self.store_db(), self.store_graph())

    async def fetch_page(self, session: grequests.Session, root_url: str):
        self.logger.debug("fetch page: %s", root_url)
        # async with aiohttp.ClientSession() as session:
        # async with session.get(root_url) as resp:
        #     return await resp.text()
        return session.get(root_url).text
