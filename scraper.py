import aiohttp
import asyncio
from urllib.parse import *
import networkx as nx
import aiosqlite
import zlib
from lxml import html
import hashlib
import os
import logging
import time


class Scraper:
    def __init__(self, profile_name, options, db, timestamp):
        assert options.keys() == {"locations", "depth", "method", "same_domain"}
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)

        (
            (_, self.loc),
            (_, self.max_depth),
            (_, self.method),
            (_, self.same_domain),
        ) = options.items()

        self.profile = profile_name
        self.visited_urls = set()
        self.graph = nx.DiGraph()
        self.tasks = []

        self.queue = self.loc
        self.new_queue = []

        self.url_data = []

        self.conn = None
        self.db = db

        self.timestamp = timestamp

    def fetch_links(self, root_url, resp_text):
        links = []
        root_domain = urlparse(root_url).netloc

        tree = html.fromstring(resp_text)
        for a_tag in tree.xpath("//a"):
            href = a_tag.get("href")
            if not href:
                continue

            domain = urlparse(href).netloc
            if not domain:
                href = urljoin(root_url, href, allow_fragments=True)
            elif self.same_domain and domain != root_domain:
                continue

            links.append(href)
        return links

    async def store_graph(self):
        try:
            nx.nx_agraph.write_dot(self.graph, f'graphs/{self.timestamp}/{self.profile}.dot')
        except Exception as err:
            self.logger.error("failed to store_graph: %s", err)

    async def store_db(self):
        self.logger.debug('%s store_db', self.profile)
        self.conn = await aiosqlite.connect(self.db)
        await self.conn.execute(
            f"CREATE TABLE IF NOT EXISTS {self.profile} (url TEXT, time INT, hash VARCHAR(64))",
        )

        for data in self.url_data:
            await self.conn.execute(f'INSERT INTO {self.profile} VALUES (?, ?, ?)', data)
        await self.conn.commit()
        await self.conn.close()

    async def _crawl(self, url):
        self.visited_urls.add(url)
        self.logger.debug("visiting: %s", url)
        try:
            content = await self.fetch_page(url)
            links = self.fetch_links(url, content)

            cdata = zlib.compress(content.encode())
            hash = hashlib.sha1(cdata)
            self.url_data.append(
                [url, self.timestamp, hash.hexdigest()]
            )

            with open(f'data/{hash.hexdigest()}', 'wb+') as fd:
                fd.write(cdata)

            self.graph.add_edges_from([(url, l) for l in links])
            self.new_queue.extend(links)
        except Exception as err:
            self.logger.error(err)
            self.graph.add_edge(url, f"ERROR {err}")

    async def crawl(self):
        for _ in range(self.max_depth):
            for url in self.queue:
                if url in self.visited_urls:
                    continue

                self.tasks.append(self._crawl(url))

            await asyncio.gather(*self.tasks)
            self.tasks.clear()

            self.queue = self.new_queue.copy()
            self.new_queue.clear()

        await asyncio.gather(
            self.store_db(), self.store_graph()
        )


    async def fetch_page(self, root_url):
        self.logger.debug("fetch page: %s", root_url)
        async with aiohttp.ClientSession() as session:
            async with session.get(root_url) as resp:
                return await resp.text()
