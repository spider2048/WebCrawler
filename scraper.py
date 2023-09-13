import aiohttp
from urllib.parse import *
import networkx as nx
import aiosqlite
import zlib
from lxml import html
import hashlib
import logging


class Scraper:
    def __init__(self, profile_name, options, db):
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
        self.queue = self.loc
        self.graph = nx.DiGraph()

        self.conn = None
        self.db = db

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

    async def store_page(self, url, resp_text):
        hash = hashlib.sha1(zlib.compress(resp_text))
        await self.db.execute("INSERT INTO ? VALUES (?, ?, ?)", )

    def store_graph(self, output_file):
        # TODO: Output better graph (using % similarity, new/old page)
        nx.nx_agraph.write_dot(self.graph, output_file)

    async def crawl(self):
        self.conn = await aiosqlite.connect(self.db)
        await self.conn.execute(
            f"CREATE TABLE IF NOT EXISTS {self.profile} (url TEXT, time INT, hash VARCHAR(64))",
        )

        new_queue = []
        for _ in range(self.max_depth):
            for url in self.queue:
                if url in self.visited_urls:
                    continue

                self.visited_urls.add(url)
                self.logger.debug("crawl: %s", url)
                try:
                    content = await self.fetch_page(url)

                    links = self.fetch_links(url, content)
                    # TODO Store page - LATER -
                    self.graph.add_edges_from([(url, l) for l in links])
                    new_queue.extend(links)
                except Exception as err:
                    self.logger.debug(f"ERROR {err}")
                    self.graph.add_edge(url, f"ERROR {err}")

            self.queue = new_queue.copy()
            new_queue.clear()

        self.store_graph("graph.dot")

    async def fetch_page(self, root_url):
        async with aiohttp.ClientSession() as session:
            async with session.get(root_url) as resp:
                return await resp.text()
