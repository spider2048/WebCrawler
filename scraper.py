import requests
import bs4
import asyncio
import aiohttp

class Scraper:
    def __init__(self, options):
        assert options.keys() == \
            {'locations', 'depth', 'method', 'same_domain'}

        (_, self.loc), (_, self.depth), \
        (_, self.method), (_, self.same_domain) = options.items()

        self.visited_urls = {}
        self.stack = []
        self.session = aiohttp.ClientSession()

    async def _fetch_page(self, url):
        async with self.session.get(url) as resp:
            return resp.text()


    def crawl(self):
        pass
