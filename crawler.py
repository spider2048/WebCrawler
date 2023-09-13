from scraper import Scraper
import logging
import asyncio
import aiosqlite

class Crawler:
    def __init__(self, options):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        self.futures :list[asyncio.Coroutine] = []

        crawl_options = options["crawl_options"]
        assert crawl_options.keys() == {
            "log_file",
            "database_location",
            "max_workers",
        }

        profiles = options.get("profiles")
        
        self.logger.info("Checking profiles [%d profile(s)]", len(profiles))
        for it, profile in enumerate(profiles):
            self.logger.info("%d> %s", it + 1, profile)
            scraper = Scraper(profile, profiles[profile], crawl_options['database_location'])
            self.futures.append(scraper.crawl())

    async def finish(self):
        return await asyncio.gather(*self.futures)