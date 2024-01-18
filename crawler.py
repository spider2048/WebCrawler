import asyncio
import logging
import os
import time
from typing import List

from scraper import Scraper


class Crawler:
    def __init__(self, options):
        self.logger = logging.getLogger(__name__)
        self.futures: List = []
        self.timestamp = time.asctime().replace(":", "-")

        crawl_options = options["crawl_options"]
        assert crawl_options.keys() == {"log_file", "database_location", "debug"}

        if crawl_options["debug"]:
            self.logger.setLevel(logging.DEBUG)
        else:
            self.logger.setLevel(logging.INFO)

        if not os.path.exists("graphs"):
            os.mkdir("graphs")

        if not os.path.exists(f"graphs/{self.timestamp}"):
            os.mkdir(f"graphs/{self.timestamp}")

        if not os.path.exists("data"):
            os.mkdir("data")

        profiles = options.get("profiles")

        self.logger.info("Checking profiles [%d profile(s)]", len(profiles))
        for it, profile in enumerate(profiles, start=1):
            self.logger.info("%d> %s", it, profile)
            scraper = Scraper(
                profile,
                profiles[profile],
                crawl_options["database_location"],
                self.timestamp,
            )
            self.futures.append(scraper.crawl())

    async def finish(self):
        await asyncio.gather(*self.futures)
