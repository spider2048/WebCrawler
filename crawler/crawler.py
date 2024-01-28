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
        timestamp = time.asctime().replace(":", "-")

        crawl_options = options["crawl_options"]
        assert crawl_options.keys() == {
            "log_file",
            "database_location",
            "debug",
            "cache_dir",
            "graph_dir",
        }

        if crawl_options["debug"]:
            self.logger.setLevel(logging.DEBUG)
        else:
            self.logger.setLevel(logging.INFO)

        if not os.path.exists(crawl_options["graph_dir"]):
            os.mkdir(crawl_options["graph_dir"])

        graph_ts_dir = os.path.join(crawl_options["graph_dir"], timestamp)
        if not os.path.exists(graph_ts_dir):
            os.mkdir(graph_ts_dir)

        if not os.path.exists(crawl_options["cache_dir"]):
            os.mkdir(crawl_options["cache_dir"])

        crawl_options["timestamp"] = timestamp

        profiles = options["profiles"]
        self.logger.info("Checking profiles [%d profile(s)]", len(profiles))
        for it, profile in enumerate(profiles, start=1):
            self.logger.info("%d> %s", it, profile)
            scraper = Scraper(profile, profiles[profile], crawl_options)
            self.futures.append(scraper.crawl())

    async def finish(self):
        await asyncio.gather(*self.futures)
