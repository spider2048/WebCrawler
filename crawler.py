from scraper import Scraper
import logging


class Crawler:
    def __init__(self, options):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        self.scrapers = []

        assert options["crawl_options"].keys() == {
            "log_file",
            "database_location",
            "max_workers",
        }

        profiles = options.get("profiles")
        self.logger.info("Checking profiles [%d profiles]", len(profiles))
        for it, profile in enumerate(profiles):
            self.logger.info("%d> %s", it + 1, profile)
            scraper = Scraper(profiles[profile])
            self.scrapers.append(
                scraper
            )
