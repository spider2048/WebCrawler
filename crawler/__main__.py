import argparse
import asyncio
import logging
import toml
import os
import cProfile
from worker import Crawler
from models import *

LOGGING_FORMAT = "[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s"

def main(args):
    logging.info("Crawler running in: %s", os.getcwd())
    with open(args.config) as fd:
        config = toml.load(fd)

    crawlopts = CrawlConfig(config["crawl_options"])
    profileopts = [
        ProfileConfig(name, opts) for (name, opts) in config["profiles"].items()
    ]

    if crawlopts.debug:
        logging.basicConfig(
            filename=crawlopts.log_file,
            level=logging.DEBUG,
            force=True,
            format=LOGGING_FORMAT,
        )
    else:
        logging.basicConfig(
            filename=crawlopts.log_file,
            level=logging.INFO,
            force=True,
            format=LOGGING_FORMAT,
        )

    logger = logging.getLogger("CrawlerMain")
    logger.info("Preparing to crawl ...")

    profiler = cProfile.Profile()

    if crawlopts.profile:
        logging.info("Profiling (profile = true) ...")
        profiler.enable()

    c = Crawler(crawlopts, profileopts)
    asyncio.run(c.run())

    if crawlopts.profile:
        profiler.disable()
        logging.info("Stopping profiler and saving stats to 'profile.perf'")
        profiler.dump_stats("profile.perf")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-config", required=True)
    args = parser.parse_args()
    main(args)
