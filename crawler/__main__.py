import argparse
import asyncio
import logging
import toml

from worker import Crawler

def main(args):
    with open(args.config) as fd:
        config = toml.load(fd)

    if config["crawl_options"]["debug"]:
        logging.basicConfig(
            filename=config["crawl_options"]["log_file"],
            level=logging.DEBUG,
            force=True,
        )
    else:
        logging.basicConfig(
            filename=config["crawl_options"]["log_file"], level=logging.INFO, force=True
        )

    logger = logging.getLogger("CrawlerMain")

    logger.info("Preparing to crawl ...")
    c = Crawler(config)
    asyncio.run(c.run())

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-config", required=True)
    args = parser.parse_args()
    main(args)
