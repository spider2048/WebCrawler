import argparse
import os
import sys
from indexer import IndexManager
import logging

sys.path.extend([os.getcwd()])
from models import *


def main(args):
    crawlopts = CrawlConfig.load_config(args.config, make_dirs=False)
    profiles = ProfileConfig.load_profiles(args.config)

    logging.info("Crawler running in: %s", os.getcwd())
    crawlopts = CrawlConfig.load_config(args.config)

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

    manager = IndexManager(crawlopts, profiles)
    manager.process()
    manager.save()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("-config", required=True, help="Path to the config file")

    args = parser.parse_args()
    main(args)
