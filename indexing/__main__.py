import argparse
import os
import sys
from indexer import IndexManager
import logging

sys.path.extend([os.getcwd()])
from models import *

LOGGING_FORMAT = "[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s"

def main(args):
    crawlopts = CrawlConfig.load_config(args.config, make_dirs=False)

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

    manager = IndexManager(crawlopts, args.profile)
    manager.process()
    
    index_dst = os.path.join(crawlopts.index_dir, f"{args.profile}.pkl")
    manager.save(index_dst)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("-config", required=True, help="Path to the config file")
    parser.add_argument("-profile", required=True, help="Name of the profile")

    args = parser.parse_args()
    main(args)