import argparse
from models import *

def main(args):
    crawlopts = CrawlConfig.load_config(args.config)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("-config", required=True, help="Path to the config file")
    parser.add_argument("-profile", required=True, help="Name of the profile")

    args = parser.parse_args()
    main(args)