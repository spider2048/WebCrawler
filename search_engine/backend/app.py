import argparse
from flask import Flask, jsonify

import sys
import os
sys.path.extend([os.getcwd()])
from models import *

def main(args) -> None:
    crawlopts: CrawlConfig = CrawlConfig.load_config(args.config)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("-config", help="Path to the config file", required=True)
    args = parser.parse_args()

    main(args)