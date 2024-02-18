import sys
import argparse
import os
import datetime
import sortedcontainers
from app import create_app

sys.path.extend(os.getcwd())
from models import *


def timestamp_to_unix(timestamp: str) -> float:
    return datetime.datetime.strptime(timestamp, TIMESTAMP_FORMAT).timestamp()


def list_graphs(root):
    folders = os.listdir(root)
    order = sortedcontainers.SortedDict()

    for folder in folders:
        order[timestamp_to_unix(folder)] = folder

    print("Snapshots: ")
    for i, (entry, profile_name) in enumerate(reversed(order.items()), start=1):
        profile_root = os.path.join(root, profile_name)
        profiles = os.listdir(profile_root)

        print(f"[{i}] {order[entry]} {len(profiles)} profiles")
        for profile in profiles:
            print(f"\t {os.path.join(profile_root, profile)}")


def show(path, debug=False):
    path = os.path.abspath(path)
    create_app(path, debug)


def main(args):
    crawlopts = CrawlConfig.load_config(args.config, make_dirs=False)

    if args.list:
        list_graphs(crawlopts.graph_dir)
        return

    if args.show:
        show(args.show, crawlopts.debug)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-config", help="Path to config file", required=True)
    parser.add_argument(
        "-list", help="List all graphs", required=False, action="store_true"
    )
    parser.add_argument("-show", help="View the specified graph", required=False)

    args = parser.parse_args()
    main(args)
