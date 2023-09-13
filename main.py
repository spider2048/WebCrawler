import argparse
import toml
import logging

from crawler import Crawler

def main(args):
    with open(args.config) as fd:
        config = toml.load(fd)

    if args.debug:
        logging.basicConfig(filename=config['crawl_options']['log_file'],
                            level=logging.DEBUG, force=True)
    else:
        logging.basicConfig(filename=config['crawl_options']['log_file'],
                            level=logging.INFO, force=True)
    
    c = Crawler(config)
    # TODO

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-config', required=True)
    parser.add_argument('-debug', required=False, type=bool)

    args = parser.parse_args()
    main(args)