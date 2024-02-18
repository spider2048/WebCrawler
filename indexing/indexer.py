from sqlalchemy import create_engine

from models import CrawlConfig

class Indexer:
    def __init__(self, crawlopts: CrawlConfig) -> None:
        self.crawlopts = crawlopts
        self.indexopts = None