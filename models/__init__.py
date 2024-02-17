from urllib.parse import urlparse
from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base

import os
import time
import datetime
from typing import Coroutine, List, Set
import re

Base = declarative_base()


class URLData(Base):
    __tablename__: str = "URLProfileData"
    id = Column(Integer, primary_key=True, autoincrement=True)
    url = Column(String, nullable=False)
    profile_name = Column(String, nullable=False)
    time = Column(Integer, nullable=False)
    hash = Column(String(64), nullable=False)


class URLCrawlStats(Base):
    __tablename__: str = "URLCrawlStats"
    id = Column(Integer, primary_key=True, autoincrement=True)
    time = Column(Integer, nullable=False)


class CrawlConfig:
    def __init__(self, options):
        self.log_file: str = options["log_file"]
        self.database_dir: str = options["database_location"]
        self.debug: bool = options["debug"]
        self.cache_dir: str = options["cache_dir"]
        self.graph_dir: str = options["graph_dir"]
        self.timestamp: str = datetime.datetime.now().strftime("%Y-%m-%d %H-%M-%S")
        self.unix_time: int = int(time.time())
        self.graph_ts_dir = os.path.join(self.graph_dir, self.timestamp)
        self.profile = options["profile"]

        # Create missing folders
        self.create_dirs()

    def create_dirs(self):
        if not os.path.exists(self.graph_dir):
            os.mkdir(self.graph_dir)

        if not os.path.exists(self.graph_ts_dir):
            os.mkdir(self.graph_ts_dir)

        if not os.path.exists(self.cache_dir):
            os.mkdir(self.cache_dir)

        if self.database_dir and not os.path.exists(self.database_dir):
            os.makedirs(self.database_dir)


class ProfileConfig:
    def __init__(self, profile_name, profile):
        self.profile_name: str = profile_name
        self.locations: List[str] = profile["locations"]
        self.depth: int = profile["depth"]
        self.same_domain: bool = profile["same_domain"]

        self.domain: str = urlparse(self.locations[0]).netloc

        self.filters: List[re.Pattern] = [
            re.compile(pattern) for pattern in profile["filter"]
        ]

        self.matches: List[re.Pattern] = [
            re.compile(pattern) for pattern in profile["match"]
        ]

        # File save tasks
        self.tasks: List[Coroutine] = []

    def filter(self, url: str) -> bool:
        for filter in self.filters:
            if re.search(filter, url):
                return False

        for matchp in self.matches:
            if re.search(matchp, url):
                return True


        if self.same_domain and urlparse(url).netloc != self.domain:
            return False

        return True
