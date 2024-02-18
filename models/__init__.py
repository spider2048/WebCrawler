from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base

import os
import time
import datetime
from typing import Coroutine, List
import re
import toml

Base = declarative_base()
TIMESTAMP_FORMAT = "%Y-%m-%d %H-%M-%S"
LOGGING_FORMAT = "[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s"


class URLData(Base):
    __tablename__: str = "URLProfileData"
    id = Column(Integer, primary_key=True, autoincrement=True)
    url = Column(String, nullable=False)
    profile_name = Column(String, nullable=False)
    time = Column(Integer, nullable=False)
    hash = Column(String(64), nullable=False)
    title = Column(String, nullable=False)


class CrawlConfig:
    def __init__(self, options, make_dirs=True):
        self.log_file: str = options["log_file"]
        self.database_dir: str = options["database_location"]
        self.debug: bool = options["debug"]
        self.cache_dir: str = options["cache_dir"]
        self.graph_dir: str = options["graph_dir"]
        self.timestamp: str = datetime.datetime.now().strftime(TIMESTAMP_FORMAT)
        self.unix_time: int = int(time.time())
        self.graph_ts_dir = os.path.join(self.graph_dir, self.timestamp)
        self.profile = options["profile"]
        self.workers: int = options["workers"]
        self.index: str = options["index"]

        # Create missing folders
        if make_dirs:
            self.create_dirs()

    @classmethod
    def load_config(cls, config_path, make_dirs=True):
        with open(config_path) as fd:
            allopts = toml.load(fd)
            return cls(allopts["crawl_options"], make_dirs)

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

        self.filters: List[re.Pattern] = [
            re.compile(pattern) for pattern in profile["filter"]
        ]

        self.matches: List[re.Pattern] = [
            re.compile(pattern) for pattern in profile["match"]
        ]

        # File save tasks
        self.tasks: List[Coroutine] = []

    @staticmethod
    def load_profiles(config):
        with open(config) as fd:
            config = toml.load(fd)

        return [
            ProfileConfig(name, opts) for (name, opts) in config["profiles"].items()
        ]

    def filter(self, url: str) -> bool:
        if self.filters:
            if any(re.search(filter, url) for filter in self.filters):
                return False

        if self.matches:
            if any(re.search(matchp, url) for matchp in self.matches):
                return True
            return False
        else:
            return True
