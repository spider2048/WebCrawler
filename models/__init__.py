from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base

import os
import time
from typing import Coroutine, List

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

class Config:
    def __init__(self, options):
        self.log_file: str = options["log_file"]
        self.database: str = options["database_location"]
        self.debug: bool = options["debug"]
        self.cache_dir: str = options["cache_dir"]
        self.graph_dir: str = options["graph_dir"]
        self.timestamp: int = time.asctime().replace(":", "-")
        self.unix_time: int = int(time.time())

        # Create missing folders
        self.create_dirs()

    def create_dirs(self):
        if not os.path.exists(self.graph_dir):
            os.mkdir(self.graph_dir)

        self.graph_ts_dir = os.path.join(self.graph_dir, self.timestamp)
        if not os.path.exists(self.graph_ts_dir):
            os.mkdir(self.graph_ts_dir)

        if not os.path.exists(self.cache_dir):
            os.mkdir(self.cache_dir)

        database_dir = os.path.dirname(self.database)
        if database_dir and not os.path.exists(database_dir):
            os.makedirs(database_dir)


class ProfileConfig:
    def __init__(self, profile_name, profile):
        self.profile_name: str = profile_name
        self.locations: List[str] = profile["locations"]
        self.depth: int = profile["depth"]
        self.method: str = profile["method"]
        self.same_domain: bool = profile["same_domain"]

        # Async tasks
        self.file_save_tasks: List[Coroutine] = []
