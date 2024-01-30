import asyncio
import logging
import os
from typing import Dict, List, Union

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import AsyncSession, AsyncEngine, create_async_engine

from scraper import Scraper
import sys

sys.path.extend(os.getcwd())
from models import *


class Crawler:
    def __init__(self, options) -> None:
        self.logger = logging.getLogger(__name__)
        self.crawl_workers: List = []

        self.crawlopts = Config(options["crawl_options"])

        if self.crawlopts.debug:
            self.logger.setLevel(logging.DEBUG)
        else:
            self.logger.setLevel(logging.INFO)

        profiles: Dict[str, Union[str, int, bool]] = options["profiles"]
        self.profiles = [ProfileConfig(name, opts) for (name, opts) in profiles.items()]

        self.logger.info("Checking profiles [%d profile(s)]", len(profiles))

        self.engine: AsyncEngine = create_async_engine(
            os.path.join("sqlite+aiosqlite:///", self.crawlopts.database),
            echo=self.crawlopts.debug,
        )

        self.session_maker = sessionmaker(bind=self.engine, class_=AsyncSession)
        self.sessions: List[AsyncSession] = []

    async def run(self):
        # Setup database
        await self.setup_database()

        self.logger.info("starting crawling at: %s", time.asctime())
        t_start = time.perf_counter_ns()

        # Schedule crawl workers
        await self.crawl()

        # Finish workers
        await asyncio.gather(*self.crawl_workers)

        t_end = time.perf_counter_ns()
        self.logger.info("ending crawling at: %s", time.asctime())
        self.logger.info("Total time taken: %.5f", (t_end - t_start) / 1e9)

        # Save entries to database
        await self.finish()

    async def crawl(self):
        for it, conf in enumerate(self.profiles, start=1):
            session = self.session_maker()
            self.logger.info("%d> %s", it, conf.profile_name)
            scraper = Scraper(conf, self.crawlopts, session)
            self.crawl_workers.append(scraper.crawl())
            self.sessions.append(session)

    async def save_all_files(self):
        for profile in self.profiles:
            await asyncio.gather(*profile.file_save_tasks)
            self.logger.info("Saved %d files to disk", len(profile.file_save_tasks))

    async def commit_sessions(self):
        self.logger.info("Committing sessions")

        # Commit one-by-one
        for session in self.sessions:
            await session.commit()

        # Close all sessions
        self.session_maker.close_all()

    async def setup_database(self):
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def finish(self):
        # Save database and files
        await asyncio.gather(self.commit_sessions(), self.save_all_files())

        # Save entry to CrawlStats
        self.logger.info("Saving entry to crawl stats")
        engine = create_engine(os.path.join("sqlite:///", self.crawlopts.database))

        session = sessionmaker(bind=engine)()
        session.add(URLCrawlStats(time=self.crawlopts.unix_time))

        session.commit()
        session.close()
