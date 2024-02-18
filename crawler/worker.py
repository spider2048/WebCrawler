import asyncio
import logging
import os
from typing import Dict, List

from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import AsyncSession, AsyncEngine, create_async_engine

from scraper import Scraper
import sys

sys.path.extend(os.getcwd())
from models import *


class Crawler:
    def __init__(
        self, crawlopts: CrawlConfig, profileopts: List[ProfileConfig]
    ) -> None:
        self.logger = logging.getLogger(__name__)
        self.profiles: List[ProfileConfig] = profileopts
        self.crawl_workers: List[Coroutine] = []

        self.crawlopts: CrawlConfig = crawlopts

        if self.crawlopts.debug:
            self.logger.setLevel(logging.DEBUG)
        else:
            self.logger.setLevel(logging.INFO)

        self.logger.info("Checking profiles [%d profile(s)]", len(profileopts))

        # SQLAlchemy (engines)
        self.engines: Dict[str, AsyncEngine] = {}
        self.session_makers: Dict[str, sessionmaker] = {}
        self.sessions: List[AsyncSession] = []

        # Profiles
        for profile in profileopts:
            engine: AsyncEngine = create_async_engine(
                os.path.join(
                    "sqlite+aiosqlite:///",
                    os.path.join(
                        self.crawlopts.database_dir, f"{profile.profile_name}.db"
                    ),
                ),
                echo=self.crawlopts.debug,
            )
            self.engines[profile.profile_name] = engine
            self.session_makers[profile.profile_name] = sessionmaker(
                bind=engine, class_=AsyncSession
            )

    async def run(self):
        # Setup database
        await self.setup_database()

        self.logger.info("Starting crawling at: %s", time.asctime())
        t_start = time.perf_counter_ns()

        # Schedule crawl workers
        await self.crawl()

        # Finish workers
        await asyncio.gather(*self.crawl_workers)

        t_end = time.perf_counter_ns()
        t_taken = (t_end - t_start) / 1e9

        self.logger.info("Ending crawling at: %s", time.asctime())
        self.logger.info("Total time taken: %.5f", t_taken)

        # Save entries to database
        await self.finish()

    async def crawl(self):
        for it, profile in enumerate(self.profiles, start=1):
            # Get database session
            session = self.session_makers[profile.profile_name]()

            # Schedule scraper
            self.logger.info("%d> %s", it, profile.profile_name)
            scraper = Scraper(profile, self.crawlopts, session)

            # Append session
            self.crawl_workers.append(scraper.crawl())
            self.sessions.append(session)

    async def save_all_files(self):
        # Save all pending files
        for profile in self.profiles:
            await asyncio.gather(*profile.tasks)
            self.logger.info(
                "%s> Saved %d files to disk",
                profile.profile_name,
                len(profile.tasks),
            )

    async def commit_sessions(self):
        self.logger.info("Committing sessions")

        # Commit all sessions
        await asyncio.gather(*[session.commit() for session in self.sessions])

        # Close all sessions
        for session_maker in self.session_makers.values():
            session_maker.close_all()

    async def setup_database(self):
        logging.info("Setting up databases ...")

        for engine in self.engines.values():
            async with engine.begin() as conn:
                await conn.exec_driver_sql("PRAGMA journal_mode = WAL;")
                await conn.exec_driver_sql("PRAGMA busy_timeout = 5000;")
                await conn.exec_driver_sql("PRAGMA synchronous = NORMAL;")
                await conn.exec_driver_sql("PRAGMA cache_size = 100000000;")
                await conn.exec_driver_sql("PRAGMA foreign_keys = true;")
                await conn.exec_driver_sql("PRAGMA temp_store = memory;")
                await conn.run_sync(Base.metadata.create_all)

    async def finish(self):
        await asyncio.gather(self.commit_sessions(), self.save_all_files())
