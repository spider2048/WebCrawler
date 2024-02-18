from ast import Tuple
import logging
from bs4 import BeautifulSoup
import fuzzyset
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy import Engine

import time
import sys
import os
import zlib
import nltk
from collections import defaultdict
from nltk.corpus import stopwords
from nltk.collocations import BigramAssocMeasures, BigramCollocationFinder
from typing import Set, Dict, List
from pqdm.processes import pqdm
from unidecode import unidecode
import pickle

sys.path.extend([os.getcwd()])
from models import CrawlConfig, ProfileConfig, URLData

STOPWORDS = set(stopwords.words())
REMOVE = set(".!#()*&^")
logger: logging.Logger = logging.getLogger("Indexer")


class IndexManager:
    def __init__(self, crawlopts: CrawlConfig, profiles: List[ProfileConfig]) -> None:
        self.crawlopts: CrawlConfig = crawlopts
        self.profiles: str = profiles

        self.sessionmakers: Dict[str, sessionmaker] = {}
        self.bigram_map = defaultdict(set)

        for profile in self.profiles:
            engine: Engine = create_engine(
                "sqlite:///"
                + os.path.join(self.crawlopts.database_dir, profile.profile_name)
                + ".db"
            )

            self.sessionmakers[profile.profile_name] = sessionmaker(bind=engine)

    def process(self):
        token_pairs = self.get_all_data()
        self.group_tokens(token_pairs)

    def get_all_data(self) -> List[Dict[str, Set[str]]]:
        hashes = []

        for _, sessionmaker in self.sessionmakers.items():
            for query in sessionmaker().query(URLData.hash.distinct()).all():
                hashes.append(str(query[0]))

        logger.info("Found %d URLs to index", len(hashes))

        worker_args = []
        for hash in hashes:
            worker_args.append(os.path.join(self.crawlopts.cache_dir, hash))

        logger.info("Indexing with %d workers.", self.crawlopts.workers)
        t_start = time.perf_counter()

        token_pairs = pqdm(worker_args, Indexer.worker, self.crawlopts.workers)

        t_end = time.perf_counter()
        logger.info("Finished indexing in %.2fs", t_end - t_start)

        return token_pairs

    def group_tokens(self, token_pairs):
        for hash, bigrams in token_pairs:
            for bigram in bigrams:
                self.bigram_map[bigram].add(hash)

    def save(self):
        with open(self.crawlopts.index, "wb+") as fd:
            pickle.dump(self.bigram_map, fd)

    def __del__(self):
        for _, v in self.sessionmakers.items():
            v.close_all()


class Indexer:
    @staticmethod
    def worker(src):
        with open(src, "rb") as fd:
            content: bytes = zlib.decompress(fd.read())

        doctext: str = BeautifulSoup(content.decode(), "lxml").get_text(separator=".\n")
        tokens: Set[str] = Indexer.get_tokens(doctext)
        return (os.path.basename(src), tokens)

    @staticmethod
    def get_tokens(document: str) -> Set[str]:
        tokens = set(
            [
                unidecode(token.casefold())
                for token in nltk.word_tokenize(document)
                if token.lower() not in STOPWORDS and token not in REMOVE
            ]
        )

        finder = BigramCollocationFinder.from_words(tokens)
        scored_bigrams = finder.score_ngrams(BigramAssocMeasures.pmi)

        return set([" ".join(bigram) for bigram, _ in scored_bigrams[:100]])
