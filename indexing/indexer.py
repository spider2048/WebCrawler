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
from typing import Set, Dict, List, TypeVarTuple
from pqdm.processes import pqdm
from unidecode import unidecode
import pickle

sys.path.extend([os.getcwd()])
from models import CrawlConfig, URLData

STOPWORDS = set(stopwords.words())
REMOVE = set(".!#()*&^")
logger: logging.Logger = logging.getLogger("Indexer")

class IndexManager:
    def __init__(self, crawlopts: CrawlConfig, profile: str) -> None:
        self.crawlopts: CrawlConfig = crawlopts
        self.profile: str = profile

        engine: Engine = create_engine(
            "sqlite:///" + os.path.join(self.crawlopts.database_dir, profile) + ".db"
        )

        self.sessionmaker: sessionmaker = sessionmaker(bind=engine)
        self.bigram_map = defaultdict(set)
        self.token_pairs: List[Dict[str, Set[str]]] = []

    def process(self):
        self.get_all_data()
        self.group_tokens()

    def get_all_data(self) -> None:
        session: Session = self.sessionmaker()
        urldata = session.query(URLData.hash.distinct()).all()

        logger.info("Found %d URLs to index", len(urldata))

        worker_args = []
        for hash in urldata:
            src = os.path.join(
                self.crawlopts.cache_dir,
                str(hash[0])
            )
            worker_args.append(src)
        
        logger.info("Indexing with %d workers.", self.crawlopts.workers)
        t_start = time.perf_counter()

        self.token_pairs = pqdm(worker_args, Indexer.worker, self.crawlopts.workers)

        t_end = time.perf_counter()
        logger.info("Finished indexing in %.2fs", t_end - t_start)

    def group_tokens(self):
        for hash, bigrams in self.token_pairs:
            for bigram in bigrams:
                self.bigram_map[bigram].add(hash)

    def save(self, dest):
        with open(dest, "wb+") as fd:
            pickle.dump(self.bigram_map, fd)

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
        tokens = set([
            unidecode(token.casefold())
            for token in nltk.word_tokenize(document)
            if token.lower() not in STOPWORDS and token not in REMOVE
        ])

        finder = BigramCollocationFinder.from_words(tokens)
        scored_bigrams = finder.score_ngrams(BigramAssocMeasures.pmi)
        
        return set([
            ' '.join(bigram) for bigram, _ in scored_bigrams[:100]
        ])