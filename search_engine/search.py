import os
import sys

from rapidfuzz import fuzz
from typing import DefaultDict, List, Set, Dict
import pickle
from collections import Counter, defaultdict
from heapq import *

sys.path.extend([os.getcwd()])

from models import *

class Search:
    def __init__(self, crawlopts: CrawlConfig):
        self.crawlopts = crawlopts
        self.bigram_map: DefaultDict[str, Set[str]]
        self.keys: Set[str] = []

        self.load()

    def load(self):
        with open(self.crawlopts.index, "rb") as index:
            self.bigram_map: DefaultDict[str, Set[str]] = pickle.load(index)

        self.keys = set(self.bigram_map.keys())

    def search(self, query: str, score_len=10):
        scores = []

        for key in self.keys:
            score = 100 - fuzz.partial_token_sort_ratio(query, key)
            heappush(scores, [score, self.bigram_map[key]])

        top_keys = [heappop(scores) for _ in range(score_len)]
        hash_counter = Counter()
        for score, hashes in top_keys:
            hash_counter.update(hashes)

        return [el for el, _ in hash_counter.most_common()]

