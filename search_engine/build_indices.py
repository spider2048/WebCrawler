from glob import glob
from typing import Set
from bs4 import BeautifulSoup
from pqdm.processes import pqdm
import zlib

from collections import defaultdict

from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer

import os
import sqlalchemy
from sqlalchemy.orm import sessionmaker
import toml

from models import Config


class IndexBuilder:
    def __init__(self, config_file: str):
        self.indices = defaultdict(set)
        self.stop_words = set(stopwords.words("english"))

        with open(config_file) as conf:
            self.crawlopts :Config = conf['crawl_options']

    def replace_punctuation(self, ss: str):
        return ss.replace(".", "").replace("`", "").replace("/", "")

    def tokenize(self, text: str) -> Set[str]:
        # Tokenize and remove stop words
        tokens = word_tokenize(text)
        tokens = [
            self.replace_punctuation(token)
            for token in tokens
            if token.lower() not in self.stop_words and 2 <= len(token) <= 20
        ]

        stemmer = PorterStemmer()
        tokens = [stemmer.stem(token) for token in tokens]
        return set(tokens)

    def process_file(self, file_path: str, src_link: str) -> Set[str]:
        # Decompress file
        with open(file_path, "rb") as fd:
            content = zlib.decompress(fd.read())

        # Get page text
        page_text = BeautifulSoup(content, "lxml") \
                        .get_text(separator=".\n", strip=True)

        tokens = self.tokenize(page_text)
        for token in tokens:
            self.indices[token].add(src_link)

    def save_indices(self):
        pass
