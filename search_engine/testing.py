from build_indices import IndexBuilder
from collections import Counter

idbx = IndexBuilder("../data")
collocations = idbx.process_file(
    r"D:\Coding\Python\WebCrawler\data\0b8748242032d91f2a908db2f792c5ae71d668b7", ""
)

print(idbx.indices)
