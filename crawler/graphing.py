from typing import Iterable, Set
import networkx as nx
import ujson as json
import logging
import aiofiles

logger: logging.Logger = logging.getLogger("Graphing")


class Graph:
    def __init__(self) -> None:
        self.graph = nx.DiGraph()

    def update_edges(self, url: str, links: Iterable[str], title: str):
        self.graph.add_node(url, description=title)
        self.graph.add_edges_from((url, l) for l in links)

    async def save(self, dest) -> None:
        try:
            async with aiofiles.open(dest, "w+") as fd:
                await fd.write(json.dumps(nx.node_link_data(self.graph)))
            logger.info("Saved graph (%d edges)", len(self.graph.edges))
        except Exception as err:
            logger.error("Failed to store graph with error %s", err)
