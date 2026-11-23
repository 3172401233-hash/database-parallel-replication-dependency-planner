"""Dependency-graph construction and cycle detection.

An edge A -> B means "A must be applied before B". Edges are only created
from the transaction with the smaller commit LSN to the one with the larger
commit LSN, so a correct input stream yields a DAG. Cycles therefore signal
inconsistent input (e.g. out-of-order LSNs or a conflict that cannot be
ordered) and are reported explicitly.
"""

from typing import Dict, List, Set, Tuple

from .model import Transaction, table_of


def _row_conflict(a, b):
    # type: (Transaction, Transaction) -> bool
    """RW / WR / WW overlap on the same row key."""
    if a.write_set & b.write_set:
        return True
    if a.write_set & b.read_set:
        return True
    if b.write_set & a.read_set:
        return True
    return False


def _unique_conflict(a, b):
    # type: (Transaction, Transaction) -> bool
    """Both transactions contend on the same unique constraint."""
    return bool(a.unique_keys & b.unique_keys)


def _ddl_conflict(a, b):
    # type: (Transaction, Transaction) -> bool
    """DDL must serialize against any DML/DDL touching its tables."""
    if not (a.is_ddl or b.is_ddl):
        return False
    return bool(a.touched_tables() & b.touched_tables())


def conflicts(a, b):
    # type: (Transaction, Transaction) -> bool
    return (
        _row_conflict(a, b)
        or _unique_conflict(a, b)
        or _ddl_conflict(a, b)
    )


class DependencyGraph(object):
    """Directed dependency graph over transactions."""

    def __init__(self, transactions):
        # type: (List[Transaction]) -> None
        self.transactions = list(transactions)
        self.by_id = {t.txid: t for t in self.transactions}  # type: Dict[str, Transaction]
        self.edges = {t.txid: set() for t in self.transactions}  # type: Dict[str, Set[str]]
        self.reverse = {t.txid: set() for t in self.transactions}  # type: Dict[str, Set[str]]

    def add_edge(self, src, dst):
        # type: (str, str) -> None
        self.edges[src].add(dst)
        self.reverse[dst].add(src)

    def dependencies_of(self, txid):
        # type: (str) -> Set[str]
        return set(self.reverse[txid])

    def find_cycle(self):
        # type: () -> List[str]
        """Return one cycle as a list of txids, or [] if the graph is a DAG."""
        WHITE, GRAY, BLACK = 0, 1, 2
        color = {t: WHITE for t in self.edges}
        stack = []  # type: List[str]

        def dfs(node):
            color[node] = GRAY
            stack.append(node)
            for nxt in sorted(self.edges[node]):
                if color[nxt] == GRAY:
                    idx = stack.index(nxt)
                    return stack[idx:] + [nxt]
                if color[nxt] == WHITE:
                    found = dfs(nxt)
                    if found:
                        return found
            stack.pop()
            color[node] = BLACK
            return []

        for txid in sorted(self.edges):
            if color[txid] == WHITE:
                found = dfs(txid)
                if found:
                    return found
        return []


def build_dependency_graph(transactions):
    # type: (List[Transaction]) -> DependencyGraph
    """Build the dependency graph for a set of transactions.

    For every conflicting pair, the edge points from the smaller commit LSN
    to the larger one. Equal LSNs for distinct conflicting transactions are
    invalid input and raise ValueError.
    """
    graph = DependencyGraph(transactions)
    txns = sorted(graph.transactions, key=lambda t: (t.commit_lsn, t.txid))
    for i, earlier in enumerate(txns):
        for later in txns[i + 1:]:
            if not conflicts(earlier, later):
                continue
            if earlier.commit_lsn == later.commit_lsn:
                raise ValueError(
                    "conflicting transactions share commit LSN {}: {} vs {}".format(
                        earlier.commit_lsn, earlier.txid, later.txid
                    )
                )
            graph.add_edge(earlier.txid, later.txid)
    return graph
