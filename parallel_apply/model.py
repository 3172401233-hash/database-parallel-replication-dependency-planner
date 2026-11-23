"""Transaction and worker data model (all in-memory, local only)."""

from typing import FrozenSet, Iterable, List, Optional, Set


def _freeze(keys):
    # type: (Optional[Iterable[str]]) -> FrozenSet[str]
    return frozenset(keys or ())


def table_of(key):
    # type: (str) -> str
    """Extract the table name from a row key of the form "table:pk"."""
    return key.split(":", 1)[0]


class Transaction(object):
    """One replicated transaction to apply.

    Attributes:
        txid:        transaction identifier.
        commit_lsn:  source commit LSN; defines the global commit order.
        read_set:    row keys read ("table:pk").
        write_set:   row keys written ("table:pk").
        unique_keys: unique-constraint identifiers touched (e.g. "users.email").
        ddl_targets: when non-empty the transaction is DDL on these tables.
        prepared:    False means the source transaction is only partially
                     committed (not yet prepared/complete upstream).
    """

    def __init__(
        self,
        txid,  # type: str
        commit_lsn,  # type: int
        read_set=None,  # type: Optional[Iterable[str]]
        write_set=None,  # type: Optional[Iterable[str]]
        unique_keys=None,  # type: Optional[Iterable[str]]
        ddl_targets=None,  # type: Optional[Iterable[str]]
        prepared=True,  # type: bool
    ):
        # type: (...) -> None
        self.txid = txid
        self.commit_lsn = commit_lsn
        self.read_set = _freeze(read_set)  # type: FrozenSet[str]
        self.write_set = _freeze(write_set)  # type: FrozenSet[str]
        self.unique_keys = _freeze(unique_keys)  # type: FrozenSet[str]
        self.ddl_targets = _freeze(ddl_targets)  # type: FrozenSet[str]
        self.prepared = prepared

    @property
    def is_ddl(self):
        # type: () -> bool
        return bool(self.ddl_targets)

    def touched_tables(self):
        # type: () -> Set[str]
        tables = {table_of(k) for k in self.read_set | self.write_set}
        tables |= set(self.ddl_targets)
        return tables

    def __repr__(self):
        return "Transaction(txid={!r}, lsn={})".format(self.txid, self.commit_lsn)


class Worker(object):
    """Apply worker (local model)."""

    def __init__(self, worker_id):
        # type: (int) -> None
        self.worker_id = worker_id
        self.assigned = []  # type: List[str]
        self.alive = True

    def __repr__(self):
        return "Worker({})".format(self.worker_id)
