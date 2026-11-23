"""Batch scheduling over the dependency graph."""

from typing import Dict, List, Optional

from .graph import build_dependency_graph
from .model import Transaction


class SerialRange(object):
    """An LSN range that must be applied serially (never in parallel)."""

    def __init__(self, start_lsn, end_lsn, reason, txids):
        # type: (int, int, str, List[str]) -> None
        self.start_lsn = start_lsn
        self.end_lsn = end_lsn
        self.reason = reason
        self.txids = list(txids)

    def __repr__(self):
        return "SerialRange([{}..{}], reason={!r}, txids={})".format(
            self.start_lsn, self.end_lsn, self.reason, self.txids
        )


class ApplyPlan(object):
    """Result of planning.

    Attributes:
        batches:            parallel batches; txids inside one batch are
                            mutually independent and sized <= worker capacity.
        serial_ranges:      LSN ranges excluded from parallel apply.
        cycles:             detected dependency cycles (lists of txids).
        partial_commits:    txids blocked because an unprepared (partially
                            committed) transaction is in their dependency
                            chain, including the unprepared txns themselves.
        worker_assignments: worker id -> ordered txids assigned to it.
        orphan_workers:     worker ids that received no assignment at all.
    """

    def __init__(self):
        self.batches = []  # type: List[List[str]]
        self.serial_ranges = []  # type: List[SerialRange]
        self.cycles = []  # type: List[List[str]]
        self.partial_commits = []  # type: List[str]
        self.worker_assignments = {}  # type: Dict[int, List[str]]
        self.orphan_workers = []  # type: List[int]

    def __repr__(self):
        return (
            "ApplyPlan(batches={}, serial_ranges={}, cycles={}, "
            "partial_commits={}, orphan_workers={})"
        ).format(
            self.batches,
            self.serial_ranges,
            self.cycles,
            self.partial_commits,
            self.orphan_workers,
        )


def _transitive_dependents(graph, roots):
    # type: (object, List[str]) -> set
    seen = set(roots)
    stack = list(roots)
    while stack:
        node = stack.pop()
        for nxt in graph.edges[node]:
            if nxt not in seen:
                seen.add(nxt)
                stack.append(nxt)
    return seen


def _serial_range_for(graph, txids, reason):
    # type: (object, List[str], str) -> SerialRange
    lsns = [graph.by_id[t].commit_lsn for t in txids]
    return SerialRange(min(lsns), max(lsns), reason, sorted(txids))


def plan_batches(transactions, worker_capacity):
    # type: (List[Transaction], int) -> ApplyPlan
    """Build the dependency graph and schedule parallel apply batches.

    Raises ValueError if worker_capacity < 1 or on invalid LSN input.
    """
    if worker_capacity < 1:
        raise ValueError("worker_capacity must be >= 1")

    plan = ApplyPlan()
    graph = build_dependency_graph(transactions)

    # 1. Cycle detection: cyclic transactions cannot be parallelised.
    cyclic = set()  # type: set
    while True:
        cycle = graph.find_cycle()
        if not cycle:
            break
        members = set(cycle)
        cyclic |= members
        plan.cycles.append(sorted(members))
        # Break the cycle so remaining scheduling can proceed.
        graph.edges[cycle[-2]].discard(cycle[-1])
        graph.reverse[cycle[-1]].discard(cycle[-2])
    if cyclic:
        plan.serial_ranges.append(
            _serial_range_for(graph, sorted(cyclic), "dependency-cycle")
        )

    # 2. Partial commits: unprepared transactions and everything that
    #    (transitively) depends on them must not be applied.
    unprepared = [t.txid for t in graph.transactions if not t.prepared]
    blocked = _transitive_dependents(graph, unprepared) if unprepared else set()
    blocked -= cyclic
    if blocked:
        plan.partial_commits = sorted(blocked)
        plan.serial_ranges.append(
            _serial_range_for(graph, sorted(blocked), "partial-commit")
        )

    # 3. Level the remaining schedulable transactions (longest-path layering,
    #    Kahn order). Transactions in the same level are independent.
    schedulable = [
        t for t in graph.transactions
        if t.txid not in cyclic and t.txid not in blocked
    ]
    level = {}  # type: Dict[str, int]
    excluded = cyclic | blocked
    for txn in sorted(schedulable, key=lambda t: (t.commit_lsn, t.txid)):
        dep_levels = [
            level[d]
            for d in graph.reverse[txn.txid]
            if d in level and d not in excluded
        ]
        level[txn.txid] = 1 + max(dep_levels) if dep_levels else 0

    # 4. Pack levels into batches bounded by worker capacity. A level larger
    #    than the capacity is split into consecutive batches (still safe:
    #    level members are mutually independent).
    max_level = max(level.values()) if level else -1
    for lv in range(max_level + 1):
        members = [
            t.txid
            for t in sorted(schedulable, key=lambda t: (t.commit_lsn, t.txid))
            if level[t.txid] == lv
        ]
        for start in range(0, len(members), worker_capacity):
            plan.batches.append(members[start:start + worker_capacity])

    # 5. Assign batch slots to workers (slot i -> worker i) and find orphans.
    plan.worker_assignments = {w: [] for w in range(worker_capacity)}
    for batch in plan.batches:
        for slot, txid in enumerate(batch):
            plan.worker_assignments[slot].append(txid)
    plan.orphan_workers = [
        w for w in range(worker_capacity) if not plan.worker_assignments[w]
    ]
    return plan
