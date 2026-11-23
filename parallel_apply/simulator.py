"""Local apply simulation, including worker-crash handling."""

from typing import Dict, List, Optional, Set

from .graph import build_dependency_graph
from .model import Transaction
from .scheduler import ApplyPlan


class ApplyResult(object):
    """Outcome of a simulated apply run."""

    def __init__(self):
        self.applied = []  # type: List[str]
        self.skipped = []  # type: List[str]
        self.crashed_workers = []  # type: List[int]
        self.partial_commit = False

    def __repr__(self):
        return (
            "ApplyResult(applied={}, skipped={}, crashed_workers={}, "
            "partial_commit={})"
        ).format(
            self.applied, self.skipped, self.crashed_workers, self.partial_commit
        )


def simulate_apply(
    transactions,  # type: List[Transaction]
    plan,  # type: ApplyPlan
    fail_worker=None,  # type: Optional[int]
    fail_after=0,  # type: int
):
    # type: (...) -> ApplyResult
    """Simulate applying a plan with local workers.

    If ``fail_worker`` is given, that worker crashes after applying
    ``fail_after`` of its assigned transactions. Transactions whose
    dependencies were lost to the crash are skipped, and the run is flagged
    as a partial commit.

    Safety invariant: a transaction is applied only after every dependency
    has been applied; violations raise RuntimeError.
    """
    graph = build_dependency_graph(transactions)
    result = ApplyResult()
    applied = set()  # type: Set[str]
    worker_done = {w: 0 for w in plan.worker_assignments}  # type: Dict[int, int]
    crashed = set()  # type: Set[int]

    for batch in plan.batches:
        for slot, txid in enumerate(batch):
            if slot in crashed:
                result.skipped.append(txid)
                continue
            if (
                fail_worker is not None
                and slot == fail_worker
                and worker_done[slot] >= fail_after
            ):
                crashed.add(slot)
                result.crashed_workers.append(slot)
                result.skipped.append(txid)
                continue
            missing = [
                dep
                for dep in graph.dependencies_of(txid)
                if dep in _all_planned(plan) and dep not in applied
            ]
            if missing:
                # Dependency lost (e.g. to a crash): refuse to apply.
                result.skipped.append(txid)
                result.partial_commit = True
                continue
            applied.add(txid)
            result.applied.append(txid)
            worker_done[slot] += 1

    if result.skipped:
        result.partial_commit = True
    return result


def _all_planned(plan):
    # type: (ApplyPlan) -> Set[str]
    return {txid for batch in plan.batches for txid in batch}
