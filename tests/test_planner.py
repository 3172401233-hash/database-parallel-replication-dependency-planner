import unittest

from parallel_apply import (
    Transaction,
    build_dependency_graph,
    plan_batches,
    simulate_apply,
)


def tx(txid, lsn, reads=None, writes=None, unique=None, ddl=None, prepared=True):
    return Transaction(
        txid=txid,
        commit_lsn=lsn,
        read_set=reads,
        write_set=writes,
        unique_keys=unique,
        ddl_targets=ddl,
        prepared=prepared,
    )


def batch_of(plan, txid):
    for i, batch in enumerate(plan.batches):
        if txid in batch:
            return i
    return None


class RowConflictTest(unittest.TestCase):
    def test_same_key_write_write_serializes(self):
        a = tx("a", 1, writes=["users:1"])
        b = tx("b", 2, writes=["users:1"])
        plan = plan_batches([a, b], worker_capacity=4)
        self.assertEqual(len(plan.batches), 2)
        self.assertLess(batch_of(plan, "a"), batch_of(plan, "b"))

    def test_read_write_conflict_serializes(self):
        a = tx("a", 1, writes=["orders:9"])
        b = tx("b", 2, reads=["orders:9"], writes=["orders:10"])
        plan = plan_batches([a, b], worker_capacity=4)
        self.assertLess(batch_of(plan, "a"), batch_of(plan, "b"))

    def test_disjoint_rows_run_parallel(self):
        a = tx("a", 1, writes=["users:1"])
        b = tx("b", 2, writes=["users:2"])
        c = tx("c", 3, reads=["users:3"])
        plan = plan_batches([a, b, c], worker_capacity=2)
        self.assertEqual(plan.batches, [["a", "b"], ["c"]])
        self.assertEqual(plan.orphan_workers, [])

    def test_orphan_worker_detected(self):
        a = tx("a", 1, writes=["users:1"])
        plan = plan_batches([a], worker_capacity=3)
        self.assertEqual(plan.orphan_workers, [1, 2])


class UniqueKeyTest(unittest.TestCase):
    def test_unique_key_contention_serializes(self):
        # Different rows, same unique constraint: cannot run in parallel.
        a = tx("a", 1, writes=["users:1"], unique=["users.email"])
        b = tx("b", 2, writes=["users:2"], unique=["users.email"])
        plan = plan_batches([a, b], worker_capacity=4)
        self.assertLess(batch_of(plan, "a"), batch_of(plan, "b"))

    def test_distinct_constraints_run_parallel(self):
        a = tx("a", 1, writes=["users:1"], unique=["users.email"])
        b = tx("b", 2, writes=["users:2"], unique=["users.ssn"])
        plan = plan_batches([a, b], worker_capacity=4)
        self.assertEqual(plan.batches, [["a", "b"]])


class DdlDmlTest(unittest.TestCase):
    def test_ddl_waits_for_related_dml(self):
        dml1 = tx("dml1", 1, writes=["users:1"])
        ddl = tx("ddl", 2, ddl=["users"])
        dml2 = tx("dml2", 3, writes=["users:2"])
        other = tx("other", 4, writes=["orders:1"])
        plan = plan_batches([dml1, ddl, dml2, other], worker_capacity=4)
        self.assertLess(batch_of(plan, "dml1"), batch_of(plan, "ddl"))
        self.assertLess(batch_of(plan, "ddl"), batch_of(plan, "dml2"))
        # Unrelated DML is not blocked by the DDL.
        self.assertEqual(batch_of(plan, "other"), batch_of(plan, "dml1"))

    def test_ddl_on_other_table_does_not_block(self):
        dml = tx("dml", 1, writes=["users:1"])
        ddl = tx("ddl", 2, ddl=["orders"])
        plan = plan_batches([dml, ddl], worker_capacity=4)
        self.assertEqual(plan.batches, [["dml", "ddl"]])


class CycleTest(unittest.TestCase):
    def _cyclic_graph(self):
        # t1 (lsn 1) and t2 (lsn 2) conflict on users:1, giving t1 -> t2.
        # Manually adding t2 -> t1 models inconsistent input that must be
        # reported as a cycle instead of being scheduled.
        t1 = tx("t1", 1, writes=["users:1"])
        t2 = tx("t2", 2, writes=["users:1"])
        graph = build_dependency_graph([t1, t2])
        graph.add_edge("t2", "t1")
        return graph

    def test_cycle_detected(self):
        graph = self._cyclic_graph()
        cycle = graph.find_cycle()
        self.assertEqual(set(cycle), {"t1", "t2"})

    def test_cyclic_transactions_become_serial_range(self):
        from parallel_apply.scheduler import _serial_range_for

        graph = self._cyclic_graph()
        rng = _serial_range_for(graph, ["t1", "t2"], "dependency-cycle")
        self.assertEqual((rng.start_lsn, rng.end_lsn), (1, 2))
        self.assertEqual(rng.reason, "dependency-cycle")

    def test_equal_lsn_conflict_rejected(self):
        a = tx("a", 5, writes=["k:1"])
        b = tx("b", 5, writes=["k:1"])
        with self.assertRaises(ValueError):
            build_dependency_graph([a, b])


class PartialCommitTest(unittest.TestCase):
    def test_unprepared_blocks_dependents(self):
        a = tx("a", 1, writes=["users:1"], prepared=False)
        b = tx("b", 2, writes=["users:1"])
        c = tx("c", 3, writes=["users:9"])
        plan = plan_batches([a, b, c], worker_capacity=4)
        self.assertEqual(plan.partial_commits, ["a", "b"])
        self.assertEqual(plan.batches, [["c"]])
        reasons = [r.reason for r in plan.serial_ranges]
        self.assertIn("partial-commit", reasons)


class WorkerCrashTest(unittest.TestCase):
    def test_crash_skips_dependents_and_flags_partial(self):
        # Batch 0: a, b (parallel). Batch 1: c depends on b.
        a = tx("a", 1, writes=["t:1"])
        b = tx("b", 2, writes=["t:2"])
        c = tx("c", 3, writes=["t:2", "t:3"])
        plan = plan_batches([a, b, c], worker_capacity=2)
        self.assertEqual(plan.batches, [["a", "b"], ["c"]])
        # Worker 1 (owns b) crashes before applying anything.
        result = simulate_apply([a, b, c], plan, fail_worker=1, fail_after=0)
        self.assertEqual(result.applied, ["a"])
        self.assertEqual(result.skipped, ["b", "c"])
        self.assertTrue(result.partial_commit)
        self.assertEqual(result.crashed_workers, [1])

    def test_clean_run_applies_everything(self):
        a = tx("a", 1, writes=["t:1"])
        b = tx("b", 2, writes=["t:2"])
        plan = plan_batches([a, b], worker_capacity=2)
        result = simulate_apply([a, b], plan)
        self.assertEqual(sorted(result.applied), ["a", "b"])
        self.assertFalse(result.partial_commit)

    def test_crash_after_work_keeps_committed_prefix(self):
        a = tx("a", 1, writes=["t:1"])
        b = tx("b", 2, writes=["t:1"])
        c = tx("c", 3, writes=["t:1"])
        plan = plan_batches([a, b, c], worker_capacity=2)
        # a, b serialize on t:1 -> batches [[a], [b], [c]] on worker 0.
        result = simulate_apply([a, b, c], plan, fail_worker=0, fail_after=1)
        self.assertEqual(result.applied, ["a"])
        self.assertEqual(result.skipped, ["b", "c"])
        self.assertTrue(result.partial_commit)


if __name__ == "__main__":
    unittest.main()
