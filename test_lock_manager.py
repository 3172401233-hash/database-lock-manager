import threading
import time
import unittest

from lock_manager import (
    DeadlockError,
    LockManager,
    LockMode,
    LockTimeoutError,
)

S = LockMode.SHARED
X = LockMode.EXCLUSIVE


class LockManagerTestCase(unittest.TestCase):
    def setUp(self):
        self.lm = LockManager()

    def assert_no_conflict(self, resource):
        holders = self.lm.holders(resource)
        modes = list(holders.values())
        if X in modes:
            self.assertEqual(
                len(modes), 1,
                "exclusive lock granted alongside others on %r" % resource,
            )


class TestReadWriteContention(LockManagerTestCase):
    def test_shared_locks_are_compatible(self):
        self.assertTrue(self.lm.try_lock("t1", "r", S))
        self.assertTrue(self.lm.try_lock("t2", "r", S))
        self.assert_no_conflict("r")

    def test_exclusive_excludes_everything(self):
        self.assertTrue(self.lm.try_lock("t1", "r", X))
        self.assertFalse(self.lm.try_lock("t2", "r", S))
        self.assertFalse(self.lm.try_lock("t3", "r", X))
        self.lm.unlock("t1", "r")
        # FIFO: t2 (S) is head; t3 (X) conflicts with granted t2.
        self.assertTrue(self.lm.is_held("t2", "r", S))
        self.assertFalse(self.lm.is_held("t3", "r"))
        self.assert_no_conflict("r")

    def test_writer_waits_for_readers(self):
        self.lm.try_lock("t1", "r", S)
        self.lm.try_lock("t2", "r", S)
        self.assertFalse(self.lm.try_lock("t3", "r", X))
        self.lm.unlock("t1", "r")
        self.assertFalse(self.lm.is_held("t3", "r"))
        self.lm.unlock("t2", "r")
        self.assertTrue(self.lm.is_held("t3", "r", X))
        self.assert_no_conflict("r")


class TestLockUpgrade(LockManagerTestCase):
    def test_upgrade_when_sole_holder(self):
        self.lm.try_lock("t1", "r", S)
        self.assertTrue(self.lm.try_lock("t1", "r", X))
        self.assertTrue(self.lm.is_held("t1", "r", X))
        self.assert_no_conflict("r")

    def test_upgrade_waits_for_other_readers(self):
        self.lm.try_lock("t1", "r", S)
        self.lm.try_lock("t2", "r", S)
        self.assertFalse(self.lm.try_lock("t1", "r", X))
        self.lm.unlock("t2", "r")
        self.assertTrue(self.lm.is_held("t1", "r", X))
        self.assert_no_conflict("r")

    def test_upgrade_upgrade_deadlock(self):
        self.lm.try_lock("t1", "r", S)
        self.lm.try_lock("t2", "r", S)
        self.assertFalse(self.lm.try_lock("t1", "r", X))
        # t2 is younger and becomes the victim.
        with self.assertRaises(DeadlockError):
            self.lm.try_lock("t2", "r", X)
        # t1 still waits on t2's shared lock until t2 rolls back.
        self.assertFalse(self.lm.is_held("t1", "r", X))
        self.lm.release_all("t2")
        self.assertTrue(self.lm.is_held("t1", "r", X))
        self.assert_no_conflict("r")


class TestFairQueuing(LockManagerTestCase):
    def test_reader_cannot_barge_ahead_of_waiting_writer(self):
        self.lm.try_lock("t1", "r", S)
        self.assertFalse(self.lm.try_lock("t2", "r", X))
        # t3's shared request is compatible with t1 but must queue
        # behind the waiting writer t2.
        self.assertFalse(self.lm.try_lock("t3", "r", S))
        self.lm.unlock("t1", "r")
        self.assertTrue(self.lm.is_held("t2", "r", X))
        self.assertFalse(self.lm.is_held("t3", "r"))
        self.lm.unlock("t2", "r")
        self.assertTrue(self.lm.is_held("t3", "r", S))
        self.assert_no_conflict("r")

    def test_compatible_group_woken_together(self):
        self.lm.try_lock("t1", "r", X)
        self.lm.try_lock("t2", "r", S)
        self.lm.try_lock("t3", "r", S)
        self.lm.try_lock("t4", "r", X)
        self.lm.unlock("t1", "r")
        # The S,S prefix is granted as a group; t4 still waits.
        self.assertTrue(self.lm.is_held("t2", "r", S))
        self.assertTrue(self.lm.is_held("t3", "r", S))
        self.assertFalse(self.lm.is_held("t4", "r"))
        self.assert_no_conflict("r")


class TestDeadlockDetection(LockManagerTestCase):
    def test_two_resource_cycle(self):
        self.lm.try_lock("t1", "a", X)
        self.lm.try_lock("t2", "b", X)
        self.assertFalse(self.lm.try_lock("t1", "b", X))
        with self.assertRaises(DeadlockError):
            self.lm.try_lock("t2", "a", X)
        # Victim (younger t2) lost its queued request but keeps its lock.
        self.assertEqual(self.lm.waiters("a"), [])
        self.assertTrue(self.lm.is_held("t2", "b", X))
        self.lm.release_all("t2")
        self.assertTrue(self.lm.is_held("t1", "b", X))

    def test_victim_is_youngest_in_cycle(self):
        self.lm.try_lock("t1", "a", X)
        self.lm.try_lock("t2", "b", X)
        self.lm.try_lock("t3", "c", X)
        self.lm.try_lock("t1", "b", X)
        self.lm.try_lock("t2", "c", X)
        # t3 closes the cycle t1 -> t2 -> t3 -> t1; t3 is youngest.
        with self.assertRaises(DeadlockError):
            self.lm.try_lock("t3", "a", X)
        self.assertFalse(self.lm.is_held("t3", "a"))
        self.assertEqual(self.lm.waiters("a"), [])

    def test_multiple_independent_cycles(self):
        # Cycle 1: t1 <-> t2 on a/b.  Cycle 2: t3 <-> t4 on c/d.
        self.lm.try_lock("t1", "a", X)
        self.lm.try_lock("t2", "b", X)
        self.lm.try_lock("t1", "b", X)
        with self.assertRaises(DeadlockError):
            self.lm.try_lock("t2", "a", X)

        self.lm.try_lock("t3", "c", X)
        self.lm.try_lock("t4", "d", X)
        self.lm.try_lock("t3", "d", X)
        with self.assertRaises(DeadlockError):
            self.lm.try_lock("t4", "c", X)

        self.lm.release_all("t2")
        self.lm.release_all("t4")
        self.assertTrue(self.lm.is_held("t1", "b", X))
        self.assertTrue(self.lm.is_held("t3", "d", X))
        self.assert_no_conflict("a")
        self.assert_no_conflict("b")
        self.assert_no_conflict("c")
        self.assert_no_conflict("d")


class TestWaitTimeout(LockManagerTestCase):
    def test_blocking_lock_times_out(self):
        self.lm.try_lock("t1", "r", X)
        start = time.monotonic()
        with self.assertRaises(LockTimeoutError):
            self.lm.lock("t2", "r", S, timeout=0.1)
        self.assertGreaterEqual(time.monotonic() - start, 0.09)
        # Timed-out request is removed from the queue.
        self.assertEqual(self.lm.waiters("r"), [])

    def test_blocking_lock_wakes_on_release(self):
        self.lm.try_lock("t1", "r", X)
        acquired = threading.Event()

        def waiter():
            self.lm.lock("t2", "r", S, timeout=5)
            acquired.set()

        thread = threading.Thread(target=waiter)
        thread.start()
        time.sleep(0.05)
        self.assertFalse(acquired.is_set())
        self.lm.unlock("t1", "r")
        thread.join(timeout=5)
        self.assertTrue(acquired.is_set())
        self.assertTrue(self.lm.is_held("t2", "r", S))

    def test_blocking_lock_raises_deadlock_for_victim(self):
        self.lm.try_lock("t1", "a", X)
        self.lm.try_lock("t2", "b", X)
        errors = []

        def victim():
            try:
                self.lm.lock("t2", "a", X)  # blocks, then cycle forms
            except DeadlockError as exc:
                errors.append(exc)

        thread = threading.Thread(target=victim)
        thread.start()
        time.sleep(0.05)
        self.lm.try_lock("t1", "b", X)  # closes the cycle; t2 is younger
        thread.join(timeout=5)
        self.assertEqual(len(errors), 1)


class TestCancelAndRollback(LockManagerTestCase):
    def test_cancel_wait_removes_request(self):
        self.lm.try_lock("t1", "r", X)
        self.assertFalse(self.lm.try_lock("t2", "r", S))
        self.assertTrue(self.lm.cancel_wait("t2", "r"))
        self.assertEqual(self.lm.waiters("r"), [])
        self.lm.unlock("t1", "r")
        self.assertFalse(self.lm.is_held("t2", "r"))

    def test_cancel_unblocks_later_compatible_waiter(self):
        self.lm.try_lock("t1", "r", S)
        self.lm.try_lock("t2", "r", X)  # queued behind t1's S
        self.lm.try_lock("t3", "r", S)  # queued behind t2's X
        self.lm.cancel_wait("t2")
        # With the writer gone, t3 is compatible with holder t1.
        self.assertTrue(self.lm.is_held("t3", "r", S))
        self.assert_no_conflict("r")

    def test_rollback_releases_locks_and_requests(self):
        self.lm.try_lock("t1", "a", X)
        self.lm.try_lock("t1", "b", S)
        self.lm.try_lock("t2", "a", S)   # queued behind t1
        self.lm.try_lock("t1", "c", X)
        self.lm.try_lock("t3", "c", X)   # queued behind t1
        self.lm.try_lock("t1", "d", X)
        self.lm.try_lock("t4", "d", X)   # queued behind t1
        self.lm.try_lock("t6", "f", X)
        self.assertFalse(self.lm.try_lock("t1", "f", X))  # t1 also queued

        self.lm.release_all("t1")
        self.assertTrue(self.lm.is_held("t2", "a", S))
        self.assertTrue(self.lm.is_held("t3", "c", X))
        self.assertTrue(self.lm.is_held("t4", "d", X))
        self.assertEqual(self.lm.waiters("f"), [])
        self.assertFalse(self.lm.is_held("t1", "b"))
        for res in "abcdf":
            self.assert_no_conflict(res)


class TestInvariantUnderStress(LockManagerTestCase):
    def test_randomized_single_threaded_simulation(self):
        import random

        rng = random.Random(20261118)
        txns = ["t%d" % i for i in range(6)]
        resources = ["r%d" % i for i in range(4)]
        for _ in range(2000):
            txn = rng.choice(txns)
            res = rng.choice(resources)
            op = rng.random()
            try:
                if op < 0.55:
                    self.lm.try_lock(txn, res, rng.choice([S, X]))
                elif op < 0.85:
                    if self.lm.is_held(txn, res):
                        self.lm.unlock(txn, res)
                else:
                    self.lm.release_all(txn)
            except DeadlockError:
                self.lm.release_all(txn)
            for r in resources:
                self.assert_no_conflict(r)


if __name__ == "__main__":
    unittest.main()
