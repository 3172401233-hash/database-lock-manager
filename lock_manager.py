"""Deterministic database lock manager core.

Provides shared/exclusive locks over named resources with fair FIFO wait
queues, a wait-for graph, deadlock detection with a stable victim
selection rule, wait timeouts, wait cancellation and transactional
rollback (release-all).

The manager is thread-safe: all state is guarded by a single condition
variable.  ``try_lock`` is the non-blocking core (returns ``True`` when
granted, ``False`` when the request was queued); ``lock`` is a blocking
wrapper with an optional timeout.
"""

from __future__ import annotations

import threading
import time
from collections import defaultdict, deque
from enum import Enum


class LockMode(Enum):
    SHARED = "S"
    EXCLUSIVE = "X"

    def compatible(self, other: "LockMode") -> bool:
        return self is LockMode.SHARED and other is LockMode.SHARED


class LockError(Exception):
    """Base class for lock manager errors."""


class DeadlockError(LockError):
    """Raised in the victim transaction when a deadlock cycle is found."""


class LockTimeoutError(LockError):
    """Raised when a lock request exceeds its wait timeout."""


class _Waiter:
    __slots__ = ("txn", "mode")

    def __init__(self, txn, mode):
        self.txn = txn
        self.mode = mode


class _ResourceState:
    __slots__ = ("holders", "queue")

    def __init__(self):
        self.holders = {}    # txn -> LockMode (granted)
        self.queue = deque()  # FIFO of _Waiter


class LockManager:
    def __init__(self, clock=time.monotonic):
        self._clock = clock
        self._cond = threading.Condition()
        self._resources = {}
        self._txn_locks = defaultdict(set)  # txn -> {resource, ...} held
        self._txn_error = {}                # txn -> exception to raise
        self._txn_order = {}                # txn -> registration sequence
        self._order_seq = 0

    # ------------------------------------------------------------------
    # public API
    # ------------------------------------------------------------------
    def try_lock(self, txn, resource, mode=LockMode.EXCLUSIVE):
        """Attempt to acquire a lock without blocking.

        Returns True when the lock is granted immediately, False when the
        request was placed on the resource's wait queue.  Raises
        DeadlockError if this transaction is chosen as a deadlock victim.
        """
        with self._cond:
            self._register(txn)
            return self._request(txn, resource, mode)

    def lock(self, txn, resource, mode=LockMode.EXCLUSIVE, timeout=None):
        """Acquire a lock, blocking until granted.

        Raises DeadlockError if this transaction is picked as a victim,
        LockTimeoutError when ``timeout`` seconds elapse without a grant.
        """
        with self._cond:
            self._register(txn)
            if self._request(txn, resource, mode):
                return True
            deadline = None if timeout is None else self._clock() + timeout
            while True:
                err = self._txn_error.pop(txn, None)
                if err is not None:
                    raise err
                if self._holds(txn, resource, mode):
                    return True
                if deadline is not None:
                    remaining = deadline - self._clock()
                    if remaining <= 0:
                        self._remove_waiter(txn, resource)
                        raise LockTimeoutError(
                            "txn %r timed out waiting for %s on %r"
                            % (txn, mode.value, resource)
                        )
                    self._cond.wait(remaining)
                else:
                    self._cond.wait()

    def unlock(self, txn, resource):
        """Release a held lock and wake the compatible waiter group."""
        with self._cond:
            state = self._resources.get(resource)
            if state is None or txn not in state.holders:
                raise LockError(
                    "txn %r does not hold a lock on %r" % (txn, resource)
                )
            del state.holders[txn]
            self._txn_locks[txn].discard(resource)
            self._grant_waiters(resource)

    def release_all(self, txn):
        """Rollback: release every lock and queued request of ``txn``."""
        with self._cond:
            affected = set(self._txn_locks.pop(txn, ()))
            for resource in affected:
                state = self._resources[resource]
                state.holders.pop(txn, None)
            for resource, state in self._resources.items():
                before = len(state.queue)
                state.queue = deque(
                    w for w in state.queue if w.txn != txn
                )
                if len(state.queue) != before:
                    affected.add(resource)
            self._txn_error.pop(txn, None)
            for resource in affected:
                self._grant_waiters(resource)

    def cancel_wait(self, txn, resource=None):
        """Cancel pending (queued) lock requests of ``txn``.

        When ``resource`` is given, only the request on that resource is
        cancelled.  Returns True if anything was removed.
        """
        with self._cond:
            removed = False
            for res, state in self._resources.items():
                if resource is not None and res != resource:
                    continue
                before = len(state.queue)
                state.queue = deque(
                    w for w in state.queue if w.txn != txn
                )
                if len(state.queue) != before:
                    removed = True
                    self._grant_waiters(res)
            self._txn_error.pop(txn, None)
            return removed

    # ------------------------------------------------------------------
    # read-only inspection helpers (used by tests and tooling)
    # ------------------------------------------------------------------
    def holders(self, resource):
        with self._cond:
            state = self._resources.get(resource)
            return dict(state.holders) if state else {}

    def waiters(self, resource):
        with self._cond:
            state = self._resources.get(resource)
            if not state:
                return []
            return [(w.txn, w.mode) for w in state.queue]

    def is_held(self, txn, resource, mode=None):
        with self._cond:
            held = self._resources.get(resource)
            if held is None or txn not in held.holders:
                return False
            if mode is None:
                return True
            actual = held.holders[txn]
            return actual == mode or actual is LockMode.EXCLUSIVE

    # ------------------------------------------------------------------
    # internals (all called with self._cond held)
    # ------------------------------------------------------------------
    def _register(self, txn):
        if txn not in self._txn_order:
            self._txn_order[txn] = self._order_seq
            self._order_seq += 1

    def _state(self, resource):
        state = self._resources.get(resource)
        if state is None:
            state = self._resources[resource] = _ResourceState()
        return state

    def _holds(self, txn, resource, mode):
        state = self._resources.get(resource)
        if state is None:
            return False
        held = state.holders.get(txn)
        if held is None:
            return False
        return held == mode or held is LockMode.EXCLUSIVE

    def _can_grant(self, state, txn, mode):
        for holder, held_mode in state.holders.items():
            if holder != txn and not held_mode.compatible(mode):
                return False
        # Fairness: never barge ahead of a conflicting queued waiter.
        for waiter in state.queue:
            if waiter.txn != txn and not waiter.mode.compatible(mode):
                return False
        return True

    def _request(self, txn, resource, mode):
        state = self._state(resource)
        held = state.holders.get(txn)
        if held is not None and (held == mode or held is LockMode.EXCLUSIVE):
            return True  # re-entrant request already covered
        if any(w.txn == txn for w in state.queue):
            return False  # already queued for this resource
        if self._can_grant(state, txn, mode):
            state.holders[txn] = (
                LockMode.EXCLUSIVE
                if held is LockMode.EXCLUSIVE or mode is LockMode.EXCLUSIVE
                else mode
            )
            self._txn_locks[txn].add(resource)
            return True
        state.queue.append(_Waiter(txn, mode))
        self._detect_deadlocks()
        err = self._txn_error.pop(txn, None)
        if err is not None:
            raise err
        return False

    def _grant_waiters(self, resource):
        """Grant the maximal compatible prefix of the wait queue."""
        state = self._resources[resource]
        granted = False
        while state.queue:
            waiter = state.queue[0]
            if all(
                holder == waiter.txn or held.compatible(waiter.mode)
                for holder, held in state.holders.items()
            ):
                state.queue.popleft()
                state.holders[waiter.txn] = waiter.mode
                self._txn_locks[waiter.txn].add(resource)
                granted = True
            else:
                break
        if granted:
            self._cond.notify_all()

    def _remove_waiter(self, txn, resource=None):
        for res, state in self._resources.items():
            if resource is not None and res != resource:
                continue
            before = len(state.queue)
            state.queue = deque(w for w in state.queue if w.txn != txn)
            if len(state.queue) != before:
                self._grant_waiters(res)

    # ------------------------------------------------------------------
    # deadlock detection
    # ------------------------------------------------------------------
    def _wait_for_graph(self):
        graph = defaultdict(set)
        for state in self._resources.values():
            blockers = list(state.holders.items())
            ahead = []
            for waiter in state.queue:
                for other, mode in blockers + ahead:
                    if other != waiter.txn and not mode.compatible(waiter.mode):
                        graph[waiter.txn].add(other)
                ahead.append((waiter.txn, waiter.mode))
        return graph

    def _find_cycle(self):
        graph = self._wait_for_graph()
        color = defaultdict(int)  # 0=white 1=gray 2=black
        stack = []
        cycle = []

        def visit(node):
            color[node] = 1
            stack.append(node)
            for nxt in sorted(
                graph.get(node, ()), key=lambda t: self._txn_order.get(t, 0)
            ):
                if color[nxt] == 1:
                    cycle.extend(stack[stack.index(nxt):])
                    return True
                if color[nxt] == 0 and visit(nxt):
                    return True
            stack.pop()
            color[node] = 2
            return False

        for node in sorted(graph, key=lambda t: self._txn_order.get(t, 0)):
            if color[node] == 0 and visit(node):
                return set(cycle)
        return None

    def _detect_deadlocks(self):
        while True:
            cycle = self._find_cycle()
            if not cycle:
                return
            # Stable victim rule: the youngest transaction in the cycle
            # (largest registration order) is aborted.
            victim = max(cycle, key=lambda t: self._txn_order[t])
            self._remove_waiter(victim)
            self._txn_error[victim] = DeadlockError(
                "txn %r chosen as deadlock victim (cycle: %s)"
                % (victim, sorted(cycle, key=lambda t: self._txn_order[t]))
            )
            self._cond.notify_all()
