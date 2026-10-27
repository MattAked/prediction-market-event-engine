# Backtest engine tests

Replace tests/engines/backtest_engine_tests with the folder in this ZIP.
It contains the complete current suite, including the revised test_shutdown.py.
Requires the backtest_shutdown_v1 production changes plus pytest/pytest-asyncio.
No production files or runtime dependencies are included.

From the directory containing PredictionMarketEventSniper:

```powershell
python -m pytest PredictionMarketEventSniper/tests/engines/backtest_engine_tests -v
```

## Coverage

- test_ordering.py: sync/async equivalence, timestamp merge, equal-time internal
  precedence, synthetic insertion order, reentrant scheduling, unorderable
  payloads, executor/strategy output ordering, synthetic draining and clock time.
- test_routing.py: original envelope preservation, executor-before-strategy,
  one-time intent dispatch with correct timestamp, scoped synthetic IDs,
  submission/expiry executor-only routing, timer strategy-only routing,
  execution notification routing, stop output boundaries, grace/policy forwarding.
- test_timers.py: zero/negative delay, cancellation, unknown cancellation,
  cancelled entries not advancing time, duplicate active IDs, reuse following
  cancellation/expiry, rescheduling from timer callbacks, shutdown suppression.
- test_sources_and_lifecycle.py: empty history, instance validation, unsorted
  input, one-item lookahead, sync/async source closure, engine reuse/reset,
  same-engine concurrent-run rejection and independent concurrent engines.
- test_failures.py: strategy/executor/intent/shutdown/finalise errors, original
  source exceptions, invalid grace, primary-error preservation, source closure,
  grouped finalisation failures and task cancellation.
- test_cleanup_routing.py: mixed history/internal cleanup, inclusive deadline,
  zero grace, preserved prefetched history, no strategy reentry and cleanup errors.
- test_shutdown.py: real-executor cancellation timing, fills before cancellation,
  unresolved work at deadline, in-flight submissions, leave-open policy, natural
  exhaustion, failure propagation, cancellation and resource finalisation.

helpers.py provides typed strategy/executor spies and closeable async history.
The engine tests control scheduled outputs, not a duplicate merge implementation.
Most assertions use public callbacks/results; a few lifecycle assertions inspect
engine bookkeeping. Shutdown tests additionally exercise the real executor.

No sleeps, real network calls or timing-precision assertions are used. Event gates
and bounded waits are used only where asynchronous cancellation/concurrency must
be exercised. The mixed output list in test_shutdown.py is explicitly annotated
list[StrategyOutput], avoiding inference as list[PlaceOrderIntent].

Verified: 76 engine-specific cases passed. Combined executor, runtime parity and
engine suites: 256 passed, no xfails. Passing tests establish these v1 contracts,
not real-venue fill accuracy or passive queue simulation.
