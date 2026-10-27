# Live/backtest pipeline parity

Extract into the repository root. Includes the updated runtime parity suite. The accompanying
contract-fix bundle also changes shared events and both executors. Requires your existing pytest and pytest-asyncio installation.

From the parent of the PredictionMarketEventSniper repository:

```powershell
python -m pytest PredictionMarketEventSniper/tests/runtime_parity -v -rx
```

## What runs

Real LiveEngine + LiveExecutor and real BacktestEngine + BacktestExecutor run
identical ParityLogic with separate fresh state, identical strategy parameters
and instrument bindings. An independent live adapter reports scripted venue
results; the backtest adapter normalises books and the executor computes fills.
The live script never invokes backtest matching code.

Each run is checked against an explicit scenario trace, then compared for:
strategy-visible payloads/order, emitted intents/stop requests, callback count,
exit reason/exception, venue instrument resolution and shutdown policy. Live
runs invoke the termination hook once; backtest intentionally never invokes it.

Coverage: market/limit buy and sell, IOC full/partial/empty, FOK full/insufficient,
GTC fill after a later book update, rejection, cancel/leave-open shutdown, and
multi-level fills and explicit cancellations. Every scenario
runs with both a list and an async historical source.

Live ordering uses asyncio events, a strategy observation queue, and Queue.join.
There are no timing sleeps. Three-second timeouts only bound failures, not the
ordering. Teardown always releases gates and shuts down the real live engine.

## Shared execution contract and results

All 30 original parity cases pass, without xfail markers. OrderPartiallyFilled and
OrderFilled both carry incremental quantity and execution price. OrderFilled
also carries avg_fill_price, the whole-order quantity-weighted average.
OrderCancelled identifies the placement intent, including explicit cancellation.
The strategy accumulates quantities and costs from both fill event types and
checks the final average. No payload differences are normalised away.

Executor, parity and engine suites together: 195 passed, no xfails (including shutdown regressions).

## Limits

These are controlled functional parity tests, not a claim that historical fills
reproduce a real venue. Zero simulated latency avoids wall-clock scheduling noise;
wall-clock timestamps are excluded from payload/state comparisons. This suite
does not validate exact real-time latency, timer duration, partial cancellation
request races, cleanup-intent replay, venue failures or passive queue simulation.
Executor unit tests cover additional latency/cancellation cases independently.
A strategy whose decisions depend on absolute timestamps needs separate clock-
controlled tests. Raw venue order IDs are deliberately identical in the two fake
adapters; other IDs, quantities and prices are compared without normalisation.
