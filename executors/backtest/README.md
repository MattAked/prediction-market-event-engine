# Backtest executor v1

Extract this bundle into the repo root. It replaces only the backtest executor,
models and adapter contract, and adds the book implementation, mock adapter and
tests. Core execution events and shared ExecutionState are unchanged. This integrated
bundle also updates the engine and its tests.

## Timeline and engine contract

Methods are synchronous except `shutdown`. This is serial offline simulation.

```python
scheduled = executor.handle_intent(intent, timestamp=current_timestamp)
# Enqueue scheduled.event at current_timestamp + timedelta(seconds=scheduled.delay).

scheduled_events = executor.handle_event(envelope)
# Enqueue each returned event relative to envelope.timestamp.
```

`handle_intent` returns ONE `ScheduledExecutionEvent`. Its payload is
`ExecutionIntentArrived`, and its delay includes outbound travel plus adapter
processing. It can omit timestamp after `handle_event` establishes current time.
The old payload-level delay has moved into the common scheduling wrapper.

Internal events (`ExecutionIntentArrived`, `OrderExpiryDue`) go to the executor
only. Existing `ExecutionEvent` notifications go to strategy logic; feeding them
through the executor too is harmless and does not apply transitions twice.
Historical events go to the executor before the strategy, so the book and adapter
rules are current. Non-book events still reach adapter normalisation.

At arrival, placement validation runs against current venue rules. Acceptance,
matching and cancellation update venue state immediately. All strategy-facing
responses receive inbound latency. Processing latency is NOT added again.
Example: 100ms outbound + 200ms processing -> submission handled at T+300ms;
100ms inbound -> strategy learns the outcome at T+400ms. Immediate fills are
emitted after OrderSubmitted in list order at the same notification time.

The engine must retain the deterministic timestamp/insertion ordering already
agreed on and convert float seconds to timedelta. The bundled engine performs this integration using the engine-facing
BacktestExecutorProtocol in executors/backtest/protocols.py. The earlier top-level
executors/backtest_protocols.py is no longer imported and can be removed if unused.

## Venue adapter

Subclass `VenueAdapter[InstrumentT]`. Supply `instrument_key`, `normalise_event`,
and `validate_place_order(instrument, intent)`. Validation returns an acceptance
with a unique simulated venue ID or a rejection reason. v1 acceptance must retain
the original quantity and limit price; market accepted_price is None.

`processing_latency` defaults to zero seconds. Optional `validate_cancel_order`
receives the instrument, cancellation request and target record, returning
`CancellationValidation`. Default permits cancellation after executor state checks.
`calculate_fee` defaults to zero and runs per level fill, with maker/taker context.
Adapter exceptions propagate as implementation/data failures, not pretend venue
rejections. Fee values may be negative to represent rebates.

The mock adapter receives BookUpdate directly; a real venue adapter normalises
its own recorded payload types. A real adapter may maintain changing tick rules
while returning None for those events. No Polymarket adapter is implemented here.

## Books, orders and accounting

- BookUpdate uses external instrument_key; the executor derives the reverse
  lookup from the supplied internal-ID-to-instrument mapping. Duplicate external
  keys are rejected. Unknown book instruments are ignored.
- A full initial snapshot is required to match. IOC/FOK submissions are rejected
  without one; GTC can wait for one. A known empty snapshot is valid.
- Snapshots replace recorded depth; absolute updates replace supplied levels;
  deltas adjust supplied levels. Zero removes a level. Invalid updates apply
  atomically or raise. Consumption remains separate and persists for the run,
  including across removal and reappearance of a price level.
- Buys consume lowest asks; sells consume highest bids. Limits bound prices.
  Matching changes fill quantity and consumes depth immediately. Later historical
  changes cannot undo fills. One notification/ledger row is emitted per level.
- IOC cancels unfilled remainder. FOK preflights the whole quantity and, if
  insufficient, produces acceptance then cancellation without consuming depth.
  GTC limits remain active, ordered by price then acceptance time/insertion order.
  This crossing-book model does not infer passive queue fills from same-side depth.
- Requests, including cancellations, reserve unique intent IDs. Cancel requests
  target target_intent_id and do not deactivate the order until processed. Pending,
  filled, cancelled or rejected targets cannot be cancelled; a rejected cancellation
  leaves an active target untouched. OrderCancelled refers to the placement ID;
  cancellation rejections refer to the cancellation ID and target ID.
- limit_expiry is interpreted as seconds from acceptance; zero means no expiry.
  OrderExpiryDue disables the unmatched remainder. Even if the expiry wake-up and
  a book update tie, matching checks the deadline before filling.
- fill_quantity on each notification is incremental. remaining_quantity on a
  cancellation is the unfilled amount cancelled, not the active amount afterwards.
- get_summary returns copies of fills/positions/cash flows, fees and open IDs.
  Fills have matched_at and notification_at. Cash flows are per instrument and
  are not PnL. v1 does not enforce initial capital, position limits or inventory;
  selling can produce a negative position. Currency conversion, settlement and
  realised/unrealised PnL are intentionally not guessed.
- `begin_shutdown(timestamp, grace_period=..., cancel_open_orders=...)` disables
  new strategy intents and schedules cancellation requests for active orders.
  In-flight placements remain eligible for acceptance/matching, then receive
  shutdown cancellations if still active. `handle_event` remains available.
- `finalise()` closes local resources without fabricating venue transitions.
  Natural exhaustion uses finalise only. Standalone callers replace the old
  shutdown method with begin_shutdown + bounded event processing + finalise.
- The engine owns the grace deadline and keeps routing historical updates and
  executor events to the executor only. No backtest termination hook is invoked.
  Errors propagate; local finalisation does not simulate recovery trading.
- get_summary additionally exposes pending_request_ids, shutdown_started_at,
  cleanup_deadline and finalised_at. Open orders, pending requests and existing
  positions remain inspectable if grace is insufficient or history is exhausted.
  Positions are not forcibly liquidated and orders are not silently cancelled.


## Tests

From the directory containing the PredictionMarketEventSniper package:

```bash
python -m unittest discover -s PredictionMarketEventSniper/tests/executors -p test_backtest_executor.py -v
```

The 23 standard-library tests also work with pytest. No real sleeps/network calls.

## Clock ownership

The engine is the sole clock authority. It passes its current timestamp to
handle_intent and supplies timestamps through EventEnvelope to handle_event.
The executor remembers this timestamp to record execution times, check expiry
and finalise shutdown. It never advances time independently. All returned delays
are relative to the current engine timestamp.

The integration calls executor.handle_event before strategy.handle_event and
schedules its outputs. Internal requests and expiry wake-ups are not delivered
to strategy logic. Existing execution notifications are delivered unchanged.

Backtest strategy termination hooks are not invoked. The only cleanup trading
comes from already-scheduled work and executor-generated shutdown cancellations.
Errors and cancellation propagate after resource finalisation. The initial
engine regression count below is superseded by the shutdown tests.

Run the 18 engine regression/integration tests from the package parent:

```bash
python -m unittest discover -s PredictionMarketEventSniper/tests/engines/backtest_engine_tests -v
```
