# Prediction Market Event-Driven Trading Engine

**Status: Work in progress — framework and mock-market testing available; recording and real-market integrations are not complete.**

A Python research project exploring an event-driven architecture for strategies that react to external events and interact with prediction-market order books. The goal is to run strategy logic through both asynchronous live-style processing and historical replay, with separable market-data feeds, strategy orchestration and execution adapters.

> **This is a development snapshot, not a deployable live trading bot.** The included live demonstration uses a local simulated market. No production prediction-market execution or historical data recording is provided by this release.

## What is implemented

- **Live engine:** asynchronous strategy-instance lifecycle management, event dispatch, timers and routing of strategy outputs.
- **Backtest engine:** deterministic processing of supplied events, including ordering, timer scheduling and routing of simulated execution results.
- **Execution abstractions:** separate live and backtest executors, with venue-adapter protocols and mock adapters; backtest order-book and fill simulation.
- **Mock market and scenario:** local market feed/trading server, mock strategy and configuration-driven live demonstration.
- **Automated tests:** engine lifecycle, concurrency, ordering, routing, timers, failure handling, execution and live/backtest behavioural parity.

These components are designed to be extended with real external event feeds and prediction-market venues; the repository does **not** imply those integrations are operational today.

## Current development status

| Area | State |
| --- | --- |
| Live engine and lifecycle | Implemented; exercised by automated tests |
| Backtest event processing and simulated execution | Implemented; exercised by automated tests |
| Mock market, adapters and scenario | Implemented for local development |
| Live/backtest behavioural parity tests | Included |
| Live orchestration with real prediction-market feeds | Not implemented in this snapshot |
| Historical-data backtest orchestration / reader | Not implemented (`backtest.py` raises `NotImplementedError` for enabled strategies) |
| **Recording / persistence / replay from stored feeds** | **Not implemented** — `recording/recorder.py` is an early scaffold; no storage adapter, persistence or integration with the live engine |
| Production venue adapter (e.g. Polymarket) | Not implemented; the adapter file is a placeholder |

**Recording is the next major development milestone.** The intended design is to capture the external events consumed by live strategies, preserve ordering and metadata, and use those recordings to drive reproducible historical backtests. The current `Recorder` class should not be used for actual data capture: its write methods are placeholders and it has not been integrated into the live runtime.

## Architecture at a glance

```text
External event feeds / mock feed
              |
       Strategy orchestrator
              |
      LiveEngine / BacktestEngine
              |
       Strategy instances
              |
    Strategy outputs (orders, timers, stop)
              |
     LiveExecutor / BacktestExecutor
              |
       Venue adapter / simulated book
```

The engines and strategy contracts are kept separate from venue-specific behaviour to support testing and future integrations.

## Repository layout

```text
core/             Event, order and strategy-output models
engine/           Live and backtest event-processing engines
strategy/         Strategy contracts and orchestrator interfaces
strategies/       Mock scenario and in-progress strategy experiments
executors/        Live/backtest executors and venue adapter interfaces
feeds/            Mock/testing feeds and early external-feed models
recording/        Incomplete recording prototype (not functional)
tests/            Unit, integration and runtime-parity tests
live.py           Configurable mock live-run entry point
backtest.py       Backtest entry point scaffold
live.toml         Local mock-market configuration
backtest.toml     Disabled backtest example configuration
```

## Running the tests

Requires **Python 3.11+** (uses `tomllib`). The project is a source snapshot rather than an installable distribution; run commands from the **parent directory of this repository**, so the repository folder is importable as a Python package. Rename the extracted folder to `PredictionMarketEventSniper` or adjust the import path accordingly.

```powershell
python -m pip install pytest pytest-asyncio
python -m pytest PredictionMarketEventSniper/tests -v
```

Tests using the local mock market do not require a live trading account. Additional dependencies may be needed for specific modules; dependency packaging is not yet finalised.

## Running the mock live scenario

From the parent directory, with the folder named `PredictionMarketEventSniper`:

```powershell
python -m PredictionMarketEventSniper.live --config PredictionMarketEventSniper/live.toml
```

This starts a **local simulated** market and mock strategy. Stop with `Ctrl+C`. `live.toml` uses a demonstration-only `test-key`, **not a real API credential**.

Enabling `recording.enabled` currently raises `NotImplementedError`. Similarly, `backtest.py` does not yet provide a complete historical replay workflow.

## Next steps

1. Implement persistent recording adapters and integrate recording into live orchestration.
2. Add recorded-feed readers and historical backtest orchestration.
3. Connect external event streams and real prediction-market data/venue adapters.
4. Expand end-to-end validation and document repeatable research workflows.

## Disclaimer

This is experimental software for architecture development and strategy research. It is not investment advice and should not be used to place real-money orders without substantial additional implementation, testing and risk controls.
