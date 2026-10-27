"""Backtest executor contracts: deterministic venue time, matching and accounting.

Run from the package parent with python -m pytest
PredictionMarketEventSniper/tests/executors/test_backtest_executor.py -q.
No network calls or real-time sleeps. The fake adapter is independently controlled.
"""
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal as D
from random import Random
from unittest.mock import Mock

import pytest

from PredictionMarketEventSniper.core.events import EventEnvelope
from PredictionMarketEventSniper.core.events.execution import (
    OrderCancelled, OrderCancellationRejected, OrderFilled, OrderPartiallyFilled,
    OrderSubmitted, OrderSubmissionRejected,
)
from PredictionMarketEventSniper.core.models.orders import OrderSide, OrderType, TimeInForce
from PredictionMarketEventSniper.core.outputs import CancelOrderIntent, PlaceOrderIntent
from PredictionMarketEventSniper.executors.models import ExecutionState
from PredictionMarketEventSniper.executors.backtest.backtest_executor import BacktestExecutor
from PredictionMarketEventSniper.executors.backtest.models import (
    BookLevel, BookSide, BookUpdate, BookUpdateKind, CancellationValidation,
    ExecutionIntentArrived, OrderExpiryDue, ScheduledExecutionEvent,
    VenueOrderAccepted, VenueOrderRejected,
)
from PredictionMarketEventSniper.executors.backtest.order_book import OrderBook
from PredictionMarketEventSniper.executors.backtest.protocols import VenueAdapter

T = datetime(2026, 1, 1, tzinfo=timezone.utc)


@dataclass  # Deliberately unhashable: reverse lookup must use the adapter's key.
class Instrument:
    key: str


@dataclass(frozen=True)
class TickChanged:
    tick: D


class FakeAdapter(VenueAdapter[Instrument]):
    def __init__(self):
        self.latency = .2
        self.tick = D('.01')
        self.fee_rate = D('0')
        self.rejection = None
        self.cancel_rejection = None
        self.acceptance_override = None
        self.error = None
        self.normalised = []
        self.validations = []
        self.fee_calls = []

    @property
    def processing_latency(self):
        return self.latency

    def instrument_key(self, instrument):
        return instrument.key

    def normalise_event(self, event):
        self.normalised.append(event)
        if isinstance(event.payload, TickChanged):
            self.tick = event.payload.tick
        return event.payload if isinstance(event.payload, BookUpdate) else None

    def validate_place_order(self, instrument, order):
        self.validations.append((instrument, order, self.tick))
        if self.error:
            raise self.error
        if self.acceptance_override is not None:
            return self.acceptance_override
        if self.rejection:
            return VenueOrderRejected(self.rejection)
        if order.limit_price is not None and order.limit_price % self.tick:
            return VenueOrderRejected('Invalid price tick')
        return VenueOrderAccepted(order.quantity, order.limit_price, f'venue-{order.intent_id}')

    def validate_cancel_order(self, instrument, order, record):
        return CancellationValidation(self.cancel_rejection)

    def calculate_fee(self, instrument, order, quantity, price, *, is_maker):
        self.fee_calls.append((instrument.key, order.intent_id, quantity, price, is_maker))
        return quantity * price * self.fee_rate


@pytest.fixture
def adapter():
    return FakeAdapter()


@pytest.fixture
def executor(adapter):
    return BacktestExecutor(.1, adapter, {'home': Instrument('token-home'), 'away': Instrument('token-away')})


def placement(id='buy', quantity='5', price=None, tif=TimeInForce.IOC,
              side=OrderSide.BUY, instrument='home', expiry=0):
    return PlaceOrderIntent(id, instrument, side, D(quantity),
                           OrderType.MARKET if price is None else OrderType.LIMIT,
                           tif, expiry, None if price is None else D(price))


def book(asks=(), bids=(), kind=BookUpdateKind.SNAPSHOT, key='token-home'):
    return BookUpdate(key, kind, tuple(
        [BookLevel(BookSide.ASK, D(p), D(q)) for p, q in asks] +
        [BookLevel(BookSide.BID, D(p), D(q)) for p, q in bids]))


def feed(executor, payload, at: float = 0.0):
    return executor.handle_event(EventEnvelope(T + timedelta(seconds=at), payload))


def submit(executor, intent, at: float = 0.0):
    request = executor.handle_intent(intent, T + timedelta(seconds=at))
    return feed(executor, request.event, at + request.delay)


def types(events):
    return [type(e.event) for e in events]


def test_pending_request_and_notification_timeline(executor, adapter):
    feed(executor, book(asks=[('.4', '5')]))
    request = executor.handle_intent(placement(), T)
    assert request.delay == pytest.approx(.3)
    assert isinstance(request.event, ExecutionIntentArrived)
    assert executor.execution_records['buy'].state is ExecutionState.PENDING_SUBMISSION
    assert not adapter.validations and not executor.fills
    events = feed(executor, request.event, .3)
    assert types(events) == [OrderSubmitted, OrderFilled]
    assert all(e.delay == .1 for e in events)
    assert executor.execution_records['buy'].accepted_at == T + timedelta(seconds=.3)
    assert executor.fills[0].matched_at == T + timedelta(seconds=.3)
    assert executor.fills[0].notification_at == T + timedelta(seconds=.4)


def test_validation_uses_rules_at_processing_time(executor, adapter):
    request = executor.handle_intent(placement(price='.45', tif=TimeInForce.GTC), T)
    feed(executor, TickChanged(D('.1')), .1)
    events = feed(executor, request.event, .3)
    assert types(events) == [OrderSubmissionRejected]
    assert adapter.validations[0][0] is executor.instruments['home']
    assert adapter.validations[0][2] == D('.1')


def test_matching_uses_latest_book_not_request_time_book(executor):
    feed(executor, book(asks=[('.4', '5')]))
    request = executor.handle_intent(placement(), T)
    feed(executor, book(asks=[('.6', '5')]), .1)
    feed(executor, request.event, .3)
    assert executor.fills[0].price == D('.6')


@pytest.mark.parametrize('side,levels,expected', [
    (OrderSide.BUY, [('0.6','3'),('0.4','2')], [D('.4'),D('.6')]),
    (OrderSide.SELL, [('0.4','3'),('0.6','2')], [D('.6'),D('.4')]),
])
def test_multi_level_best_price_first_and_incremental_events(executor, side, levels, expected):
    feed(executor, book(asks=levels if side is OrderSide.BUY else (),
                        bids=levels if side is OrderSide.SELL else ()))
    events = submit(executor, placement(side=side))
    assert types(events) == [OrderSubmitted, OrderPartiallyFilled, OrderFilled]
    assert [f.price for f in executor.fills] == expected
    assert events[1].event.fill_quantity == D('2')
    assert events[1].event.remaining_quantity == D('3')
    assert events[2].event.fill_quantity == D('3')
    assert events[2].event.avg_fill_price == (D('.52') if side is OrderSide.BUY else D('.48'))
    assert executor.execution_records['buy'].filled_quantity == D('5')


@pytest.mark.parametrize('side', [OrderSide.BUY, OrderSide.SELL])
@pytest.mark.parametrize('tif', list(TimeInForce))
def test_limit_boundary_and_time_in_force(executor, side, tif):
    # Exactly the limit is executable; the worse level must never be consumed.
    levels = [('.5','2'),('.6' if side is OrderSide.BUY else '.4','10')]
    feed(executor, book(asks=levels if side is OrderSide.BUY else (),
                        bids=levels if side is OrderSide.SELL else ()))
    events = submit(executor, placement(price='.5', side=side, tif=tif))
    record = executor.execution_records['buy']
    expected = D('0') if tif is TimeInForce.FOK else D('2')
    assert record.filled_quantity == expected
    assert all(f.price == D('.5') for f in executor.fills)
    assert record.state is (ExecutionState.SUBMITTED if tif is TimeInForce.GTC else ExecutionState.CANCELLED)
    assert events[-1].event.remaining_quantity == D('5') - expected


@pytest.mark.parametrize('side', [OrderSide.BUY, OrderSide.SELL])
def test_fok_failure_does_not_consume_any_levels(executor, side):
    levels = [('.4','2'),('.5','2')]
    feed(executor, book(asks=levels if side is OrderSide.BUY else (),
                        bids=levels if side is OrderSide.SELL else ()))
    events = submit(executor, placement(side=side, tif=TimeInForce.FOK))
    assert types(events) == [OrderSubmitted, OrderCancelled]
    assert not executor.fills
    assert not executor.books['home'].consumed[BookSide.ASK if side is OrderSide.BUY else BookSide.BID]
    assert executor.positions['home'] == D('0')


def test_fok_complete_multi_level(executor):
    feed(executor, book(asks=[('.4','2'),('.5','3')]))
    submit(executor, placement(tif=TimeInForce.FOK))
    assert executor.execution_records['buy'].state is ExecutionState.FILLED
    assert sum((f.quantity for f in executor.fills), D('0')) == D('5')


def test_ioc_empty_book_accepts_then_cancels(executor):
    feed(executor, book())
    events = submit(executor, placement())
    assert types(events) == [OrderSubmitted, OrderCancelled]
    assert events[-1].event.remaining_quantity == D('5')


@pytest.mark.parametrize('tif', [TimeInForce.IOC, TimeInForce.FOK])
def test_missing_snapshot_rejects_immediate_order(executor, tif):
    events = submit(executor, placement(tif=tif))
    assert types(events) == [OrderSubmissionRejected]
    assert executor.execution_records['buy'].state is ExecutionState.REJECTED


def test_gtc_waits_for_initial_snapshot(executor):
    submit(executor, placement(price='.5', tif=TimeInForce.GTC))
    feed(executor, book(asks=[('.4','5')], kind=BookUpdateKind.ABSOLUTE), 1)
    assert not executor.fills
    events = feed(executor, book(asks=[('.4','5')]), 2)
    assert types(events) == [OrderFilled]


def test_gtc_partial_fills_across_updates_and_never_overfills(executor):
    feed(executor, book(asks=[('.4','2')]))
    submit(executor, placement(price='.5', tif=TimeInForce.GTC))
    feed(executor, book(asks=[('.4','5')]), 1)
    feed(executor, book(asks=[('.4','100')]), 2)
    assert [f.quantity for f in executor.fills] == [D('2'), D('3')]
    assert executor.execution_records['buy'].remaining_quantity == D('0')


@pytest.mark.parametrize('side', [OrderSide.BUY, OrderSide.SELL])
def test_competing_orders_price_then_time_priority(executor, side):
    feed(executor, book())
    prices = ('.4','.5') if side is OrderSide.BUY else ('.6','.5')
    submit(executor, placement('worse', price=prices[0], tif=TimeInForce.GTC, side=side))
    submit(executor, placement('better-first', price=prices[1], tif=TimeInForce.GTC, side=side), 1)
    submit(executor, placement('better-second', price=prices[1], tif=TimeInForce.GTC, side=side), 2)
    levels = [('.4' if side is OrderSide.BUY else '.6','6')]
    feed(executor, book(asks=levels if side is OrderSide.BUY else (),
                        bids=levels if side is OrderSide.SELL else ()), 3)
    assert [(f.intent_id,f.quantity) for f in executor.fills] == [('better-first',D('5')),('better-second',D('1'))]


def test_same_price_same_acceptance_time_preserves_request_order(executor):
    feed(executor, book())
    a = executor.handle_intent(placement('first',price='.5',tif=TimeInForce.GTC),T)
    b = executor.handle_intent(placement('second',price='.5',tif=TimeInForce.GTC),T)
    feed(executor,a.event,.3)
    feed(executor,b.event,.3)
    feed(executor,book(asks=[('.4','5')]),1)
    assert [f.intent_id for f in executor.fills] == ['first']


def test_consumption_survives_level_removal_and_reappearance(executor):
    feed(executor,book(asks=[('.4','5')]))
    submit(executor,placement(quantity='3'))
    feed(executor,book(),1)
    feed(executor,book(asks=[('.4','5')]),2)
    submit(executor,placement('second',quantity='3'),2)
    assert executor.execution_records['second'].filled_quantity == D('2')


def test_instrument_updates_and_accounting_are_isolated(executor):
    feed(executor,book())
    submit(executor,placement(price='.5',tif=TimeInForce.GTC))
    feed(executor,book(asks=[('.4','10')],key='token-away'),1)
    assert not executor.fills
    submit(executor,placement('away',instrument='away'),1)
    assert executor.positions == {'home':D('0'),'away':D('5')}
    assert executor.execution_records['buy'].state is ExecutionState.SUBMITTED


def test_cancel_targets_placement_and_keeps_partial_fill_totals(executor):
    feed(executor,book(asks=[('.4','2')]))
    submit(executor,placement(price='.5',tif=TimeInForce.GTC))
    events=submit(executor,CancelOrderIntent('cancel','buy'),1)
    event=events[0].event
    assert types(events)==[OrderCancelled]
    assert (event.intent_id,event.venue_order_id,event.instrument_id)==('buy','venue-buy','home')
    assert (event.filled_quantity,event.remaining_quantity)==(D('2'),D('3'))
    assert executor.execution_records['buy'].state is ExecutionState.CANCELLED
    assert feed(executor,book(asks=[('.4','10')]),2)==[]


def test_fill_while_cancel_in_flight_is_not_undone(executor):
    feed(executor,book())
    submit(executor,placement(price='.5',tif=TimeInForce.GTC))
    request=executor.handle_intent(CancelOrderIntent('cancel','buy'),T+timedelta(seconds=1))
    assert executor.execution_records['buy'].state is ExecutionState.SUBMITTED
    feed(executor,book(asks=[('.4','5')]),1.1)
    events=feed(executor,request.event,1.3)
    assert types(events)==[OrderCancellationRejected]
    assert executor.positions['home']==D('5')


@pytest.mark.parametrize('target_state', ['unknown','pending','rejected','filled','cancelled'])
def test_cancel_ineligible_target(executor,adapter,target_state):
    if target_state=='pending':
        executor.handle_intent(placement(),T)
    elif target_state=='rejected':
        adapter.rejection='blocked'
        submit(executor,placement())
    elif target_state=='filled':
        feed(executor,book(asks=[('.4','5')]))
        submit(executor,placement())
    elif target_state=='cancelled':
        feed(executor,book())
        submit(executor,placement())
    events=submit(executor,CancelOrderIntent('cancel','buy'),1)
    event=events[0].event
    assert isinstance(event,OrderCancellationRejected)
    assert (event.intent_id,event.target_intent_id)==('cancel','buy')


def test_adapter_rejected_cancel_does_not_deactivate(executor,adapter):
    adapter.cancel_rejection='venue restriction'
    submit(executor,placement(price='.5',tif=TimeInForce.GTC))
    events=submit(executor,CancelOrderIntent('cancel','buy'),1)
    assert events[0].event.reason=='venue restriction'
    assert executor.execution_records['buy'].state is ExecutionState.SUBMITTED


def test_notifications_and_duplicate_arrivals_do_not_apply_fills_twice(executor,adapter):
    feed(executor,book(asks=[('.4','5')]))
    pending=executor.handle_intent(placement(),T)
    events=feed(executor,pending.event,.3)
    assert feed(executor,pending.event,.3)==[]
    snapshot=executor.get_summary()
    for event in events:
        assert feed(executor,event.event,.4)==[]
        assert feed(executor,event.event,.4)==[]
    assert executor.get_summary()==snapshot
    assert len(adapter.validations)==1


def test_duplicate_intent_does_not_overwrite_original(executor):
    original=placement()
    executor.handle_intent(original,T)
    with pytest.raises(ValueError,match='Duplicate'):
        executor.handle_intent(placement(quantity='10'),T)
    assert executor.execution_records['buy'].intent is original
    with pytest.raises(ValueError,match='Duplicate'):
        executor.handle_intent(CancelOrderIntent('buy','other'),T)


def test_unregistered_arrival_and_timestamp_regression(executor):
    with pytest.raises(ValueError,match='registered'):
        feed(executor,ExecutionIntentArrived(placement()))
    feed(executor,'unrelated',1)
    with pytest.raises(ValueError,match='timestamp ordered'):
        feed(executor,'unrelated',0)


def test_timestamp_fallback_and_missing_initial_clock(executor):
    with pytest.raises(RuntimeError,match='timestamp'):
        executor.handle_intent(placement())
    feed(executor,'unrelated',1)
    executor.handle_intent(placement())
    assert executor.execution_records['buy'].requested_at==T+timedelta(seconds=1)


@pytest.mark.parametrize('invalid', [-1,float('nan'),float('inf')])
def test_invalid_latency_and_delay(adapter,invalid):
    with pytest.raises(ValueError):
        BacktestExecutor(invalid,adapter,{'home':Instrument('token-home')})
    with pytest.raises(ValueError):
        ScheduledExecutionEvent(ExecutionIntentArrived(placement()),invalid)
    executor=BacktestExecutor(.1,adapter,{'home':Instrument('token-home')})
    adapter.latency=invalid
    with pytest.raises(ValueError):
        executor.handle_intent(placement(),T)
    assert executor.execution_records=={}


def test_duplicate_instrument_keys_and_unknown_feed(adapter,executor):
    with pytest.raises(ValueError,match='Duplicate'):
        BacktestExecutor(0,adapter,{'one':Instrument('same'),'two':Instrument('same')})
    assert feed(executor,book(asks=[('.4','5')],key='unknown'))==[]
    assert not executor.books['home'].initialised


@pytest.mark.parametrize('result,error_type', [
    (object(),TypeError),
    (VenueOrderAccepted(D('4'),None,'id'),ValueError),
    (VenueOrderAccepted(D('5'),D('.5'),'id'),ValueError),
    (VenueOrderAccepted(D('5'),None,''),ValueError),
])
def test_invalid_adapter_results_propagate(executor,adapter,result,error_type):
    adapter.acceptance_override=result
    with pytest.raises(error_type):
        submit(executor,placement())
    assert not executor.fills


def test_adapter_exception_is_not_a_simulated_rejection(executor,adapter):
    error=RuntimeError('adapter bug')
    adapter.error=error
    with pytest.raises(RuntimeError) as caught:
        submit(executor,placement())
    assert caught.value is error
    assert executor.execution_records['buy'].state is ExecutionState.PENDING_SUBMISSION


def test_rejection_and_unknown_instrument_have_no_accounting_effect(executor,adapter):
    adapter.rejection='disabled'
    events=submit(executor,placement())
    assert events[0].event.reason=='disabled'
    assert executor.execution_records['buy'].rejection_reason=='disabled'
    events=submit(executor,placement('other',instrument='missing'),1)
    assert types(events)==[OrderSubmissionRejected]
    assert len(adapter.validations)==1
    assert not executor.fills


def test_expiry_due_and_deadline_book_race(executor):
    feed(executor,book())
    events=submit(executor,placement(price='.5',tif=TimeInForce.GTC,expiry=2))
    expiry=next(e for e in events if isinstance(e.event,OrderExpiryDue))
    assert expiry.delay==2
    assert executor.execution_records['buy'].expires_at==T+timedelta(seconds=2.3)
    assert feed(executor,expiry.event,1)==[]  # Early wake-up does not expire.
    events=feed(executor,book(asks=[('.4','5')]),2.3)
    assert types(events)==[OrderCancelled]
    assert feed(executor,expiry.event,2.3)==[]
    assert not executor.fills


def test_no_expiry_for_zero_and_stale_expiry_for_filled_order(executor):
    feed(executor,book(asks=[('.4','5')]))
    events=submit(executor,placement())
    assert not any(isinstance(e.event,OrderExpiryDue) for e in events)
    assert feed(executor,OrderExpiryDue('buy'),1)==[]
    assert feed(executor,OrderExpiryDue('unknown'),1)==[]


@pytest.mark.parametrize('fee_rate', ['.01','-.01'])
def test_buy_sell_accounting_and_fee_rebates(executor,adapter,fee_rate):
    adapter.fee_rate=D(fee_rate)
    feed(executor,book(asks=[('.4','5')],bids=[('.6','5')]))
    submit(executor,placement())
    submit(executor,placement('sell',side=OrderSide.SELL),1)
    summary=executor.get_summary()
    expected_fees=D('5')*D(fee_rate)
    assert summary.positions['home']==D('0')
    assert summary.cash_flows['home']==D('1')-expected_fees
    assert summary.total_fees==expected_fees
    assert sum((r.fees for r in executor.execution_records.values()),D('0'))==expected_fees
    summary.positions['home']=D('999')
    summary.cash_flows['home']=D('999')
    assert executor.get_summary().positions['home']==D('0')


def test_maker_taker_context_and_nonfinite_fee_atomicity(executor,adapter):
    feed(executor,book(asks=[('.4','2')]))
    submit(executor,placement(price='.5',tif=TimeInForce.GTC))
    feed(executor,book(asks=[('.4','5')]),1)
    assert [call[-1] for call in adapter.fee_calls]==[False,True]
    before=executor.get_summary()
    feed(executor,book(asks=[('.5','2'),('.6','3')]),2)
    adapter.calculate_fee=Mock(side_effect=[D('0'),D('NaN')])
    with pytest.raises(ValueError,match='nonfinite'):
        submit(executor,placement('bad'),2)
    assert executor.get_summary().fills==before.fills
    assert not executor.books['home'].consumed[BookSide.ASK].get(D('.5'))


@pytest.mark.parametrize('leave_open',[False,True])
@pytest.mark.asyncio
async def test_shutdown_preserves_fills_and_handles_pending_and_open(executor,leave_open):
    feed(executor,book(asks=[('.4','2')]))
    submit(executor,placement(price='.5',tif=TimeInForce.GTC))
    pending = executor.handle_intent(placement('pending', price='.3', tif=TimeInForce.GTC),T+timedelta(seconds=1))
    before=executor.get_summary()
    cleanup = executor.begin_shutdown(T+timedelta(seconds=1), cancel_open_orders=not leave_open)
    assert executor.begin_shutdown(T+timedelta(seconds=1), cancel_open_orders=not leave_open) == []
    # Neither request has been processed yet: no imaginary cancellation.
    assert executor.execution_records['buy'].state is ExecutionState.SUBMITTED
    arrivals = feed(executor, pending.event, 1 + pending.delay)
    for event in cleanup:
        feed(executor, event.event, 1 + event.delay)
    for event in arrivals:
        if isinstance(event.event, ExecutionIntentArrived):
            feed(executor, event.event, 1 + pending.delay + event.delay)
    await executor.finalise()
    await executor.finalise()
    after=executor.get_summary()
    assert after.fills==before.fills
    assert after.positions==before.positions
    assert set(after.open_order_ids)==({'buy','pending'} if leave_open else set())
    with pytest.raises(RuntimeError):
        executor.handle_intent(placement('new'),T+timedelta(seconds=2))
    with pytest.raises(RuntimeError):
        feed(executor,book(),2)


def test_randomised_update_sequence_preserves_quantity_and_cash_invariants(executor):
    rng=Random(417)
    feed(executor,book())
    for i in range(10):
        submit(executor,placement(f'order-{i}',quantity='7',price='.5',tif=TimeInForce.GTC),i)
    max_depth={D('.4'):D('0'),D('.5'):D('0')}
    for i in range(60):
        values={p:D(rng.randrange(0,25)) for p in max_depth}
        max_depth={p:max(max_depth[p],values[p]) for p in values}
        feed(executor,book(asks=[(str(p),str(q)) for p,q in values.items()]),10+i)
        fills=executor.get_summary().fills
        for record in executor.execution_records.values():
            assert D('0')<=record.filled_quantity<=record.intent.quantity
            assert record.remaining_quantity>=D('0')
        for price,maximum in max_depth.items():
            assert sum((f.quantity for f in fills if f.price==price),D('0'))<=maximum
        assert executor.positions['home']==sum((f.quantity for f in fills),D('0'))
        assert executor.cash_flows['home']==-sum((f.quantity*f.price for f in fills),D('0'))


# Book unit tests exercise normalisation semantics independently of order lifecycle.
@pytest.mark.parametrize('kind,quantity,expected', [
    (BookUpdateKind.ABSOLUTE,'3','3'),
    (BookUpdateKind.DELTA,'3','8'),
    (BookUpdateKind.DELTA,'-3','2'),
])
def test_book_update_semantics(kind,quantity,expected):
    b=OrderBook()
    b.apply(book(asks=[('.4','5'),('.5','2')],bids=[('.3','7')]))
    b.apply(book(asks=[('.4',quantity)],kind=kind))
    assert b.levels[BookSide.ASK]=={D('.4'):D(expected),D('.5'):D('2')}
    assert b.levels[BookSide.BID]=={D('.3'):D('7')}


@pytest.mark.parametrize('kind,quantity', [(BookUpdateKind.ABSOLUTE,'0'),(BookUpdateKind.DELTA,'-5')])
def test_book_zero_removes_level(kind,quantity):
    b=OrderBook()
    b.apply(book(asks=[('.4','5')]))
    b.apply(book(asks=[('.4',quantity)],kind=kind))
    assert D('.4') not in b.levels[BookSide.ASK]


@pytest.mark.parametrize('bad_price,bad_quantity', [('-1','1'),('NaN','1'),('Infinity','1'),('.4','NaN'),('.4','Infinity'),('.4','-1')])
def test_invalid_book_update_is_atomic(bad_price,bad_quantity):
    b=OrderBook()
    b.apply(book(asks=[('.4','5')]))
    with pytest.raises(ValueError):
        b.apply(book(asks=[('.5','2'),(bad_price,bad_quantity)],kind=BookUpdateKind.ABSOLUTE))
    assert b.levels[BookSide.ASK]=={D('.4'):D('5')}


def test_snapshot_replaces_both_sides_without_resetting_consumption():
    b=OrderBook()
    b.apply(book(asks=[('.4','5')],bids=[('.3','7')]))
    b.consume(OrderSide.BUY,D('.4'),D('3'))
    b.apply(book(asks=[('.4','2')]))
    assert b.levels[BookSide.BID]=={}
    assert b.available(BookSide.ASK,D('.4'))==D('0')
    b.apply(book(asks=[('.4','6')]))
    assert b.available(BookSide.ASK,D('.4'))==D('3')


@pytest.mark.parametrize('amount',['0','-1','6'])
def test_cannot_consume_invalid_or_excess_quantity(amount):
    b=OrderBook()
    b.apply(book(asks=[('.4','5')]))
    with pytest.raises(ValueError):
        b.consume(OrderSide.BUY,D('.4'),D(amount))
    assert b.available(BookSide.ASK,D('.4'))==D('5')
