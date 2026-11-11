"""Offline record fixtures: UTC boundaries, archive scope and completeness."""
from datetime import datetime, timezone

import pytest

DAY = '2026-10-28'
MIDNIGHT = int(datetime(2026, 10, 28, tzinfo=timezone.utc).timestamp() * 1000)


def item(kind, interval=None, monthly=False):
    period = DAY[:7] if monthly else DAY
    return dict(type=kind, interval=interval, symbol='BTCUSDT', date=DAY,
                source=f"{'monthly' if monthly else 'daily'}/{kind}/BTCUSDT/BTCUSDT-{interval or kind}-{period}.zip")


def collect(rows, selected):
    from backfill_records import validated_rows
    return list(validated_rows(iter(rows), selected))


def candle(opened, step=900000):
    return [str(opened), '1', '2', '1', '2', '3', str(opened + step - 1),
            '6', '2', '1', '2', '0']


def test_monthly_fallback_validates_source_and_selects_only_target_utc_day():
    rows = [candle(MIDNIGHT - 900000)] + [candle(MIDNIGHT + i * 900000) for i in range(96)]
    rows += [candle(MIDNIGHT + 86400000)]
    result = collect(rows, item('klines', '15m', monthly=True))
    assert len(result) == 96 and result[0][0] == str(MIDNIGHT)
    assert result[-1][0] == str(MIDNIGHT + 95 * 900000)


@pytest.mark.parametrize('kind', ['klines', 'markPriceKlines'])
@pytest.mark.parametrize('step,interval,count', [(60000, '1m', 1440), (300000, '5m', 288)])
def test_full_kline_grid_with_exact_header(kind, step, interval, count):
    from backfill_records import HEADERS
    rows = [list(HEADERS[kind])] + [candle(MIDNIGHT + i * step, step) for i in range(count)]
    assert len(collect(rows, item(kind, interval))) == count


@pytest.mark.parametrize('mutation', ['missing', 'duplicate', 'close', 'nan', 'outside', 'header'])
def test_corrupt_or_incomplete_klines_are_never_complete(mutation):
    rows = [candle(MIDNIGHT + i * 900000) for i in range(96)]
    if mutation == 'missing': rows.pop(12)
    if mutation == 'duplicate': rows[12] = rows[11][:]
    if mutation == 'close': rows[12][6] = rows[12][0]
    if mutation == 'nan': rows[12][4] = 'NaN'
    if mutation == 'outside': rows[12][0] = str(MIDNIGHT - 86400000)
    if mutation == 'header': rows.insert(0, ['unexpected'] * 12)
    with pytest.raises(ValueError):
        collect(rows, item('klines', '15m'))


@pytest.mark.parametrize('kind,row', [
    ('trades', ['12', '2', '3', '6', str(MIDNIGHT), 'true']),
    ('aggTrades', ['12', '2', '3', '10', '11', str(MIDNIGHT), 'false']),
    ('bookDepth', ['2026-10-28 00:00:04', '-1', '3', '6']),
    ('metrics', ['2026-10-28 00:00:00', 'BTCUSDT', '1', '2', '3', '4', '5', '6']),
])
def test_observed_daily_schemas_and_utc_boundaries(kind, row):
    assert collect([row], item(kind)) == [row]
    with pytest.raises(ValueError):
        collect([row, row], item(kind))


def test_metrics_left_closed_boundary_and_monthly_filter():
    rows = [['2026-10-28 00:00:00', 'BTCUSDT', '1', '2', '3', '4', '5', '6'],
            ['2026-10-28 00:05:00', 'BTCUSDT', '1', '2', '3', '4', '5', '6'],
            ['2026-10-29 00:00:00', 'BTCUSDT', '1', '2', '3', '4', '5', '6']]
    assert collect(rows, item('metrics', monthly=True)) == rows[:2]
    assert collect([rows[0]], item('metrics')) == [rows[0]]
    with pytest.raises(ValueError):
        collect([rows[-1]], item('metrics'))


@pytest.mark.parametrize('row', [
    ['2026-10-28T07:00:00+07:00', '1', '2', '3'],
    ['2026-10-29 00:00:00', '1', '2', '3'],
    ['2026-10-28 01:00:00', 'NaN', '2', '3'],
])
def test_depth_rejects_wrong_timezone_date_and_nonfinite_numbers(row):
    with pytest.raises(ValueError): collect([row], item('bookDepth'))


def test_depth_allows_multiple_distinct_percentages_per_timestamp():
    rows = [['2026-10-28 00:00:04', p, '3', '6'] for p in ['-1', '1']]
    assert collect(rows, item('bookDepth')) == rows
    with pytest.raises(ValueError): collect(rows + [rows[0]], item('bookDepth'))


def test_funding_is_monthly_and_signed_rate_is_valid():
    selected = dict(item('fundingRate', monthly=True), date='2026-10')
    rows = [[str(MIDNIGHT), '8', '-0.0001']]
    assert collect(rows, selected) == rows
    with pytest.raises(ValueError): collect([[str(MIDNIGHT), '0', '0.1']], selected)


@pytest.mark.parametrize('kind', ['trades', 'aggTrades', 'bookDepth', 'metrics', 'fundingRate'])
def test_empty_archives_do_not_prove_completeness(kind):
    with pytest.raises(ValueError): collect([], item(kind))


@pytest.mark.parametrize('row', [
    ['12', '2', '3', '6', str(MIDNIGHT * 1000), 'true'],
    ['12', '2', '3', '6', str(MIDNIGHT), 'yes'],
    ['12', '2', '3', '6', str(MIDNIGHT), 'true', 'extra'],
])
def test_trade_units_boolean_and_field_count_are_strict(row):
    with pytest.raises(ValueError): collect([row], item('trades'))


def test_monthly_rows_outside_source_month_are_rejected():
    rows = [candle(MIDNIGHT - 40 * 86400000)]
    with pytest.raises(ValueError): collect(rows, item('klines', '15m', monthly=True))


def test_metrics_symbol_matches_requested_symbol():
    row = ['2026-10-28 00:05:00', 'ETHUSDT', '1', '2', '3', '4', '5', '6']
    with pytest.raises(ValueError): collect([row], item('metrics'))


@pytest.mark.parametrize('kind,rows', [
    ('trades', [['2', '2', '3', '6', str(MIDNIGHT + 1), 'true'],
                ['3', '2', '3', '6', str(MIDNIGHT), 'true']]),
    ('aggTrades', [['2', '2', '3', '1', '2', str(MIDNIGHT + 1), 'true'],
                   ['3', '2', '3', '3', '4', str(MIDNIGHT), 'true']]),
    ('bookDepth', [['2026-10-28 00:05:00', '1', '2', '3'],
                   ['2026-10-28 00:04:00', '2', '2', '3']]),
    ('fundingRate', [[str(MIDNIGHT + 1), '8', '0.001'],
                     [str(MIDNIGHT), '8', '0.001']]),
])
def test_non_metrics_still_reject_unordered_timestamps(kind, rows):
    with pytest.raises(ValueError, match='Unordered timestamps'):
        collect(rows, item(kind))


def test_metrics_sorts_by_parsed_create_time():
    rows = [[instant, 'BTCUSDT', '1', '2', '3', '4', '5', '6'] for instant in
            ['2026-10-28 00:40:00', '2026-10-28T00:05:00Z', '2026-10-28 00:10:00']]
    assert collect(rows, item('metrics')) == [rows[1], rows[2], rows[0]]


def test_metrics_nonadjacent_duplicate_instants_remain_invalid():
    rows = [[instant, 'BTCUSDT', '1', '2', '3', '4', '5', '6'] for instant in
            ['2026-10-28 00:05:00', '2026-10-28 00:10:00', '2026-10-28T00:05:00Z']]
    with pytest.raises(ValueError):
        collect(rows, item('metrics'))
