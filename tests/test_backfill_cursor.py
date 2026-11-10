"""UTC progress must distinguish acknowledged upstream absence from new gaps."""
from datetime import date
import json

import pytest


def entry(symbol='BTCUSDT'):
    return dict(symbol=symbol, type='bookDepth', date='2026-10-08',
                source='https://data.binance.vision/?prefix=data/futures/um/daily/bookDepth/',
                checked_at='2026-11-10')


def test_known_absences_remain_unavailable_and_never_complete(tmp_path):
    from backfill import make_plan
    from backfill_cursor import classify_unavailable
    from backfill_executor import execute_plan
    plan = make_plan(date(2026, 10, 8), date(2026, 10, 9), ('BTCUSDT', 'ETHUSDT'),
                     ('bookDepth',), ('1m',), head=lambda key: None, today=date(2026, 11, 10))
    classify_unavailable(plan, [entry(), entry('ETHUSDT')])
    result = execute_plan(plan, tmp_path)
    assert result['missing'] == 2 and result['unavailable'] == 2
    assert result['complete_days'] == []
    assert result['settled_days'] == ['2026-10-08']
    assert result['outcome'] == 'incomplete'


@pytest.mark.parametrize('patch', [dict(type='trades'), dict(date='20261008'),
                                   dict(checked_at='tomorrow'), dict(extra=True)])
def test_absence_inventory_is_closed_and_dates_are_checked(patch):
    from backfill_cursor import classify_unavailable
    with pytest.raises(ValueError):
        classify_unavailable(dict(files=[], missing=0), [dict(entry(), **patch)])


def test_upstream_publication_overrides_acknowledged_absence():
    from backfill_cursor import classify_unavailable
    item = dict(symbol='BTCUSDT', type='bookDepth', date='2026-10-08',
                interval=None, source='published.zip', status='available')
    plan = dict(files=[item], missing=0)
    classify_unavailable(plan, [entry()])
    assert item['status'] == 'available' and plan['unavailable'] == 0


def test_cursor_advances_only_through_contiguous_settled_dates(tmp_path):
    from backfill_cursor import run_cursor
    calls = []
    def execute(start, end):
        calls.append((start, end))
        return dict(outcome='incomplete', settled_days=['2026-10-08', '2026-10-10'],
                    complete_days=['2026-10-10'], unavailable=2, missing=1)
    result = run_cursor(tmp_path, date(2026, 10, 8), date(2026, 10, 11), execute)
    assert result['cursor'] == '2026-10-09'
    assert json.loads((tmp_path / '.cold-cursor.json').read_text()) == dict(
        timezone='UTC', next_date='2026-10-09')
    run_cursor(tmp_path, date(2026, 6, 29), date(2026, 10, 11), execute)
    assert calls == [(date(2026, 10, 8), date(2026, 10, 10)),
                     (date(2026, 10, 9), date(2026, 10, 10))]


@pytest.mark.parametrize('patch', [dict(outcome='failed'), dict(cursor_blocked=True)])
def test_failure_or_missing_closed_funding_does_not_advance_cursor(tmp_path, patch):
    from backfill_cursor import run_cursor
    result = run_cursor(tmp_path, date(2026, 10, 8), date(2026, 10, 9),
                        lambda *args: dict(outcome='complete', settled_days=['2026-10-08'], **patch)
                        if 'outcome' not in patch else dict(settled_days=['2026-10-08'], **patch))
    assert result['cursor'] == '2026-10-08'


def test_invalid_persisted_cursor_fails_before_downloading(tmp_path):
    from backfill_cursor import run_cursor
    (tmp_path / '.cold-cursor.json').write_text('{"timezone":"local","next_date":"2026-10-08"}')
    with pytest.raises(ValueError):
        run_cursor(tmp_path, date(2026, 10, 8), date(2026, 10, 9),
                   lambda *args: pytest.fail('invalid cursor reached downloader'))
