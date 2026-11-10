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


def test_late_publication_is_recovered_after_cursor_passed_absence(tmp_path, monkeypatch, capsys):
    import hashlib
    from io import BytesIO
    import backfill_cursor as cursor
    from backfill import BASE, make_plan
    from backfill_executor import execute_plan
    from test_backfill_archives import zipped
    published = False
    requests = []
    key = 'daily/bookDepth/BTCUSDT/BTCUSDT-bookDepth-2026-10-08.zip'
    payload = zipped([(key.rsplit('/', 1)[-1][:-4] + '.csv',
                      b'timestamp,percentage,depth,notional\n2026-10-08 00:00:04,-1,3,6\n')])
    def head(candidate):
        requests.append(candidate)
        return len(payload) if published and candidate.removesuffix('.CHECKSUM') == key else None
    def plan(start, end, *_args, **kwargs):
        return make_plan(start, end, ('BTCUSDT',), ('bookDepth',), ('1m',),
                         head=head, today=kwargs['today'])
    def fetch(url, timeout):
        assert published and url.removesuffix('.CHECKSUM') == BASE + key
        return BytesIO((hashlib.sha256(payload).hexdigest() + '  ' + key.rsplit('/', 1)[-1]).encode()
                       if url.endswith('.CHECKSUM') else payload)
    monkeypatch.setattr(cursor, 'make_plan', plan)
    monkeypatch.setattr(cursor, 'execute_plan', lambda plan, root: execute_plan(plan, root, fetch))
    argv = ['--initial-date', '2026-10-08', '--data-root', str(tmp_path)]
    assert cursor.main(argv, today=date(2026, 10, 9)) == 1
    first = json.loads(capsys.readouterr().out)
    assert first['cursor'] == '2026-10-09' and first['unavailable'] == 1
    published = True
    requests.clear()
    cursor.main(argv, today=date(2026, 10, 10))
    second = json.loads(capsys.readouterr().out)
    assert key in requests
    assert second['written'] == 1 and second['unavailable'] == 0
    target = tmp_path / 'BTCUSDT/bookDepth/BTCUSDT-bookDepth-2026-10-08.csv'
    assert target.exists() and target.with_suffix('.json').exists()


def test_cursor_persistence_failure_preserves_published_counts(tmp_path, monkeypatch, capsys):
    import backfill_cursor as cursor
    monkeypatch.setattr(cursor, 'make_plan', lambda *args, **kwargs: dict(files=[], missing=0))
    monkeypatch.setattr(cursor, 'execute_plan', lambda *args: dict(timezone='UTC',
        outcome='complete', written=2, skipped=3, records=5, missing=0,
        settled_days=['2026-11-09']))
    def fail_replace(*args):
        raise OSError('fixture cursor storage failure')
    monkeypatch.setattr(cursor.os, 'replace', fail_replace)
    assert cursor.main(['--initial-date', '2026-11-09', '--data-root', str(tmp_path)],
                       today=date(2026, 11, 10)) == 1
    lines = capsys.readouterr().out.splitlines()
    assert len(lines) == 1
    record = json.loads(lines[0])
    assert record['written'] == 2 and record['skipped'] == 3 and record['records'] == 5
    assert record['outcome'] == 'failed' and record['phase'] == 'cursor'
