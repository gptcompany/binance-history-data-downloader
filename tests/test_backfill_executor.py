"""Real offline executor: archive integrity, atomic publication and restart safety."""
import csv
from datetime import date, datetime, timezone
import fcntl
import hashlib
import json

import pytest

from backfill import make_plan
from test_backfill_archives import CSV, KEY, NAME, transport, zipped


def fixture_plan(end='2026-10-28'):
    payload = zipped()
    return make_plan(date(2026, 10, 28), date.fromisoformat(end), ['BTCUSDT'],
                     ['trades'], ['1m'], lambda _: len(payload), today=date(2026, 11, 10))


def test_publish_and_rerun_skip_only_hash_verified_csv(tmp_path):
    from backfill_executor import execute_plan
    plan = fixture_plan()
    fetch, calls, digest = transport(zipped())
    result = execute_plan(plan, tmp_path, fetch)
    target = tmp_path / plan['files'][0]['target']
    manifest = json.loads(target.with_suffix('.json').read_text())
    assert result['outcome'] == 'complete' and result['written'] == 1
    assert result['records'] == 1 and result['complete_days'] == ['2026-10-28']
    assert manifest['archive_sha256'] == digest and manifest['rows'] == 1
    assert manifest['timezone'] == 'UTC' and manifest['source'] == KEY
    assert list(csv.reader(target.open()))[-1][-1] == 'true'
    calls.clear()
    result = execute_plan(plan, tmp_path, fetch)
    assert result['skipped'] == 1 and result['written'] == 0 and not calls
    target.write_text('corrupted')
    result = execute_plan(plan, tmp_path, fetch)
    assert result['written'] == 1 and len(calls) == 2


@pytest.mark.parametrize('corrupt', ['missing', 'extra', 'source'])
def test_untrusted_or_missing_manifest_forces_verification(tmp_path, corrupt):
    from backfill_executor import execute_plan
    plan = fixture_plan()
    fetch, calls, _ = transport(zipped())
    execute_plan(plan, tmp_path, fetch)
    target = tmp_path / plan['files'][0]['target']
    sidecar = target.with_suffix('.json')
    manifest = json.loads(sidecar.read_text())
    if corrupt == 'missing': sidecar.unlink()
    else:
        manifest['unexpected' if corrupt == 'extra' else 'source'] = 'invalid'
        sidecar.write_text(json.dumps(manifest))
    calls.clear()
    assert execute_plan(plan, tmp_path, fetch)['written'] == 1
    assert len(calls) == 2


def test_late_invalid_record_preserves_existing_csv_and_manifest(tmp_path):
    from backfill_executor import execute_plan
    plan = fixture_plan()
    target = tmp_path / plan['files'][0]['target']
    target.parent.mkdir(parents=True)
    target.write_text('legacy-content')
    payload = zipped([(NAME[:-4] + '.csv', CSV + b'2,2,3,6,1793145600001,invalid\n')])
    plan['files'][0]['bytes'] = len(payload)
    fetch, _, _ = transport(payload)
    result = execute_plan(plan, tmp_path, fetch)
    assert result['outcome'] == 'failed' and result['written'] == 0
    assert not result['complete_days'] and target.read_text() == 'legacy-content'
    assert result['error_target'] == plan['files'][0]['target']
    assert result['error_source'] == KEY
    assert result['error_type'] == 'trades'
    assert result['missing'] == 0
    assert not target.with_suffix('.json').exists()
    assert not list(tmp_path.glob('.backfill-stage-*'))


def test_missing_archive_never_completes_day(tmp_path):
    from backfill_executor import execute_plan
    plan = make_plan(date(2026, 10, 28), date(2026, 10, 28), ['BTCUSDT'],
                     ['trades'], ['1m'], lambda _: None, today=date(2026, 11, 10))
    result = execute_plan(plan, tmp_path, lambda *_args, **_kwargs: pytest.fail('no GET'))
    assert result['outcome'] == 'incomplete' and result['missing'] == 1
    assert result['complete_days'] == []


def test_concurrent_run_fails_before_download(tmp_path):
    from backfill_executor import execute_plan
    with (tmp_path / '.backfill.lock').open('w') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        result = execute_plan(fixture_plan(), tmp_path, lambda *_a, **_k: pytest.fail('no GET'))
    assert result['outcome'] == 'failed' and result['written'] == 0


def test_actual_scheduler_argv_runs_real_parser_and_executor(tmp_path, monkeypatch, capsys):
    import backfill
    from scripts.cold_deploy import docker_command
    from test_cold_contract import CONFIG
    command = docker_command(CONFIG, date(2026, 10, 28), date(2026, 10, 28))
    argv = command[command.index('/app/src/backfill.py') + 1:]
    argv[argv.index('/data')] = str(tmp_path)
    monkeypatch.setattr(backfill, 'make_plan', lambda *_a, **_k: fixture_plan())
    fetch, _, _ = transport(zipped())
    assert backfill.main(argv, head=lambda _: len(zipped()), today=date(2026, 11, 10), fetch=fetch) == 0
    result = json.loads(capsys.readouterr().out)
    assert result['outcome'] == 'complete' and result['written'] == 1
    assert result['timezone'] == 'UTC' and result['finished_at'].endswith('+00:00')


def test_monthly_source_downloaded_once_for_two_atomic_daily_outputs(tmp_path):
    from backfill_executor import execute_plan
    from io import BytesIO
    import hashlib
    key = KEY.replace('daily/', 'monthly/').replace('2026-10-28', '2026-10')
    name = key.rsplit('/', 1)[-1]
    payload = zipped([(name[:-4] + '.csv', CSV + b'2,2,3,6,1793232000000,false\n')])
    plan = make_plan(date(2026, 10, 28), date(2026, 10, 29), ['BTCUSDT'], ['trades'],
                     ['1m'], lambda k: None if k.startswith('daily/') else len(payload),
                     today=date(2026, 11, 10))
    calls = []
    def fetch(url, timeout):
        calls.append(url)
        return BytesIO((hashlib.sha256(payload).hexdigest() + '  ' + name).encode()
                       if url.endswith('.CHECKSUM') else payload)
    result = execute_plan(plan, tmp_path, fetch)
    assert result['written'] == 2 and result['records'] == 2 and len(calls) == 2
    assert result['complete_days'] == ['2026-10-28', '2026-10-29']


@pytest.mark.parametrize('gap_status', ['missing', 'unavailable', 'partial'])
def test_monthly_kline_gap_does_not_stop_other_days(tmp_path, gap_status):
    from backfill import archive_key
    from backfill_cursor import classify_unavailable
    from backfill_executor import execute_plan

    key = archive_key('BTCUSDT', 'klines', '5m', '2026-10', monthly=True)
    rows = []
    for day in (27, 28, 29) if gap_status == 'partial' else (27, 29):
        start = int(datetime(2026, 10, day, tzinfo=timezone.utc).timestamp() * 1000)
        duration = 300000 if day == 28 else 86400000
        for instant in range(start, start + duration, 300000):
            rows.append(f'{instant},1,1,1,1,1,{instant + 299999},1,1,1,1,0\n')
    payload = zipped([(key.rsplit('/', 1)[-1][:-4] + '.csv', ''.join(rows).encode())])
    plan = make_plan(date(2026, 10, 27), date(2026, 10, 29), ['BTCUSDT'],
                     ['klines'], ['5m'], lambda candidate: (
                         len(payload) if candidate.removesuffix('.CHECKSUM') == key else None),
                     today=date(2026, 11, 10))
    assert {item['status'] for item in plan['files']} == {'pending'}
    if gap_status == 'unavailable':
        classify_unavailable(plan, [dict(symbol='BTCUSDT', type='klines', interval='5m',
                                        date='2026-10-28', source='https://data.binance.vision/',
                                        checked_at='2026-11-10')])
    checksum = f"{hashlib.sha256(payload).hexdigest()}  {key.rsplit('/', 1)[-1]}"
    fetch, calls, _ = transport(payload, checksum.encode())
    result = execute_plan(plan, tmp_path, fetch)

    if gap_status == 'partial':
        assert result['outcome'] == 'failed'
        assert result['error'] == 'Empty selection or incomplete kline day'
        assert result['written'] == 1 and result['missing'] == 0
        assert 'monthly_gaps' not in result
        return

    assert result['outcome'] == 'incomplete'
    assert result[gap_status] == 1 and result['written'] == 2
    assert result['records'] == 576 and len(calls) == 2
    assert result['complete_days'] == ['2026-10-27', '2026-10-29']
    assert result['settled_days'] == (
        ['2026-10-27', '2026-10-28', '2026-10-29'] if gap_status == 'unavailable'
        else result['complete_days'])
    assert result['monthly_gaps'] == [dict(target=plan['files'][1]['target'], source=key,
        status=gap_status, reason='No rows for the requested UTC day in the monthly archive')]
    before, gap, after = [tmp_path / item['target'] for item in plan['files']]
    assert before.exists() and after.exists()
    assert not gap.exists() and not gap.with_suffix('.json').exists()


@pytest.mark.parametrize('missing', [None, ('BTCUSDT', '2026-10'), ('BTCUSDT', '2026-09')])
def test_day_requires_selected_funding_of_same_month_for_every_symbol(tmp_path, missing):
    from backfill_executor import execute_plan
    from backfill_archives import BASE
    from datetime import datetime, timezone
    from io import BytesIO
    import hashlib
    archives = {}
    for symbol in ['BTCUSDT', 'ETHUSDT']:
        daily = KEY.replace('BTCUSDT', symbol)
        archives[daily] = zipped([(daily.rsplit('/', 1)[-1][:-4] + '.csv', CSV)])
        for month in [9, 10]:
            period = f'2026-{month:02d}'
            if missing == (symbol, period):
                continue
            key = f'monthly/fundingRate/{symbol}/{symbol}-fundingRate-{period}.zip'
            instant = int(datetime(2026, month, 28, tzinfo=timezone.utc).timestamp() * 1000)
            data = f'calc_time,funding_interval_hours,last_funding_rate\n{instant},8,0.001\n'.encode()
            archives[key] = zipped([(key.rsplit('/', 1)[-1][:-4] + '.csv', data)])
    def head(key):
        key = key.removesuffix('.CHECKSUM')
        return len(archives[key]) if key in archives else None
    def fetch(url, timeout):
        key = url.removeprefix(BASE)
        if key.endswith('.CHECKSUM'):
            key = key.removesuffix('.CHECKSUM')
            return BytesIO((hashlib.sha256(archives[key]).hexdigest() + '  ' + key.rsplit('/', 1)[-1]).encode())
        return BytesIO(archives[key])
    plan = make_plan(date(2026, 10, 28), date(2026, 10, 28), ['BTCUSDT', 'ETHUSDT'],
                     ['trades', 'fundingRate'], ['1m'], head,
                     funding_start=date(2026, 9, 1), today=date(2026, 11, 10))
    result = execute_plan(plan, tmp_path, fetch)
    assert result['outcome'] == ('complete' if missing is None else 'incomplete')
    assert result['complete_days'] == ([] if missing == ('BTCUSDT', '2026-10') else ['2026-10-28'])


@pytest.mark.parametrize('case', ['midnight', 'unordered', 'duplicate', 'outside',
                                  'symbol', 'numeric', 'fields'])
def test_metrics_publication_and_validation_summary(tmp_path, case):
    from backfill_executor import execute_plan
    from backfill_records import HEADERS

    times = ['00:40:00', '00:05:00', '00:10:00'] if case == 'unordered' else ['00:00:00']
    if case == 'duplicate':
        times = ['00:05:00', '00:10:00', '00:05:00']
    rows = [[f'2026-10-28 {instant}', 'BTCUSDT', '1', '2', '3', '4', '5', '6']
            for instant in times]
    if case == 'outside': rows[0][0] = '2026-10-29 00:00:00'
    if case == 'symbol': rows[0][1] = 'ETHUSDT'
    if case == 'numeric': rows[0][2] = 'NaN'
    if case == 'fields': rows[0].pop()
    key = 'daily/metrics/BTCUSDT/BTCUSDT-metrics-2026-10-28.zip'
    name = key.rsplit('/', 1)[-1]
    data = '\n'.join(','.join(row) for row in [HEADERS['metrics'], *rows]) + '\n'
    payload = zipped([(name[:-4] + '.csv', data.encode())])
    plan = make_plan(date(2026, 10, 28), date(2026, 10, 28), ['BTCUSDT'],
                     ['metrics'], ['1m'], lambda _: len(payload), today=date(2026, 11, 10))
    checksum = f'{hashlib.sha256(payload).hexdigest()}  {name}'.encode()
    fetch, calls, _ = transport(payload, checksum)
    result = execute_plan(plan, tmp_path, fetch)
    target = tmp_path / plan['files'][0]['target']

    if case in ('midnight', 'unordered'):
        assert result['outcome'] == 'complete', result
        with target.open() as stream:
            published = list(csv.reader(stream))
        assert published == [list(HEADERS['metrics']), *sorted(rows)]
        assert result['records'] == len(rows) and result['written'] == 1
        manifest = json.loads(target.with_suffix('.json').read_text())
        assert manifest['csv_sha256'] == hashlib.sha256(target.read_bytes()).hexdigest()
    else:
        assert result['outcome'] == 'failed'
        assert result['error_target'] == plan['files'][0]['target']
        assert result['error_source'] == key and result['error_type'] == 'metrics'
        assert result['written'] == 0 and result['missing'] == 0
        assert not target.exists() and not target.with_suffix('.json').exists()
    assert len(calls) == 2
