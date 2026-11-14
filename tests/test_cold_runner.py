"""Actual host CLI on temporary inventory and fake Docker/systemd boundaries."""
from datetime import date
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from test_cold_contract import CONFIG


@pytest.fixture
def host(tmp_path):
    approved = tmp_path / 'approved'
    (approved / 'deploy').mkdir(parents=True)
    (approved / 'deploy/cold.json').write_text(json.dumps(CONFIG))
    release = tmp_path / ('a' * 40)
    release.mkdir()
    return approved, release, tmp_path / 'units'


def invoke(host, action, capsys, monkeypatch, *, findings=(), result=None, extra=()):
    from scripts import cold_runner
    approved, release, units = host
    calls = []
    monkeypatch.setattr(cold_runner, 'drift', lambda *args: list(findings))
    def execute(command, **kwargs):
        calls.append(command)
        if callable(result):
            return result(command, kwargs)
        return result or SimpleNamespace(stderr=b'', returncode=0, stdout=json.dumps(dict(
            timezone='UTC', outcome='complete', written=2, skipped=3, records=5,
            missing=0, unavailable=0, complete_days=['2026-11-09'],
            settled_days=['2026-11-09'], finished_at='2026-11-10T03:30:00+00:00')))
    monkeypatch.setattr(cold_runner.subprocess, 'run', execute)
    code = cold_runner.main([action, str(release), '--unit-directory', str(units), *extra],
                            approved=approved)
    lines = capsys.readouterr().out.splitlines()
    assert len(lines) == 1
    return code, json.loads(lines[0]), calls


def test_installer_cli_writes_only_units_and_masks(host, capsys, monkeypatch):
    code, record, calls = invoke(host, 'install', capsys, monkeypatch)
    assert code == 0 and record['outcome'] == 'complete' and calls == []
    assert (host[2] / 'binance-cold.timer').is_file()
    assert (host[2] / 'binance-sync.timer').readlink() == Path('/dev/null')


@pytest.mark.parametrize('action', ['run', 'backfill', 'drift'])
def test_drift_failure_blocks_docker_and_logs_counts(host, capsys, monkeypatch, action):
    extra = ('--start-date', '2026-06-29', '--end-date', '2026-11-09') if action == 'backfill' else ()
    code, record, calls = invoke(host, action, capsys, monkeypatch,
                                 findings=['unit bytes: bad'], extra=extra)
    assert code == 1 and record['outcome'] == 'failed' and calls == []
    assert record['written'] == 0 and record['timezone'] == 'UTC'
    assert record['findings'] == ['unit bytes: bad']


def test_daily_runner_uses_pinned_cursor_cli(host, capsys, monkeypatch):
    code, record, calls = invoke(host, 'run', capsys, monkeypatch)
    assert code == 0 and record['records'] == 5
    assert '/app/src/backfill_cursor.py' in calls[0]
    assert calls[0][-4:] == ['--initial-date', '2026-06-29', '--data-root', '/data']
    assert CONFIG['image'] in calls[0] and '--pull=never' in calls[0]


def test_manual_backfill_dry_run_keeps_dates_and_funding(host, capsys, monkeypatch):
    from backfill import make_plan
    plan = make_plan(date(2026, 6, 29), date(2026, 11, 9), ('BTCUSDT',),
                     ('bookDepth',), ('1m',), head=lambda key: None,
                     today=date(2026, 11, 10))
    code, record, calls = invoke(host, 'backfill', capsys, monkeypatch, extra=(
        '--start-date', '2026-06-29', '--end-date', '2026-11-09', '--dry-run'),
        result=SimpleNamespace(stderr=b'', returncode=0, stdout=json.dumps(plan)))
    assert code == 0 and record['plan'] == plan and calls[0][-1] == '--dry-run'
    assert '--funding-start' in calls[0] and '2026-05-01' in calls[0]


@pytest.mark.parametrize('result', [SimpleNamespace(stderr=b'', returncode=125, stdout=''),
    SimpleNamespace(stderr=b'', returncode=0, stdout='{}'),
    SimpleNamespace(stderr=b'', returncode=1, stdout=json.dumps(dict(timezone='UTC', outcome='complete',
        written=1, skipped=0, records=1, missing=0)))])
def test_invalid_child_result_is_visible_failure(host, capsys, monkeypatch, result):
    code, record, calls = invoke(host, 'run', capsys, monkeypatch, result=result)
    assert code == 1 and record['outcome'] == 'failed' and len(calls) == 1


def test_install_dry_run_never_writes_units(host, capsys, monkeypatch):
    code, record, calls = invoke(host, 'install', capsys, monkeypatch, extra=('--dry-run',))
    assert code == 1 and record['phase'] == 'preflight' and calls == []
    assert not host[2].exists()


def test_binary_stderr_preserves_real_child_counts(host, capsys, monkeypatch):
    import io
    import subprocess
    import sys
    real_run = subprocess.run
    stderr = io.BytesIO()
    monkeypatch.setattr(sys, 'stderr', SimpleNamespace(buffer=stderr))
    payload = dict(timezone='UTC', outcome='complete', written=2, skipped=3, records=5, missing=0)
    script = 'import os;os.write(1,' + repr(json.dumps(payload).encode()) + ');os.write(2,b"\\xff")'
    code, record, _ = invoke(host, 'run', capsys, monkeypatch,
        result=lambda command, kwargs: real_run([sys.executable, '-c', script], **kwargs))
    assert code == 0 and record['written'] == 2 and record['records'] == 5
    assert record['phase'] == 'container' and record['counts_status'] == 'reported'
    assert stderr.getvalue() == b'\xff'


@pytest.mark.parametrize('returncode,stdout,status', [(0, b'{}', 'unknown'),
    (125, b'', 'not_started'), (1, json.dumps(dict(timezone='UTC', outcome='complete',
        written=2, skipped=3, records=5, missing=0)).encode(), 'unknown')])
def test_untrusted_child_never_invents_zero_progress(host, capsys, monkeypatch,
                                                     returncode, stdout, status):
    result = SimpleNamespace(returncode=returncode, stdout=stdout, stderr=b'')
    code, record, _ = invoke(host, 'run', capsys, monkeypatch, result=result)
    assert code == 1 and record['phase'] == 'container'
    assert record['counts_status'] == status
    assert all(record[key] is None for key in ('written', 'skipped', 'records'))


def test_missing_docker_has_explicit_not_started_counts(host, capsys, monkeypatch):
    def absent(command, kwargs):
        raise FileNotFoundError('fixture missing Docker')
    code, record, _ = invoke(host, 'run', capsys, monkeypatch, result=absent)
    assert code == 1 and record['phase'] == 'container'
    assert record['counts_status'] == 'not_started' and record['written'] is None


@pytest.mark.parametrize('invalid', [False, True])
def test_real_container_summary_passes_host_parser(host, tmp_path, capsys, monkeypatch, invalid):
    from backfill import main
    from test_backfill_archives import CSV, NAME, transport, zipped

    data = CSV.replace(b'true', b'invalid') if invalid else CSV
    payload = zipped([(NAME[:-4] + '.csv', data)])
    fetch, _, _ = transport(payload)
    status = main(['--start-date', '2026-10-28', '--end-date', '2026-10-28',
                   '--symbols', 'BTCUSDT', '--data-types', 'trades',
                   '--data-root', str(tmp_path / 'data')],
                  head=lambda _: len(payload), today=date(2026, 11, 10), fetch=fetch)
    stdout = capsys.readouterr().out
    summary = json.loads(stdout)
    assert status == int(invalid)
    assert summary['outcome'] == ('failed' if invalid else 'complete')
    code, record, _ = invoke(host, 'backfill', capsys, monkeypatch,
        extra=('--start-date', '2026-10-28', '--end-date', '2026-10-28'),
        result=SimpleNamespace(returncode=status, stdout=stdout.encode(), stderr=b''))
    assert code == status
    assert record['counts_status'] == 'reported', record
    assert all(record[key] == value for key, value in summary.items() if key != 'finished_at')
    assert record['records'] == (0 if invalid else 1)
    if invalid:
        assert record['error_source'].endswith(NAME)
        assert record['error_type'] == 'trades'


@pytest.mark.parametrize('field', ['error_target', 'error_source', 'error_type'])
@pytest.mark.parametrize('value', [None, 'fixture', 1, False, [], {}])
def test_error_context_schema_is_nullable_string(field, value):
    from jsonschema import Draft202012Validator
    from scripts.cold_runner import RESULT

    validator = Draft202012Validator(RESULT['properties'][field])
    assert validator.is_valid(value) == (value is None or isinstance(value, str))


def test_daily_poll_downloads_new_sources_without_replaying_unchanged_plan(monkeypatch):
    from datetime import datetime, timedelta, timezone
    from scripts import cold_runner
    clock = [datetime(2026, 11, 13, 6, 30, tzinfo=timezone.utc)]
    sleeps, commands = [], []
    def sleep(seconds):
        sleeps.append(seconds)
        clock[0] += timedelta(seconds=seconds)
    def payload(outcome, missing):
        return dict(timezone='UTC', outcome=outcome, written=0, skipped=0, records=0,
                    missing=missing, unavailable=0, cursor='2026-11-12')
    def plan(available):
        return dict(timezone='UTC', start='2026-11-12', end='2026-11-12', days=1,
            archives=len(available), archive_bytes=len(available), missing=2-len(available),
            files=[dict(symbol='ETHUSDT', type=kind, interval=None, date='2026-11-12',
                target=kind+'.csv', source=kind if kind in available else None,
                bytes=1 if kind in available else None,
                status='available' if kind in available else 'unpublished')
                for kind in ('metrics', 'bookDepth')])
    responses = iter([payload('incomplete', 2), plan(()), plan(('metrics',)),
        payload('incomplete', 1), plan(('metrics',)), plan(('metrics', 'bookDepth')),
        payload('complete', 0)])
    def execute(command, **kwargs):
        commands.append(command)
        value = next(responses)
        return SimpleNamespace(stdout=json.dumps(value), stderr=b'',
            returncode=0 if 'outcome' not in value or value['outcome']=='complete' else 1)
    monkeypatch.setattr(cold_runner.subprocess, 'run', execute)
    result = cold_runner.run_daily(['docker', '/app/src/backfill_cursor.py'],
                                  now=lambda: clock[0], sleep=sleep)
    assert result.returncode == 0
    assert sleeps == [900, 900, 900]
    assert sum('/app/src/backfill_cursor.py' in cmd for cmd in commands) == 3
    probes = [cmd for cmd in commands if '--dry-run' in cmd]
    assert len(probes) == 4
    assert all(cmd[cmd.index('--end-date')+1] == '2026-11-12' for cmd in probes)


def test_daily_poll_deadline_lists_gaps_and_remains_nonzero(monkeypatch):
    from datetime import datetime, timedelta, timezone
    from scripts import cold_runner
    clock = [datetime(2026, 11, 13, 13, 50, tzinfo=timezone.utc)]
    def sleep(seconds):
        assert seconds == 600
        clock[0] += timedelta(seconds=seconds)
    summary = dict(timezone='UTC', outcome='incomplete', written=0, skipped=0,
                   records=0, missing=1, unavailable=0, cursor='2026-11-12')
    plan = dict(timezone='UTC', start='2026-11-12', end='2026-11-12', days=1,
        archives=0, archive_bytes=0, missing=1, files=[dict(symbol='ETHUSDT',
        type='metrics', interval=None, date='2026-11-12', source=None, bytes=None,
        target='ETHUSDT/metrics/ETHUSDT-metrics-2026-11-12.csv', status='unpublished')])
    replies = iter([summary, plan])
    def execute(command, **kwargs):
        value = next(replies)
        return SimpleNamespace(stdout=json.dumps(value), stderr=b'',
                               returncode=int('outcome' in value))
    monkeypatch.setattr(cold_runner.subprocess, 'run', execute)
    result = cold_runner.run_daily(['docker', '/app/src/backfill_cursor.py'],
                                  now=lambda: clock[0], sleep=sleep)
    assert result.returncode == 1
    record = json.loads(result.stdout)
    assert record['unresolved_targets'] == [plan['files'][0]['target']]
    assert '14:00' in record['error']
    assert clock[0].hour == 14


def test_deadline_result_survives_host_schema_and_exit_boundary(host, capsys, monkeypatch):
    from scripts import cold_runner
    summary = dict(timezone='UTC', outcome='incomplete', written=1, skipped=2,
        records=3, missing=1, unavailable=2, unresolved_targets=['ETHUSDT/metrics/day.csv'],
        error='Publication deadline 14:00 UTC exceeded')
    monkeypatch.setattr(cold_runner, 'run_daily', lambda command: SimpleNamespace(
        returncode=1, stdout=json.dumps(summary), stderr=b''))
    code, record, _ = invoke(host, 'run', capsys, monkeypatch)
    assert code == 1 and record['counts_status'] == 'reported'
    assert record['unresolved_targets'] == summary['unresolved_targets']
    assert record['records'] == 3 and record['unavailable'] == 2


@pytest.mark.parametrize('hour', [14, 15])
@pytest.mark.parametrize('outcome', ['complete', 'incomplete'])
def test_late_start_attempts_d_minus_one_once_without_polling(monkeypatch, hour, outcome):
    from datetime import datetime, timezone
    from scripts import cold_runner
    commands = []
    summary = dict(timezone='UTC', outcome=outcome, written=20, skipped=12,
                   records=51000, missing=0, unavailable=2 if outcome == 'incomplete' else 0,
                   cursor='2026-11-14')
    def execute(command, **kwargs):
        commands.append(command)
        assert '/app/src/backfill_cursor.py' in command
        return SimpleNamespace(stdout=json.dumps(summary), stderr=b'',
                               returncode=int(outcome != 'complete'))
    monkeypatch.setattr(cold_runner.subprocess, 'run', execute)
    def no_sleep(seconds):
        pytest.fail('Late invocation must not wait for publication')
    result = cold_runner.run_daily(['docker', '/app/src/backfill_cursor.py'],
        now=lambda: datetime(2026, 11, 14, hour, tzinfo=timezone.utc), sleep=no_sleep)
    assert len(commands) == 1
    assert result.returncode == int(outcome != 'complete')
    record = json.loads(result.stdout)
    assert record == dict(summary, attempt_totals=dict(written=20, skipped=12, records=51000))


@pytest.mark.parametrize('outcome', ['complete', 'incomplete', 'failed'])
def test_poll_replay_preserves_counts_when_cursor_advances(monkeypatch, outcome):
    from datetime import datetime, timedelta, timezone
    from scripts import cold_runner
    clock = [datetime(2026, 11, 14, 13, 50, tzinfo=timezone.utc)]
    first = dict(timezone='UTC', outcome='incomplete', written=20, skipped=12,
                 records=51000, missing=0, unavailable=2, cursor='2026-11-14')
    last = dict(first, outcome=outcome, written=0, records=1000,
                unavailable=0 if outcome == 'complete' else 2)
    plan = dict(timezone='UTC', start='2026-11-13', end='2026-11-13', days=1,
                archives=1, archive_bytes=1, missing=0, files=[dict(symbol='BTCUSDT',
                    type='fundingRate', interval=None, date='2026-11-01', source='funding.zip',
                    bytes=1, target='funding.csv', status='available')])
    responses = iter([first, plan, last])
    def execute(command, **kwargs):
        value = next(responses)
        return SimpleNamespace(stdout=json.dumps(value), stderr=b'',
            returncode=int(value.get('outcome', 'complete') != 'complete'))
    def sleep(seconds):
        clock[0] += timedelta(seconds=seconds)
    monkeypatch.setattr(cold_runner.subprocess, 'run', execute)
    result = cold_runner.run_daily(['docker', '/app/src/backfill_cursor.py'],
                                  now=lambda: clock[0], sleep=sleep)
    summary = json.loads(result.stdout)
    assert summary['written'] == 20
    assert summary['skipped'] == 12 and summary['records'] == 1000
    assert summary['attempt_totals'] == dict(written=20, skipped=24, records=52000)
    assert summary['outcome'] == outcome
    assert summary['unavailable'] == last['unavailable']
    assert summary['missing'] == 0 and summary['cursor'] == '2026-11-14'
    assert result.returncode == int(outcome != 'complete')
    cold_runner.Draft202012Validator(cold_runner.RESULT).validate(summary)
