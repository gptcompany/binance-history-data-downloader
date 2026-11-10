"""Real offline executor: archive integrity, atomic publication and restart safety."""
import csv
from datetime import date
import fcntl
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
