"""Offline contract: host rendering is pure and the legacy entrypoint is bypassed."""
from configparser import ConfigParser
from datetime import date
from pathlib import Path

import pytest

RELEASE = Path('/opt/binance-cold/' + 'a' * 40)
CONFIG = dict(image='ghcr.io/gptcompany/binance-downloader@sha256:' + 'b' * 64,
              image_source_sha='c' * 40, user='sam', uid=1000, gid=1000,
              cold_root='/data/cold/12TB/binance-history-data-downloader/data')


def parse_unit(text):
    parser = ConfigParser(interpolation=None)
    parser.read_string(text)
    return parser


def test_existing_image_still_requires_explicit_entrypoint_override():
    dockerfile = (Path(__file__).parents[1] / 'Dockerfile').read_text()
    assert 'CMD ["bash", "scripts/run-sync.sh"]' in dockerfile


def test_only_one_download_timer_and_independent_drift_timer(tmp_path):
    from scripts.cold_deploy import render_units
    units = render_units(CONFIG, RELEASE)
    assert set(units) == {name + suffix for name in ('binance-cold', 'binance-cold-drift')
                          for suffix in ('.service', '.timer')}
    service = parse_unit(units['binance-cold.service'])
    assert service['Unit']['RequiresMountsFor'] == CONFIG['cold_root']
    assert service['Unit']['Requires'] == 'docker.service'
    assert service['Service']['User'] == 'sam'
    assert service['Service']['Environment'] == 'TZ=UTC'
    assert "'run'" in service['Service']['ExecStart']
    assert service['Service']['StandardOutput'] == 'journal'
    assert service['Service']['StandardError'] == 'journal'
    for name, hour in [('binance-cold', '06:30'), ('binance-cold-drift', '03:00')]:
        timer = parse_unit(units[name + '.timer'])['Timer']
        assert timer['OnCalendar'] == f'*-*-* {hour}:00 UTC'
        assert timer['Persistent'] == 'true'
        assert timer['Unit'] == name + '.service'
    assert "'drift'" in parse_unit(units['binance-cold-drift.service'])['Service']['ExecStart']
    assert render_units(CONFIG, RELEASE) == units
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize('patch', [
    dict(image='ghcr.io/gptcompany/binance-downloader:latest'), dict(image_source_sha='main'),
    dict(uid=True), dict(uid=-1), dict(gid='1000'), dict(user='sam\nExecStart=bad'),
    dict(cold_root='relative'), dict(cold_root='/data/../runtime'),
    dict(cold_root='/data//cold'), dict(extra='host-only'),
])
def test_closed_inventory_rejects_unpinned_or_ambiguous_values(patch):
    from scripts.cold_deploy import render_units
    from jsonschema import ValidationError
    with pytest.raises((ValueError, ValidationError)):
        render_units(dict(CONFIG, **patch), RELEASE)


@pytest.mark.parametrize('release', ['relative', '/opt/../runtime', '/opt/a b', '/opt/a\n'])
def test_release_cannot_inject_unit_settings(release):
    from scripts.cold_deploy import render_units
    with pytest.raises(ValueError):
        render_units(CONFIG, release)


@pytest.mark.parametrize('uid', [1000, 1000.0])
def test_container_argv_is_pinned_cold_only_and_overrides_legacy_entrypoint(uid):
    from scripts.cold_deploy import docker_command
    command = docker_command(dict(CONFIG, uid=uid, gid=uid), date(2026, 6, 29), date(2026, 11, 9))
    assert command[:4] == ['/usr/bin/docker', 'run', '--rm', '--pull=never']
    assert command[command.index('--user') + 1] == '1000:1000'
    assert command[command.index('--mount') + 1] == f"type=bind,src={CONFIG['cold_root']},dst=/data"
    assert command[command.index('--entrypoint') + 1] == 'python'
    assert command[command.index('--entrypoint') + 2] == CONFIG['image']
    assert '/app/src/backfill.py' in command and 'TZ=UTC' in command
    assert command[-6:] == ['--start-date', '2026-06-29', '--end-date', '2026-11-09', '--data-root', '/data']
    assert 'bash' not in command and 'compose' not in command and '--dry-run' not in command
    assert docker_command(CONFIG, date(2026, 6, 29), date(2026, 11, 9), dry_run=True)[-1] == '--dry-run'
