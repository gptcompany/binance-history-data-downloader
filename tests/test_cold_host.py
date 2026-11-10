"""Offline host reconciliation: no real systemd, Docker or cold data."""
from pathlib import Path

import pytest

from test_cold_contract import CONFIG, parse_unit

LEGACY = [name + suffix for name in ('binance-sync', 'binance-sync-docker')
          for suffix in ('.service', '.timer')]


@pytest.fixture
def host(tmp_path):
    release = tmp_path / ('a' * 40)
    release.mkdir()
    source = tmp_path / 'approved'
    source.mkdir()
    (source / 'scripts').mkdir()
    (source / 'scripts/cold_deploy.py').write_text('approved code')
    (release / 'scripts').mkdir()
    (release / 'scripts/cold_deploy.py').write_text('approved code')
    units = tmp_path / 'units'
    units.mkdir()
    return release, source, units


def state(units):
    def query(name):
        if name in LEGACY:
            return dict(FragmentPath='/dev/null', DropInPaths='', NeedDaemonReload='no',
                        ActiveState='inactive', UnitFileState='masked')
        return dict(FragmentPath=str(units / name), DropInPaths='', NeedDaemonReload='no',
                    ActiveState='inactive', UnitFileState='static')
    return query


def test_install_only_writes_exact_units_and_legacy_masks(host, monkeypatch):
    from scripts.cold_host import install, drift
    from scripts.cold_deploy import render_units
    release, source, units = host
    monkeypatch.setattr('subprocess.run', lambda *a, **k: pytest.fail('installer invoked a process'))
    install(CONFIG, release, units)
    install(CONFIG, release, units)
    for name, text in render_units(CONFIG, release).items():
        assert (units / name).read_text() == text
        assert (units / name).stat().st_mode & 0o777 == 0o644
    for name in LEGACY:
        assert (units / name).is_symlink()
        assert (units / name).readlink() == Path('/dev/null')
    assert drift(CONFIG, release, source, units, state(units)) == []


@pytest.mark.parametrize('change', ['code', 'unit', 'dropin', 'reload', 'legacy', 'active'])
def test_drift_detects_bytes_loaded_manager_and_legacy_authorities(host, change):
    from scripts.cold_host import install, drift
    release, source, units = host
    install(CONFIG, release, units)
    query = state(units)
    def altered(name):
        result = query(name)
        if name == 'binance-cold.service' and change in ('dropin', 'reload'):
            result['DropInPaths' if change == 'dropin' else 'NeedDaemonReload'] = (
                '/etc/systemd/system/binance-cold.service.d/local.conf' if change == 'dropin' else 'yes')
        if name == LEGACY[0] and change == 'active':
            result['ActiveState'] = 'active'
        return result
    if change == 'code':
        (release / 'scripts/cold_deploy.py').write_text('tampered')
    if change == 'unit':
        (units / 'binance-cold.service').write_text('tampered')
    if change == 'legacy':
        (units / LEGACY[0]).unlink()
    assert drift(CONFIG, release, source, units, altered)


def test_service_bootstraps_committed_snapshot_without_executing_worktree():
    from scripts.cold_deploy import render_units
    release = Path('/opt/binance-cold/' + 'a' * 40)
    command = parse_unit(render_units(CONFIG, release)['binance-cold.service'])['Service']['ExecStart']
    assert ' -I -c ' in command
    assert "'show', '" + 'a' * 40 + ":scripts/cold_snapshot.py'" in command
    assert 'exec(subprocess.check_output' in command
    assert f'{release}/scripts/cold_deploy.py' not in command


def test_bootstrap_disables_git_replacement_objects():
    from scripts.cold_deploy import render_units
    release = Path('/opt/binance-cold/' + 'a' * 40)
    for name, text in render_units(CONFIG, release).items():
        if name.endswith('.service'):
            command = parse_unit(text)['Service']['ExecStart']
            assert "'git', '--no-replace-objects', '-C'" in command
