"""Approved Git objects, never the mutable checkout, supply deploy code."""
import json
from pathlib import Path
import subprocess

import pytest


def git(repo, *args):
    return subprocess.check_output(['git', '-C', str(repo), *args], text=True).strip()


@pytest.fixture
def approved(tmp_path):
    repo = tmp_path / 'repository'
    repo.mkdir()
    git(repo, 'init', '--quiet')
    git(repo, 'config', 'user.name', 'CI fixture')
    git(repo, 'config', 'user.email', 'fixture@example.invalid')
    script = repo / 'scripts/cold_deploy.py'
    script.parent.mkdir()
    script.write_text('import json, sys\nprint(json.dumps({"source": "approved", "args": sys.argv[1:]}))\n')
    git(repo, 'add', '.')
    git(repo, 'commit', '--quiet', '-m', 'approved fixture')
    return repo, git(repo, 'rev-parse', 'HEAD'), script


@pytest.mark.parametrize('flag', [None, '--assume-unchanged', '--skip-worktree'])
def test_executes_approved_archive_despite_hidden_worktree_changes(approved, flag):
    from scripts.cold_snapshot import run_approved
    repo, sha, script = approved
    if flag:
        git(repo, 'update-index', flag, 'scripts/cold_deploy.py')
    script.write_text('raise RuntimeError("mutable checkout was executed")\n')
    result = run_approved(repo, sha, ['drift', '/opt/binance-cold/' + sha])
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == dict(source='approved', args=['drift', '/opt/binance-cold/' + sha])
    assert script.read_text().startswith('raise RuntimeError')


def test_snapshot_contains_only_committed_bytes_and_is_removed(approved):
    from scripts.cold_snapshot import approved_snapshot
    repo, sha, script = approved
    (repo / 'untracked.py').write_text('raise RuntimeError("untracked")')
    with approved_snapshot(repo, sha) as root:
        assert root != repo and (root / 'scripts/cold_deploy.py').read_bytes() == script.read_bytes()
        assert not (root / 'untracked.py').exists()
        snapshot = root
    assert not snapshot.exists()


@pytest.mark.parametrize('sha', ['main', 'HEAD', 'a' * 39, 'a' * 40 + '\n', 'f' * 40])
def test_no_branch_or_missing_object_fallback_to_worktree(approved, sha):
    from scripts.cold_snapshot import approved_snapshot
    repo, _, _ = approved
    with pytest.raises((ValueError, subprocess.CalledProcessError)):
        with approved_snapshot(repo, sha):
            pytest.fail('unapproved snapshot accepted')


def test_snapshot_cleanup_on_failed_action(approved):
    from scripts.cold_snapshot import approved_snapshot
    repo, sha, _ = approved
    with pytest.raises(RuntimeError):
        with approved_snapshot(repo, sha) as root:
            raise RuntimeError('installer failed')
    assert not root.exists()


def test_pythonpath_cannot_shadow_approved_code(approved, tmp_path, monkeypatch):
    from scripts.cold_snapshot import run_approved
    repo, sha, _ = approved
    shadow = tmp_path / 'shadow'
    shadow.mkdir()
    (shadow / 'json.py').write_text('raise RuntimeError("environment injection")')
    monkeypatch.setenv('PYTHONPATH', str(shadow))
    assert json.loads(run_approved(repo, sha, ['render']).stdout)['source'] == 'approved'


def test_replace_ref_cannot_substitute_approved_commit(approved):
    from scripts.cold_snapshot import run_approved
    repo, sha, script = approved
    script.write_text('import json\nprint(json.dumps({"source": "replacement"}))\n')
    git(repo, 'add', '.')
    git(repo, 'commit', '--quiet', '-m', 'replacement fixture')
    replacement = git(repo, 'rev-parse', 'HEAD')
    git(repo, 'replace', sha, replacement)
    assert json.loads(run_approved(repo, sha, ['drift']).stdout)['source'] == 'approved'


def test_bootstrap_failure_still_emits_one_final_record(approved, capsys):
    from scripts.cold_snapshot import main
    repo, _, _ = approved
    code = main([str(repo), 'f' * 40, 'drift'])
    lines = capsys.readouterr().out.splitlines()
    assert code == 1 and len(lines) == 1
    record = json.loads(lines[0])
    assert record['outcome'] == 'failed' and record['phase'] == 'bootstrap'
    assert record['written'] == 0 and record['counts_status'] == 'not_started'
