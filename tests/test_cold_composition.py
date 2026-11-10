"""Real approved subprocess imports, drift and runner with only host boundaries faked."""
import json
from pathlib import Path
import shutil

from test_cold_contract import CONFIG
from test_cold_snapshot import git


def test_clean_approved_bootstrap_reaches_container(tmp_path):
    from scripts.cold_deploy import render_units
    from scripts.cold_host import install
    from scripts.cold_snapshot import run_approved
    repo = tmp_path / 'repository'
    (repo / 'scripts').mkdir(parents=True)
    source = Path(__file__).parents[1] / 'scripts'
    for name in ('cold_deploy.py', 'cold_snapshot.py', 'cold_runner.py', 'cold_host.py'):
        shutil.copyfile(source / name, repo / 'scripts' / name)
    (repo / 'deploy').mkdir()
    (repo / 'deploy/cold.json').write_text(json.dumps(CONFIG))
    script = repo / 'scripts/cold_deploy.py'
    text = script.read_text().split("if __name__ == '__main__':")[0]
    script.write_text(text + '''
if __name__ == '__main__':
    import json, sys
    from pathlib import Path
    from types import SimpleNamespace
    from scripts import cold_runner, cold_host
    units = Path(sys.argv[sys.argv.index('--unit-directory') + 1])
    def query(name):
        return dict(FragmentPath=str(units / name), DropInPaths='', NeedDaemonReload='no',
                    ActiveState='inactive', UnitFileState='masked')
    cold_runner.drift = lambda *args: cold_host.drift(*args, query=query)
    cold_runner.subprocess.run = lambda *args, **kwargs: SimpleNamespace(returncode=0,
        stdout=json.dumps(dict(timezone='UTC', outcome='complete', written=2,
                               skipped=3, records=5, missing=0)).encode(), stderr=b'')
    raise SystemExit(cold_runner.main())
''')
    git(repo, 'init', '--quiet')
    git(repo, 'config', 'user.name', 'CI fixture')
    git(repo, 'config', 'user.email', 'fixture@example.invalid')
    git(repo, 'add', '.')
    git(repo, 'commit', '--quiet', '-m', 'approved composition fixture')
    sha = git(repo, 'rev-parse', 'HEAD')
    release = tmp_path / sha
    shutil.copytree(repo, release)
    units = tmp_path / 'units'
    install(CONFIG, release, units)
    result = run_approved(release, sha, ['run', str(release), '--unit-directory', str(units)])
    record = json.loads(result.stdout)
    assert result.returncode == 0, record
    assert record['written'] == 2 and record['records'] == 5
    assert set(render_units(CONFIG, release)) <= {p.name for p in units.iterdir()}
