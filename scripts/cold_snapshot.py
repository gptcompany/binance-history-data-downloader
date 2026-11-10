"""Bootstrap cold actions from approved Git objects, independent of index flags."""
import argparse
from contextlib import contextmanager
import io
from pathlib import Path
import re
import subprocess
import sys
import tarfile
from tempfile import TemporaryDirectory


def git_command(repository, *arguments):
    """Read literal approved objects, ignoring mutable replacement references."""
    return ['git', '--no-replace-objects', '-C', str(repository), *arguments]


@contextmanager
def approved_snapshot(repository, sha):
    if not re.fullmatch('[0-9a-f]{40}', sha):
        raise ValueError('Require an approved full commit SHA')
    command = git_command(repository)
    resolved = subprocess.check_output(command + ['rev-parse', '--verify', sha + '^{commit}'], text=True).strip()
    if resolved != sha:
        raise ValueError('Approved object must be a commit')
    archive = subprocess.check_output(command + ['archive', '--format=tar', sha])
    with TemporaryDirectory(prefix='binance-approved-') as temporary:
        root = Path(temporary)
        with tarfile.open(fileobj=io.BytesIO(archive), mode='r:') as source:
            source.extractall(root, filter='data')
        yield root


def run_approved(repository, sha, arguments):
    with approved_snapshot(repository, sha) as root:
        # -I ignores PYTHONPATH, user site and the caller's current directory.
        bootstrap = ("import runpy,sys; root=sys.argv.pop(1); sys.path.insert(0,root); "
                     "runpy.run_path(root+'/scripts/cold_deploy.py',run_name='__main__')")
        return subprocess.run([sys.executable, '-I', '-c', bootstrap, str(root), *arguments],
                              cwd=root, capture_output=True, text=True, check=False)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('repository', type=Path)
    parser.add_argument('sha')
    parser.add_argument('arguments', nargs='+')
    args = parser.parse_args(argv)
    result = run_approved(args.repository, args.sha, args.arguments)
    sys.stdout.write(result.stdout)
    sys.stderr.write(result.stderr)
    return result.returncode


if __name__ == '__main__':
    raise SystemExit(main())
