"""Install owner units without activation; compare host state with approved bytes."""
from configparser import ConfigParser
import os
from pathlib import Path
import subprocess
from tempfile import NamedTemporaryFile

from scripts.cold_deploy import render_units

LEGACY = [name + suffix for name in ('binance-sync', 'binance-sync-docker')
          for suffix in ('.service', '.timer')]
PROPERTIES = {'FragmentPath', 'DropInPaths', 'NeedDaemonReload', 'ActiveState', 'UnitFileState'}


def manager_state(name):
    result = subprocess.run(['/usr/bin/systemctl', 'show', name,
                             '--property=' + ','.join(sorted(PROPERTIES))],
                            capture_output=True, text=True, check=True)
    parser = ConfigParser(interpolation=None)
    parser.optionxform = str
    parser.read_string('[Unit]\n' + result.stdout)
    state = dict(parser['Unit'])
    if set(state) != PROPERTIES:
        raise ValueError('Incomplete systemd state: ' + name)
    return state


def install(config, release, unit_directory):
    directory = Path(unit_directory)
    rendered = render_units(config, release)
    directory.mkdir(parents=True, exist_ok=True)
    for name, text in rendered.items():
        with NamedTemporaryFile(dir=directory, delete=False) as temporary:
            path = Path(temporary.name)
            try:
                temporary.write(text.encode())
                temporary.flush()
                os.fchmod(temporary.fileno(), 0o644)
                os.fsync(temporary.fileno())
                os.replace(path, directory / name)
            finally:
                path.unlink(missing_ok=True)
    for name in LEGACY:
        target = directory / name
        if target.is_symlink() and target.readlink() == Path('/dev/null'):
            continue
        target.unlink(missing_ok=True)
        target.symlink_to('/dev/null')
    # No daemon-reload, enable, start or stop: B5 reconciles runtime after merge.


def drift(config, release, approved, unit_directory, query=manager_state):
    release, approved, directory = Path(release), Path(approved), Path(unit_directory)
    findings = []
    for source in approved.rglob('*'):
        if source.is_file():
            target = release / source.relative_to(approved)
            if target.is_symlink() or not target.is_file() or target.read_bytes() != source.read_bytes():
                findings.append('source bytes: ' + str(source.relative_to(approved)))
    for name, text in render_units(config, release).items():
        target = directory / name
        if target.is_symlink() or not target.is_file() or target.read_bytes() != text.encode():
            findings.append('unit bytes: ' + name)
        state = query(name)
        if (state['FragmentPath'] != str(target) or state['DropInPaths'] or
                state['NeedDaemonReload'] != 'no'):
            findings.append('loaded unit: ' + name)
    for name in LEGACY:
        target = directory / name
        if not target.is_symlink() or target.readlink() != Path('/dev/null'):
            findings.append('legacy mask: ' + name)
        state = query(name)
        if state['UnitFileState'] != 'masked' or state['ActiveState'] != 'inactive':
            findings.append('legacy authority: ' + name)
    return findings
