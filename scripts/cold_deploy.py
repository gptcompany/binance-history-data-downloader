from scripts.cold_snapshot import git_command
"""Pure contract for the owner-managed Binance historical cold scheduler."""
from datetime import date
from pathlib import PurePosixPath
import re

from jsonschema import Draft202012Validator

CONFIG_SCHEMA = {
    "type": "object", "additionalProperties": False,
    "required": ["image", "image_source_sha", "user", "uid", "gid", "cold_root"],
    "properties": {
        "image": {"type": "string", "pattern": "^ghcr[.]io/gptcompany/binance-downloader@sha256:[0-9a-f]{64}$"},
        "image_source_sha": {"type": "string", "pattern": "^[0-9a-f]{40}$"},
        "user": {"type": "string", "pattern": "^[a-z_][a-z0-9_-]*$"},
        "uid": {"type": "integer", "minimum": 0},
        "gid": {"type": "integer", "minimum": 0},
        "cold_root": {"type": "string", "pattern": "^/[A-Za-z0-9_./-]+$"},
    },
}


def safe_path(value):
    text = str(value)
    allowed = set('/._-0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ')
    path = PurePosixPath(text)
    if not path.is_absolute() or str(path) != text or '..' in path.parts or set(text) - allowed:
        raise ValueError('Require a normalized absolute path without unit substitutions')
    return text


def inventory(config):
    Draft202012Validator(CONFIG_SCHEMA).validate(config)
    safe_path(config['cold_root'])
    # JSON Schema's $ anchor can match before a final newline; units cannot allow it.
    if any(c.isspace() for key in ('image', 'image_source_sha', 'user') for c in config[key]):
        raise ValueError('Whitespace is forbidden in unit settings')
    return config


def render_units(config, release):
    config = inventory(config)
    release = safe_path(release)
    sha = PurePosixPath(release).name
    if not re.fullmatch('[0-9a-f]{40}', sha):
        raise ValueError('Release directory must be the approved full commit SHA')
    units = {}
    for name, action, clock in [('binance-cold', 'run', '03:30'),
                                ('binance-cold-drift', 'drift', '03:00')]:
        header = ['[Unit]', f'Description=Binance historical cold {action}']
        if action == 'run':
            header += [f"RequiresMountsFor={config['cold_root']}",
                       'Requires=docker.service', 'After=docker.service network-online.target',
                       'Wants=network-online.target']
        bootstrap = ("import subprocess,sys;sys.argv=" +
                     repr(['cold_snapshot', release, sha, action, release]) +
                     ";exec(subprocess.check_output(" +
                     repr(git_command(release, 'show', sha + ':scripts/cold_snapshot.py')) + "))")
        units[name + '.service'] = '\n'.join(header + [
            '', '[Service]', 'Type=oneshot', f"User={config['user']}", 'Environment=TZ=UTC',
            f'ExecStart={release}/.venv/bin/python -I -c "{bootstrap}"',
            'TimeoutStartSec=infinity', 'StandardOutput=journal', 'StandardError=journal', ''])
        units[name + '.timer'] = '\n'.join([
            '[Unit]', f'Description=Binance historical cold {action} UTC', '', '[Timer]',
            f'OnCalendar=*-*-* {clock}:00 UTC', 'Persistent=true', f'Unit={name}.service',
            '', '[Install]', 'WantedBy=timers.target', ''])
    return units


def docker_command(config, start: date, end: date, dry_run=False):
    config = inventory(config)
    return ['/usr/bin/docker', 'run', '--rm', '--pull=never', '--env', 'TZ=UTC',
            '--user', f"{int(config['uid'])}:{int(config['gid'])}", '--mount',
            f"type=bind,src={config['cold_root']},dst=/data", '--entrypoint', 'python',
            config['image'], '/app/src/backfill.py', '--start-date', start.isoformat(),
            '--end-date', end.isoformat(), '--data-root', '/data'] + (['--dry-run'] if dry_run else [])
