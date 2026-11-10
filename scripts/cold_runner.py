"""Approved host actions; pre-run drift and one UTC JSON outcome per invocation."""
import argparse
from datetime import date, datetime, timezone
import json
from pathlib import Path
import subprocess

from jsonschema import Draft202012Validator
from scripts.cold_deploy import docker_command, inventory
from scripts.cold_host import drift, install

COUNTER = {'type': 'integer', 'minimum': 0}
DATES = {'type': 'array', 'items': {'type': 'string'}}
RESULT = {'type': 'object', 'additionalProperties': False,
          'required': ['timezone', 'outcome', 'written', 'skipped', 'records', 'missing'],
          'properties': dict(timezone={'const': 'UTC'}, outcome={'enum': ['complete', 'incomplete', 'failed']},
              **{key: COUNTER for key in ('written', 'skipped', 'records', 'missing', 'unavailable')},
              complete_days=DATES, settled_days=DATES, cursor={'type': 'string'},
              cursor_blocked={'type': 'boolean'}, error={'type': 'string'}, finished_at={'type': 'string'})}
FILE = {'type': 'object', 'additionalProperties': False,
        'required': ['symbol', 'type', 'interval', 'date', 'source', 'target', 'bytes', 'status'],
        'properties': {**{key: {'type': 'string'} for key in ('symbol', 'type', 'date', 'target')},
                       'interval': {'type': ['string', 'null']}, 'source': {'type': ['string', 'null']},
                       'bytes': {'type': ['integer', 'null'], 'minimum': 1},
                       'status': {'enum': ['available', 'unpublished']}}}
PLAN = {'type': 'object', 'additionalProperties': False,
        'required': ['timezone', 'start', 'end', 'days', 'files', 'archives', 'archive_bytes', 'missing'],
        'properties': dict(timezone={'const': 'UTC'}, start={'type': 'string'}, end={'type': 'string'},
            files={'type': 'array', 'items': FILE},
            **{key: COUNTER for key in ('days', 'archives', 'archive_bytes', 'missing')})}


def main(argv=None, approved=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['install', 'drift', 'run', 'backfill'])
    parser.add_argument('release', type=Path)
    parser.add_argument('--unit-directory', type=Path, default=Path('/etc/systemd/system'))
    parser.add_argument('--start-date', type=date.fromisoformat)
    parser.add_argument('--end-date', type=date.fromisoformat)
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args(argv)
    approved = approved or Path(__file__).parents[1]
    result = dict(timezone='UTC', outcome='complete', written=0, skipped=0,
                  records=0, missing=0, unavailable=0)
    try:
        config = inventory(json.loads((approved / 'deploy/cold.json').read_text()))
        if args.action == 'install':
            install(config, args.release, args.unit_directory)
        else:
            findings = drift(config, args.release, approved, args.unit_directory)
            if findings:
                result.update(outcome='failed', findings=findings)
            elif args.action != 'drift':
                if args.action == 'run':
                    if args.start_date or args.end_date or args.dry_run:
                        raise ValueError('Daily run uses only its UTC cursor')
                    command = docker_command(config, date(2026, 6, 29), date(2026, 6, 29))
                    command = command[:command.index('/app/src/backfill.py')] + [
                        '/app/src/backfill_cursor.py', '--initial-date', '2026-06-29', '--data-root', '/data']
                else:
                    if not args.start_date or not args.end_date:
                        raise ValueError('Backfill requires explicit start and end dates')
                    command = docker_command(config, args.start_date, args.end_date, args.dry_run)
                    command[command.index('--start-date'):command.index('--start-date')] = [
                        '--funding-start', '2026-05-01']
                child = subprocess.run(command, capture_output=True, text=True, check=False)
                payload = json.loads(child.stdout)
                if args.dry_run and 'outcome' not in payload:
                    Draft202012Validator(PLAN).validate(payload)
                    result.update(plan=payload, missing=payload['missing'])
                else:
                    Draft202012Validator(RESULT).validate(payload)
                    result = payload
                expected = 0 if result['outcome'] == 'complete' else 1
                if child.returncode != expected:
                    raise ValueError('Container exit status disagrees with JSON outcome')
    except Exception as error:
        result.update(outcome='failed', error=str(error))
    result['finished_at'] = datetime.now(timezone.utc).isoformat()
    print(json.dumps(result, sort_keys=True))
    return 0 if result['outcome'] == 'complete' else 1
