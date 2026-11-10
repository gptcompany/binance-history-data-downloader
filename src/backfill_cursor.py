"""Resume closed UTC days, preserving declared absences and stopping at new gaps."""
import argparse
from datetime import date, datetime, timedelta, timezone
import fcntl
import json
import os
from pathlib import Path
from tempfile import NamedTemporaryFile

from jsonschema import Draft202012Validator, FormatChecker, ValidationError

from backfill import INTERVALS, SYMBOLS, TYPES, make_plan
from backfill_executor import execute_plan

DATE = {'type': 'string', 'format': 'date'}
ABSENCES = {'type': 'array', 'uniqueItems': True, 'items': {
    'type': 'object', 'additionalProperties': False,
    'required': ['symbol', 'type', 'date', 'source', 'checked_at'], 'properties': {
        'symbol': {'enum': list(SYMBOLS)}, 'type': {'const': 'bookDepth'},
        'date': DATE, 'checked_at': DATE, 'source': {'type': 'string', 'format': 'uri'}}}}
CURSOR = {'type': 'object', 'additionalProperties': False,
          'required': ['timezone', 'next_date'],
          'properties': {'timezone': {'const': 'UTC'}, 'next_date': DATE}}


def validate(schema, value):
    try:
        Draft202012Validator(schema, format_checker=FormatChecker()).validate(value)
    except ValidationError as error:
        raise ValueError(error.message) from error


def classify_unavailable(plan, entries):
    validate(ABSENCES, entries)
    known = {(entry['symbol'], entry['type'], entry['date']) for entry in entries}
    if len(known) != len(entries):
        raise ValueError('Duplicate absence identity')
    for item in plan['files']:
        if item['source'] is None and (item['symbol'], item['type'], item['date']) in known:
            item['status'] = 'unavailable'
    plan['missing'] = sum(item['status'] == 'unpublished' for item in plan['files'])
    plan['unavailable'] = sum(item['status'] == 'unavailable' for item in plan['files'])


def run_cursor(data_root, initial, today, execute):
    if initial >= today:
        raise ValueError('Initial date must be a closed UTC day')
    root = Path(data_root)
    root.mkdir(parents=True, exist_ok=True)
    with (root / '.cold-cursor.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        path = root / '.cold-cursor.json'
        state = json.loads(path.read_text()) if path.exists() else dict(
            timezone='UTC', next_date=initial.isoformat())
        validate(CURSOR, state)
        start = date.fromisoformat(state['next_date'])
        if start > today or start < initial:
            raise ValueError('Cursor outside the approved UTC range')
        result = execute(start, today - timedelta(days=1))
        current = start
        if result['outcome'] != 'failed' and not result.get('cursor_blocked', False):
            settled = set(result['settled_days'])
            while current < today and current.isoformat() in settled:
                current += timedelta(days=1)
        try:
            state['next_date'] = current.isoformat()
            with NamedTemporaryFile(mode='w', dir=root, delete=False) as temporary:
                staged = Path(temporary.name)
                try:
                    json.dump(state, temporary, sort_keys=True)
                    temporary.flush()
                    os.fsync(temporary.fileno())
                    os.replace(staged, path)
                finally:
                    staged.unlink(missing_ok=True)
            directory = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        except Exception as error:
            result.update(outcome='failed', phase='cursor', error=str(error))
            return result
        result['cursor'] = current.isoformat()
        return result


def main(argv=None, today=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--initial-date', type=date.fromisoformat, required=True)
    parser.add_argument('--data-root', type=Path, required=True)
    args = parser.parse_args(argv)
    today = today or datetime.now(timezone.utc).date()
    try:
        entries = json.loads((Path(__file__).parents[1] / 'config/unavailable.json').read_text())
        def execute(start, end):
            plan = (make_plan(start, end, SYMBOLS, TYPES, INTERVALS,
                              funding_start=date(2026, 5, 1), today=today)
                    if start <= end else dict(files=[], missing=0))
            # The cursor settles acknowledged absence, never its eventual recovery.
            validate(ABSENCES, entries)
            targets = {item['target'] for item in plan['files']}
            for entry in entries:
                day = date.fromisoformat(entry['date'])
                if day < start and day <= end:
                    retry = make_plan(day, day, (entry['symbol'],), (entry['type'],),
                                      INTERVALS, today=today)
                    for item in retry['files']:
                        if item['target'] not in targets:
                            plan['files'].append(item)
                            targets.add(item['target'])
            classify_unavailable(plan, entries)
            result = execute_plan(plan, args.data_root)
            result['cursor_blocked'] = any(item['type'] == 'fundingRate' and
                                           item['source'] is None for item in plan['files'])
            return result
        result = run_cursor(args.data_root, args.initial_date, today, execute)
    except Exception as error:
        result = dict(timezone='UTC', outcome='failed', error=str(error), written=0,
                      skipped=0, records=0, missing=0, unavailable=0,
                      finished_at=datetime.now(timezone.utc).isoformat())
    print(json.dumps(result, sort_keys=True))
    return 0 if result['outcome'] == 'complete' else 1


if __name__ == '__main__':
    raise SystemExit(main())
