"""Locked cold backfill: verify fully, then atomically publish CSV and provenance."""
import csv
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.request import urlopen

from jsonschema import Draft202012Validator, ValidationError

from backfill_archives import archive_rows, prepare_archive
from backfill_records import HEADERS

SELECTION = ('symbol', 'type', 'interval', 'date', 'source')
HASH = {'type': 'string', 'pattern': '^[0-9a-f]{64}$', 'minLength': 64, 'maxLength': 64}
PROPERTIES = {key: {'type': 'string'} for key in SELECTION}
PROPERTIES.update(interval={'type': ['string', 'null']}, timezone={'const': 'UTC'},
                  archive_sha256=HASH, csv_sha256=HASH, rows={'type': 'integer', 'minimum': 1})
MANIFEST = Draft202012Validator({'type': 'object', 'additionalProperties': False,
                                'required': list(PROPERTIES), 'properties': PROPERTIES})


def file_digest(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def verified(target, item):
    try:
        manifest = json.loads(target.with_suffix('.json').read_text())
        MANIFEST.validate(manifest)
        if (all(manifest[key] == item[key] for key in SELECTION)
                and file_digest(target) == manifest['csv_sha256']):
            return manifest
    except (OSError, ValueError, ValidationError):
        pass
    return None


def publish(target, item, archive, digest):
    target.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix='.backfill-stage-', dir=target.parent) as temporary:
        staged = Path(temporary) / 'data.csv'
        count = 0
        with staged.open('w', newline='') as output:
            writer = csv.writer(output, lineterminator='\n')
            writer.writerow(HEADERS[item['type']])
            for row in archive_rows(archive, item):
                writer.writerow(row)
                count += 1
            output.flush()
            os.fsync(output.fileno())
        manifest = {key: item[key] for key in SELECTION}
        manifest.update(timezone='UTC', archive_sha256=digest,
                        csv_sha256=file_digest(staged), rows=count)
        MANIFEST.validate(manifest)
        sidecar = Path(temporary) / 'manifest.json'
        with sidecar.open('w') as output:
            json.dump(manifest, output, sort_keys=True)
            output.flush()
            os.fsync(output.fileno())
        os.replace(staged, target)
        os.replace(sidecar, target.with_suffix('.json'))
        directory = os.open(target.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
        return manifest


def execute_plan(plan, data_root, fetch=urlopen):
    result = dict(timezone='UTC', outcome='failed', written=0, skipped=0, records=0,
                  missing=plan['missing'], complete_days=[])
    completed = set()
    root = Path(data_root)
    try:
        root.mkdir(parents=True, exist_ok=True)
        with (root / '.backfill.lock').open('a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            groups = {}
            for item in plan['files']:
                if item['source'] is not None:
                    groups.setdefault(item['source'], []).append(item)
            with TemporaryDirectory(prefix='.backfill-stage-', dir=root) as temporary:
                for source, items in groups.items():
                    pending = []
                    for item in items:
                        manifest = verified(root / item['target'], item)
                        if manifest is None:
                            pending.append(item)
                        else:
                            result['skipped'] += 1
                            result['records'] += manifest['rows']
                            completed.add(item['target'])
                    if not pending:
                        continue
                    archive, digest = prepare_archive(source, pending[0]['bytes'], temporary, fetch)
                    for item in pending:
                        manifest = publish(root / item['target'], item, archive, digest)
                        result['written'] += 1
                        result['records'] += manifest['rows']
                        completed.add(item['target'])
                    archive.unlink()
            days = {item['date'] for item in plan['files'] if item['type'] != 'fundingRate'}
            result['complete_days'] = sorted(day for day in days if all(
                item['target'] in completed for item in plan['files']
                if item['date'] == (day[:7] if item['type'] == 'fundingRate' else day)))
            result['outcome'] = 'incomplete' if plan['missing'] else 'complete'
    except Exception as error:
        result['error'] = str(error)
    result['finished_at'] = datetime.now(timezone.utc).isoformat()
    return result
