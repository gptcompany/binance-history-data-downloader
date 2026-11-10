"""Checksum-verified ZIP staging and validated CSV streaming; no extraction."""
import csv
import hashlib
from io import TextIOWrapper
from pathlib import Path
from urllib.request import urlopen
from zipfile import ZipFile

from backfill import BASE
from backfill_records import validated_rows


def prepare_archive(key, expected_size, directory, fetch=urlopen):
    """Verify Binance's checksum document first, then stream the exact ZIP bytes."""
    with fetch(BASE + key + '.CHECKSUM', timeout=30) as response:
        document = response.read(4097)
    fields = document.decode('ascii').split()
    if (len(document) > 4096 or len(fields) != 2 or len(fields[0]) != 64
            or set(fields[0]) - set('0123456789abcdefABCDEF')
            or fields[1].removeprefix('*') != key.rsplit('/', 1)[-1]):
        raise ValueError('Invalid CHECKSUM document')
    expected_digest = fields[0].lower()
    path = Path(directory) / (hashlib.sha256(key.encode()).hexdigest() + '.zip')
    digest, size = hashlib.sha256(), 0
    try:
        with fetch(BASE + key, timeout=30) as response, path.open('wb') as output:
            while chunk := response.read(1024 * 1024):
                size += len(chunk)
                if size > expected_size:
                    raise ValueError('Archive exceeds planned size')
                digest.update(chunk)
                output.write(chunk)
        if size != expected_size or digest.hexdigest() != expected_digest:
            raise ValueError('Archive size or SHA256 mismatch')
    except Exception:
        path.unlink(missing_ok=True)
        raise
    return path, expected_digest


def archive_rows(path, item):
    """Exhaust this iterator before publication: CSV, ZIP CRC and final grid checks."""
    expected = item['source'].rsplit('/', 1)[-1][:-4] + '.csv'
    with ZipFile(path) as archive:
        members = archive.infolist()
        if len(members) != 1 or members[0].filename != expected or members[0].is_dir():
            raise ValueError('Require exactly the expected CSV member')
        with archive.open(members[0]) as raw, TextIOWrapper(raw, encoding='utf-8-sig', newline='') as stream:
            yield from validated_rows(csv.reader(stream, strict=True), item)
