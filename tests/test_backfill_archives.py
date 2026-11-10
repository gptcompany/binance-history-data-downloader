"""Offline archives: verify official SHA256 before consuming a single CSV."""
import hashlib
from io import BytesIO
from zipfile import ZipFile

import pytest

KEY = 'daily/trades/BTCUSDT/BTCUSDT-trades-2026-10-28.zip'
NAME = KEY.rsplit('/', 1)[-1]
CSV = b'id,price,qty,quote_qty,time,is_buyer_maker\n1,2,3,6,1793145600000,true\n'
ITEM = dict(symbol='BTCUSDT', type='trades', interval=None, date='2026-10-28', source=KEY)


def zipped(members=None):
    output = BytesIO()
    with ZipFile(output, 'w') as archive:
        for name, data in (members or [(NAME[:-4] + '.csv', CSV)]):
            archive.writestr(name, data)
    return output.getvalue()


def transport(payload, checksum=None):
    calls = []
    digest = hashlib.sha256(payload).hexdigest()
    checksum = checksum if checksum is not None else f'{digest}  {NAME}\n'.encode()
    def fetch(url, timeout):
        calls.append(url)
        return BytesIO(checksum if url.endswith('.CHECKSUM') else payload)
    return fetch, calls, digest


def test_checksum_size_and_csv_are_verified_offline(tmp_path):
    from backfill_archives import prepare_archive, archive_rows
    payload = zipped()
    fetch, calls, digest = transport(payload)
    path, actual = prepare_archive(KEY, len(payload), tmp_path, fetch)
    assert actual == digest and path.read_bytes() == payload
    assert [url.rsplit('/', 1)[-1] for url in calls] == [NAME + '.CHECKSUM', NAME]
    assert list(archive_rows(path, ITEM)) == [['1', '2', '3', '6', '1793145600000', 'true']]


@pytest.mark.parametrize('checksum', [
    b'not-a-checksum', b'a' * 64 + b'  other.zip\n',
    b'a' * 64 + b'  ' + NAME.encode() + b'\nextra', b'x' * 5000,
])
def test_invalid_checksum_document_prevents_archive_request(tmp_path, checksum):
    from backfill_archives import prepare_archive
    payload = zipped()
    fetch, calls, _ = transport(payload, checksum)
    with pytest.raises(ValueError): prepare_archive(KEY, len(payload), tmp_path, fetch)
    assert len(calls) == 1 and not list(tmp_path.iterdir())


@pytest.mark.parametrize('error', ['digest', 'short', 'long'])
def test_corrupt_or_wrong_size_archives_leave_no_unverified_file(tmp_path, error):
    from backfill_archives import prepare_archive
    payload = zipped()
    checksum = b'0' * 64 + b'  ' + NAME.encode() if error == 'digest' else None
    fetch, _, _ = transport(payload, checksum)
    size = len(payload) + (1 if error == 'short' else -1 if error == 'long' else 0)
    with pytest.raises(ValueError): prepare_archive(KEY, size, tmp_path, fetch)
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize('members', [
    [('../escape.csv', CSV)], [('unexpected.csv', CSV)],
    [(NAME[:-4] + '.csv', CSV), ('extra.csv', CSV)],
    [(NAME[:-4] + '.csv', CSV), (NAME[:-4] + '.csv', CSV)],
])
def test_zip_contract_rejects_unsafe_wrong_or_multiple_members(tmp_path, members):
    from backfill_archives import prepare_archive, archive_rows
    payload = zipped(members)
    fetch, _, _ = transport(payload)
    path, _ = prepare_archive(KEY, len(payload), tmp_path, fetch)
    with pytest.raises(ValueError): list(archive_rows(path, ITEM))
    assert not (tmp_path.parent / 'escape.csv').exists()


def test_verified_checksum_does_not_bypass_record_validation(tmp_path):
    from backfill_archives import prepare_archive, archive_rows
    payload = zipped([(NAME[:-4] + '.csv', CSV.replace(b'true', b'invalid'))])
    fetch, _, _ = transport(payload)
    path, _ = prepare_archive(KEY, len(payload), tmp_path, fetch)
    with pytest.raises(ValueError): list(archive_rows(path, ITEM))


def test_checksum_binary_marker_is_supported(tmp_path):
    from backfill_archives import prepare_archive
    payload = zipped()
    digest = hashlib.sha256(payload).hexdigest()
    fetch, _, _ = transport(payload, f'{digest} *{NAME}\n'.encode())
    assert prepare_archive(KEY, len(payload), tmp_path, fetch)[1] == digest
