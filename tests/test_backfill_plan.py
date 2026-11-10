"""Offline metadata fixtures: no archive or cold filesystem access."""
from datetime import date
from urllib.error import HTTPError

import pytest

from backfill import make_plan, main


def metadata(missing=()):
    calls = []

    def head(key):
        calls.append(key)
        return None if key in missing else 123

    return head, calls


def plan(head, start="2026-10-28", end="2026-10-28", **kwargs):
    return make_plan(date.fromisoformat(start), date.fromisoformat(end),
                     ["BTCUSDT"], ["klines"], ["1m"], head,
                     today=date(2026, 11, 10), **kwargs)


def test_daily_plan_checks_archive_and_checksum():
    head, calls = metadata()
    result = plan(head)
    item = result["files"][0]
    assert item["source"] == "daily/klines/BTCUSDT/1m/BTCUSDT-1m-2026-10-28.zip"
    assert item["target"] == "BTCUSDT/klines/1m/BTCUSDT-1m-2026-10-28.csv"
    assert item["status"] == "available"
    assert result["days"] == 1
    assert result["archive_bytes"] == 123
    assert calls == [item["source"], item["source"] + ".CHECKSUM"]


def test_monthly_fallback_shared_archive_counted_once():
    missing = {f"daily/klines/BTCUSDT/1m/BTCUSDT-1m-2026-10-{day}.zip"
               for day in (28, 29)}
    head, calls = metadata(missing)
    result = plan(head, end="2026-10-29")
    assert len(result["files"]) == 2
    assert all(item["source"].startswith("monthly/") for item in result["files"])
    assert result["archives"] == 1
    assert result["archive_bytes"] == 123
    assert len(calls) == 4  # two missing daily HEADs, one monthly + checksum


def test_missing_checksum_never_counts_as_available():
    key = "daily/klines/BTCUSDT/1m/BTCUSDT-1m-2026-10-28.zip"
    head, _ = metadata({key + ".CHECKSUM"})
    with pytest.raises(ValueError, match="CHECKSUM"):
        plan(head)


def test_bookdepth_hole_is_explicit():
    head, _ = metadata({"daily/bookDepth/BTCUSDT/BTCUSDT-bookDepth-2026-10-08.zip",
                        "monthly/bookDepth/BTCUSDT/BTCUSDT-bookDepth-2026-10.zip"})
    result = make_plan(date(2026, 10, 8), date(2026, 10, 8), ["BTCUSDT"],
                       ["bookDepth"], ["1m"], head, today=date(2026, 11, 10))
    assert result["files"][0]["status"] == "unpublished"
    assert result["missing"] == 1
    assert result["archives"] == 0


@pytest.mark.parametrize("today,last_month", [(date(2026, 11, 10), 10),
                                               (date(2026, 12, 1), 11)])
def test_funding_explicit_start_includes_may_and_last_closed_month(today, last_month):
    head, _ = metadata()
    result = make_plan(date(2026, 6, 29), date(2026, 11, 9), ["BTCUSDT"],
                       ["fundingRate"], ["1m"], head, funding_start=date(2026, 5, 1),
                       today=today)
    assert [item["date"] for item in result["files"]] == [
        f"2026-{month:02d}" for month in range(5, last_month + 1)]
    assert result["archives"] == last_month - 4


@pytest.mark.parametrize("start,end", [("2026-11-09", "2026-11-08"),
                                       ("2999-01-01", "2999-01-02")])
def test_bad_dates_fail_before_metadata(start, end):
    def no_network(key):
        pytest.fail("must validate before network")
    with pytest.raises(ValueError):
        plan(no_network, start, end)


def test_unknown_series_rejected():
    head, calls = metadata()
    with pytest.raises(ValueError):
        make_plan(date(2026, 1, 1), date(2026, 1, 1), ["../BTC"],
                  ["klines"], ["1m"], head, today=date(2026, 11, 10))
    assert not calls


def test_dry_run_prints_json_and_creates_no_files(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    head, _ = metadata()
    assert main(["--start-date", "2026-10-28", "--end-date", "2026-10-28",
                 "--symbols", "BTCUSDT", "--data-types", "klines",
                 "--intervals", "1m", "--dry-run"], head=head, today=date(2026, 11, 10)) == 0
    import json
    assert json.loads(capsys.readouterr().out)["archives"] == 1
    assert list(tmp_path.iterdir()) == []


def test_http_errors_other_than_404_fail_closed(monkeypatch):
    from backfill import head_size
    def unavailable(*args, **kwargs):
        raise HTTPError("https://data.binance.vision/", 503, "offline", {}, None)
    monkeypatch.setattr("backfill.urlopen", unavailable)
    with pytest.raises(HTTPError):
        head_size("daily/trades/BTCUSDT/example.zip")
