"""Plan or execute a verified cold USD-M backfill; dry-run reads metadata only."""
import argparse
from datetime import date, datetime, timedelta, timezone
import json
from urllib.error import HTTPError
from urllib.request import Request, urlopen

BASE = "https://data.binance.vision/data/futures/um/"
TYPES = ("klines", "markPriceKlines", "trades", "aggTrades", "bookDepth",
         "metrics", "fundingRate")
INTERVALS = ("1m", "5m", "15m")
SYMBOLS = ("BTCUSDT", "ETHUSDT")


def head_size(key):
    """Only 404 means unpublished; TLS, rate limits and outages must fail."""
    try:
        with urlopen(Request(BASE + key, method="HEAD"), timeout=30) as response:
            size = int(response.headers["Content-Length"])
            if size <= 0:
                raise ValueError(f"Empty upstream object: {key}")
            return size
    except HTTPError as error:
        if error.code == 404:
            return None
        raise


def archive_key(symbol, kind, interval, period, monthly=False):
    frequency = "monthly" if monthly else "daily"
    directory = f"{frequency}/{kind}/{symbol}"
    if interval:
        directory += f"/{interval}"
    return f"{directory}/{symbol}-{interval or kind}-{period}.zip"


def make_plan(start, end, symbols, types, intervals, head=head_size, funding_start=None,
              *, today):
    if start > end or end >= today:
        raise ValueError("Require an ordered range of closed UTC days")
    for selected, allowed in ((symbols, SYMBOLS), (types, TYPES), (intervals, INTERVALS)):
        if not selected or len(set(selected)) != len(selected) or set(selected) - set(allowed):
            raise ValueError(f"Unsupported or duplicate selection: {selected}")
    if funding_start and (funding_start.day != 1 or funding_start > start):
        raise ValueError("funding-start must be a month boundary no later than start")
    cache = {}
    files = []

    def inspect(key):
        if key not in cache:
            size = head(key)
            if size is not None and head(key + ".CHECKSUM") is None:
                raise ValueError(f"Missing CHECKSUM: {key}")
            cache[key] = size
        return cache[key]

    for symbol in symbols:
        for kind in types:
            monthly = kind == "fundingRate"
            current = (funding_start or start).replace(day=1) if monthly else start
            while current <= end:
                # Include the last completed month; never request the open month.
                if monthly and current >= today.replace(day=1):
                    break
                period = current.strftime("%Y-%m" if monthly else "%Y-%m-%d")
                for interval in intervals if kind in ("klines", "markPriceKlines") else (None,):
                    key = archive_key(symbol, kind, interval, period, monthly)
                    size = inspect(key)
                    source = key
                    if size is None and not monthly:
                        source = archive_key(symbol, kind, interval, period[:7], True)
                        size = inspect(source)
                    status = "unpublished"
                    if size is not None:
                        status = "pending" if source != key else "available"
                    directory = f"{symbol}/{kind}" + (f"/{interval}" if interval else "")
                    files.append({"symbol": symbol, "type": kind, "interval": interval,
                                  "date": period, "source": source if size is not None else None,
                                  "target": f"{directory}/{key.rsplit('/', 1)[1][:-4]}.csv",
                                  "bytes": size, "status": status})
                current = ((current.replace(day=28) + timedelta(days=4)).replace(day=1)
                           if monthly else current + timedelta(days=1))
    sources = {item["source"]: item["bytes"] for item in files if item["source"]}
    return {"timezone": "UTC", "start": start.isoformat(), "end": end.isoformat(),
            "days": (end - start).days + 1, "files": files, "archives": len(sources),
            "archive_bytes": sum(sources.values()),
            "missing": sum(item["status"] == "unpublished" for item in files)}


def main(argv=None, head=head_size, today=None, fetch=urlopen):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-date", type=date.fromisoformat, required=True)
    parser.add_argument("--end-date", type=date.fromisoformat, required=True)
    parser.add_argument("--funding-start", type=date.fromisoformat)
    parser.add_argument("--symbols", default=",".join(SYMBOLS))
    parser.add_argument("--data-types", default=",".join(TYPES))
    parser.add_argument("--intervals", default=",".join(INTERVALS))
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--data-root")
    args = parser.parse_args(argv)
    if not args.dry_run and not args.data_root:
        parser.error('--data-root is required for execution')
    try:
        result = make_plan(args.start_date, args.end_date, args.symbols.split(","),
                           args.data_types.split(","), args.intervals.split(","), head,
                           args.funding_start,
                           today=today if today is not None else datetime.now(timezone.utc).date())
        if not args.dry_run:
            from backfill_executor import execute_plan
            result = execute_plan(result, args.data_root, fetch)
    except Exception as error:
        result = dict(timezone='UTC', outcome='failed', error=str(error), written=0,
                      skipped=0, records=0, missing=0, complete_days=[],
                      finished_at=datetime.now(timezone.utc).isoformat())
    print(json.dumps(result, indent=2))
    return 0 if result.get('outcome', 'complete') == 'complete' else 1



if __name__ == "__main__":
    raise SystemExit(main())
