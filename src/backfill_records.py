"""Strict streaming USD-M validation; exhaust the iterator before publishing CSV."""
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation

HEADERS = {
    'klines': ('open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time',
               'quote_volume', 'count', 'taker_buy_volume', 'taker_buy_quote_volume', 'ignore'),
    'trades': ('id', 'price', 'qty', 'quote_qty', 'time', 'is_buyer_maker'),
    'aggTrades': ('agg_trade_id', 'price', 'quantity', 'first_trade_id', 'last_trade_id',
                  'transact_time', 'is_buyer_maker'),
    'bookDepth': ('timestamp', 'percentage', 'depth', 'notional'),
    'metrics': ('create_time', 'symbol', 'sum_open_interest', 'sum_open_interest_value',
                'count_toptrader_long_short_ratio', 'sum_toptrader_long_short_ratio',
                'count_long_short_ratio', 'sum_taker_long_short_vol_ratio'),
    'fundingRate': ('calc_time', 'funding_interval_hours', 'last_funding_rate'),
}
HEADERS['markPriceKlines'] = HEADERS['klines']
UTC = timezone.utc


def bounds(period):
    start = datetime.fromisoformat(period + ('-01' if len(period) == 7 else '')).replace(tzinfo=UTC)
    end = ((start.replace(day=28) + timedelta(days=4)).replace(day=1)
           if len(period) == 7 else start + timedelta(days=1))
    return start, end


def timestamp(value, textual=False):
    if not textual:
        return datetime.fromtimestamp(int(value) / 1000, UTC)
    parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if parsed.tzinfo is not None and parsed.utcoffset() != timedelta(0):
        raise ValueError('Timestamp must use UTC')
    return parsed.replace(tzinfo=UTC)


def validated_rows(rows, item):
    """Validate every source record, yield only selected UTC day/month without header.

    Metrics archives cover (start, end]; other types cover [start, end).
    Uniqueness uses monotonic IDs/times and a bounded set of depth percentages
    at the current timestamp, avoiding a set of all historical trade IDs.
    """
    kind, interval = item['type'], item['interval']
    columns = HEADERS[kind]
    candle = kind in ('klines', 'markPriceKlines')
    step = {'1m': 60000, '5m': 300000, '15m': 900000}[interval] if candle else None
    prefix = f"{item['symbol']}-{interval or kind}-"
    name = item['source'].rsplit('/', 1)[-1]
    if not name.startswith(prefix) or not name.endswith('.zip'):
        raise ValueError('Archive name does not match selection')
    source_start, source_end = bounds(name[len(prefix):-4])
    target_start, target_end = bounds(item['date'])
    previous_time, previous_key, percentages, count = None, None, set(), 0
    for index, row in enumerate(rows):
        if index == 0 and tuple(row) == columns:
            continue
        if len(row) != len(columns):
            raise ValueError('Unexpected CSV field count')
        textual = kind in ('bookDepth', 'metrics')
        time_index = 4 if kind == 'trades' else 5 if kind == 'aggTrades' else 0
        instant = timestamp(row[time_index], textual)
        in_range = lambda start, end: (start < instant <= end if kind == 'metrics'
                                       else start <= instant < end)
        if not in_range(source_start, source_end):
            raise ValueError('Record outside source UTC period')
        if previous_time is not None and instant < previous_time:
            raise ValueError('Unordered timestamps')
        excluded = {time_index} | ({1} if kind == 'metrics' else set())
        if kind in ('trades', 'aggTrades'):
            excluded.add(len(row) - 1)
            if row[-1] not in ('true', 'false'):
                raise ValueError('Invalid maker boolean')
        try:
            for field, value in enumerate(row):
                if field in excluded:
                    continue
                number = Decimal(value)
                signed = (kind == 'bookDepth' and field == 1) or (kind == 'fundingRate' and field == 2)
                if not number.is_finite() or (number < 0 and not signed):
                    raise ValueError('Nonfinite or negative numeric value')
        except InvalidOperation as error:
            raise ValueError('Invalid numeric field') from error
        if kind == 'metrics' and row[1] != item['symbol']:
            raise ValueError('Unexpected metrics symbol')
        if kind == 'fundingRate' and not 1 <= int(row[1]) <= 24:
            raise ValueError('Invalid funding interval')
        key = int(row[0]) if kind in ('trades', 'aggTrades') else instant
        if kind == 'aggTrades' and not 0 <= int(row[3]) <= int(row[4]):
            raise ValueError('Invalid aggregate trade range')
        if kind == 'bookDepth':
            if instant != previous_time:
                percentages.clear()
            percentage = Decimal(row[1])
            if percentage in percentages:
                raise ValueError('Duplicate depth key')
            percentages.add(percentage)
        elif previous_key is not None and key <= previous_key:
            raise ValueError('Duplicate or unordered record key')
        if candle and (int(row[0]) % step or int(row[6]) != int(row[0]) + step - 1
                       or int(row[8]) < 0):
            raise ValueError('Invalid kline grid or close time')
        previous_time, previous_key = instant, key
        if in_range(target_start, target_end):
            if candle and instant != target_start + timedelta(milliseconds=count * step):
                raise ValueError('Gap in selected kline grid')
            count += 1
            yield row
    if not count or (candle and count != 86400000 // step):
        raise ValueError('Empty selection or incomplete kline day')
