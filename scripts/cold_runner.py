"""Approved host actions; pre-run drift and one UTC JSON outcome per invocation."""
import argparse
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
import json
from pathlib import Path
import signal
import subprocess
import sys
import time

from jsonschema import Draft202012Validator
from scripts.cold_deploy import docker_command, inventory
from scripts.cold_host import drift, install

COUNTER = {'type': 'integer', 'minimum': 0}
DATES = {'type': 'array', 'items': {'type': 'string'}}
MONTHLY_GAP = {'type': 'object', 'additionalProperties': False,
               'required': ['target', 'source', 'status', 'reason'],
               'properties': dict(target={'type': 'string'}, source={'type': 'string'},
                                  status={'enum': ['missing', 'unavailable']},
                                  reason={'type': 'string'})}
RESULT = {'type': 'object', 'additionalProperties': False,
          'required': ['timezone', 'outcome', 'written', 'skipped', 'records', 'missing'],
          'properties': dict(timezone={'const': 'UTC'}, outcome={'enum': ['complete', 'incomplete', 'failed']},
              **{key: COUNTER for key in ('written', 'skipped', 'records', 'missing', 'unavailable')},
              complete_days=DATES, settled_days=DATES, cursor={'type': 'string'},
              monthly_gaps={'type': 'array', 'items': MONTHLY_GAP},
              **{key: {'type': ['string', 'null']} for key in ('error_target', 'error_source', 'error_type')},
              cursor_blocked={'type': 'boolean'}, error={'type': 'string'}, finished_at={'type': 'string'})}
RESULT['properties']['final_state_status'] = {'enum': ['unknown']}
RESULT['properties']['phase'] = {'enum': ['cursor']}
RESULT['properties']['unresolved_targets'] = DATES
RESULT['properties']['attempt_totals'] = {
    'type': 'object', 'additionalProperties': False,
    'required': ['written', 'skipped', 'records'],
    'properties': {key: COUNTER for key in ('written', 'skipped', 'records')}}
FILE = {'type': 'object', 'additionalProperties': False,
        'required': ['symbol', 'type', 'interval', 'date', 'source', 'target', 'bytes', 'status'],
        'properties': {**{key: {'type': 'string'} for key in ('symbol', 'type', 'date', 'target')},
                       'interval': {'type': ['string', 'null']}, 'source': {'type': ['string', 'null']},
                       'bytes': {'type': ['integer', 'null'], 'minimum': 1},
                       'status': {'enum': ['available', 'pending', 'unpublished']}}}
PLAN = {'type': 'object', 'additionalProperties': False,
        'required': ['timezone', 'start', 'end', 'days', 'files', 'archives', 'archive_bytes', 'missing'],
        'properties': dict(timezone={'const': 'UTC'}, start={'type': 'string'}, end={'type': 'string'},
            files={'type': 'array', 'items': FILE},
            **{key: COUNTER for key in ('days', 'archives', 'archive_bytes', 'missing')})}


@dataclass
class CycleState:
    """Only validated cursor passes contribute progress; probes never replace it."""
    payload: dict | None = None
    child: object = None
    plan: dict | None = None
    seen: set = field(default_factory=set)

    def download(self, command):
        self.child = subprocess.run(command, capture_output=True, check=False)
        try:
            payload = json.loads(self.child.stdout)
            Draft202012Validator(RESULT).validate(payload)
            if self.child.returncode != int(payload['outcome'] != 'complete'):
                raise ValueError('Container exit status disagrees with JSON outcome')
        except Exception as error:
            if self.payload is not None:
                raise ValueError('Invalid cursor result') from error
            return
        previous = self.payload['attempt_totals'] if self.payload else {}
        totals = {key: previous.get(key, 0) + payload[key]
                  for key in ('written', 'skipped', 'records')}
        # Publish counts and last-pass metadata together, never partial totals.
        self.payload = dict(payload, written=totals['written'], attempt_totals=totals)

    def finish(self, command):
        if self.payload is not None:
            self.child = subprocess.CompletedProcess(command,
                int(self.payload['outcome'] != 'complete'),
                json.dumps(self.payload).encode(), self.child.stderr if self.child else b'')


def run_daily(command, *, now=None, sleep=time.sleep):
    """Poll HEAD metadata with a single cycle state finalized on every exit."""
    now = now or (lambda: datetime.now(timezone.utc))
    started = now()
    deadline = started.replace(hour=14, minute=0, second=0, microsecond=0)
    end = (started.date() - timedelta(days=1)).isoformat()
    state = CycleState()
    try:
        state.download(command)
        # Persistent timer catch-up still attempts D-1 once, without polling.
        while (started < deadline and state.payload is not None
               and state.payload['outcome'] == 'incomplete'):
            probe = command[:command.index('/app/src/backfill_cursor.py')] + [
                '/app/src/backfill.py', '--start-date', min(state.payload.get('cursor', end), end),
                '--end-date', end, '--funding-start', '2026-05-01', '--dry-run']
            if state.plan is not None and now() >= deadline:
                break
            checked = subprocess.run(probe, capture_output=True, check=False)
            sys.stderr.buffer.write(checked.stderr)
            if checked.returncode:
                failure = json.loads(checked.stdout)
                Draft202012Validator(RESULT).validate(failure)
                raise ValueError(failure.get('error', 'Metadata probe failed'))
            state.plan = json.loads(checked.stdout)
            Draft202012Validator(PLAN).validate(state.plan)
            available = {(item['source'], item['bytes']) for item in state.plan['files'] if item['source']}
            if now() >= deadline:
                break
            if available - state.seen:
                sys.stderr.buffer.write(state.child.stderr)
                state.download(command)
                if state.payload['outcome'] != 'incomplete':
                    break
            state.seen = available
            remaining = (deadline - now()).total_seconds()
            if remaining > 0:
                sleep(min(900, remaining))
        if state.plan is not None and state.payload['outcome'] == 'incomplete':
            state.payload['unresolved_targets'] = sorted(
                {item['target'] for item in state.plan['files'] if item['source'] is None}
                | {item['target'] for item in state.payload.get('monthly_gaps', [])})
            state.payload['error'] = 'Publication deadline 14:00 UTC exceeded'
    except (Exception, KeyboardInterrupt) as error:
        if state.payload is None:
            raise
        state.payload.update(outcome='failed', error=str(error) or type(error).__name__,
                             final_state_status='unknown')
    finally:
        state.finish(command)
    return state.child


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
                  records=0, missing=0, unavailable=0, phase='preflight', counts_status='not_started')
    def interrupted(signum, frame):
        raise InterruptedError(signal.Signals(signum).name)

    handlers = {sig: signal.signal(sig, interrupted) for sig in (signal.SIGINT, signal.SIGTERM)}
    try:
        if args.dry_run and args.action != 'backfill':
            raise ValueError('Only backfill supports dry-run')
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
                # Adapted from the approved Bybit cold runner: bytes, explicit
                # container boundary and unknown progress until the child validates.
                result.update(phase='container', counts_status='not_started',
                              **{key: None for key in ('written', 'skipped', 'records', 'missing', 'unavailable')})
                child = (run_daily(command) if args.action == 'run' else
                         subprocess.run(command, capture_output=True, check=False))
                # A signal after the cycle returns must not cut the summary
                # write: it stays pending across the snapshot and the atomic
                # JSON, then keeps its exit status when re-raised in finally.
                signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGINT, signal.SIGTERM})
                result.update(returncode=child.returncode, counts_status='unknown')
                sys.stderr.buffer.write(child.stderr)
                sys.stderr.buffer.flush()
                if child.returncode in (125, 126, 127):
                    result['counts_status'] = 'not_started'
                    raise ValueError('ContainerNotStarted')
                payload = json.loads(child.stdout)
                if args.dry_run and 'outcome' not in payload:
                    Draft202012Validator(PLAN).validate(payload)
                    summary = dict(timezone='UTC', outcome='complete', plan=payload,
                                   written=0, skipped=0, records=0, missing=payload['missing'])
                else:
                    Draft202012Validator(RESULT).validate(payload)
                    summary = payload
                expected = 0 if summary['outcome'] == 'complete' else 1
                if child.returncode != expected:
                    raise ValueError('Container exit status disagrees with JSON outcome')
                result = dict(summary, phase=summary.get('phase', 'container'),
                              counts_status='reported', returncode=child.returncode)
    except (Exception, KeyboardInterrupt) as error:
        result.update(outcome='failed', error=str(error) or type(error).__name__)
    finally:
        result['finished_at'] = datetime.now(timezone.utc).isoformat()
        print(json.dumps(result, sort_keys=True))
        pending = signal.sigpending()
        for sig, handler in handlers.items():
            signal.signal(sig, signal.SIG_DFL if sig in pending else handler)
        signal.pthread_sigmask(signal.SIG_UNBLOCK, set(handlers))
        for sig in handlers:
            if sig in pending:
                signal.raise_signal(sig)
    return 0 if result['outcome'] == 'complete' else 1
