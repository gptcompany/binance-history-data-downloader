"""The deployable owner inventory must render the approved cold-only command."""
from datetime import date
import json
from pathlib import Path

from scripts.cold_deploy import docker_command, inventory, render_units


def test_versioned_inventory_is_the_complete_cold_deployment_source():
    root = Path(__file__).parents[1]
    config = inventory(json.loads((root / 'deploy/cold.json').read_text()))
    command = docker_command(config, date(2026, 6, 29), date(2026, 11, 9), True)
    assert command[command.index('--mount') + 1] == (
        'type=bind,src=/data/cold/12TB/binance-history-data-downloader/data,dst=/data')
    assert command[command.index('--entrypoint') + 1] == 'python'
    assert '--pull=never' in command and command[-1] == '--dry-run'
    assert (
        'ghcr.io/gptcompany/binance-downloader'
        '@sha256:beca5d7b878a0e0889014080cbd4a47dccc4a46970b43cec1ff5f0e2ab6a34cf'
    ) in command
    assert config['image_source_sha'] == 'd979c2c47d9a87f5e7d17a9a9ecc45299d836270'
    units = render_units(config, '/opt/binance-cold/' + config['image_source_sha'])
    assert len(units) == 4
    assert '06:30:00 UTC' in units['binance-cold.timer']
    assert '03:00:00 UTC' in units['binance-cold-drift.timer']
