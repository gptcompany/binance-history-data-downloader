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
        '@sha256:92cf8a26ccd63a3500de5c453fd5828913b24d36cfe048fe95d78821287d255d'
    ) in command
    assert config['image_source_sha'] == '313f8a87bb7c7a716349e527974d38170b702452'
    units = render_units(config, '/opt/binance-cold/' + config['image_source_sha'])
    assert len(units) == 4
    assert '03:30:00 UTC' in units['binance-cold.timer']
    assert '03:00:00 UTC' in units['binance-cold-drift.timer']
