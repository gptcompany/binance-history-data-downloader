"""Source merges must not request the retired HTTP deployment path."""
from pathlib import Path

import yaml


def test_no_workflow_dispatches_progressive_deploy():
    root = Path(__file__).parents[1]
    for path in (root / ".github/workflows").glob("*.y*ml"):
        workflow = yaml.load(path.read_text(), Loader=yaml.BaseLoader)
        for job in workflow["jobs"].values():
            assert "gptcompany/progressive-deploy" not in job.get("uses", ""), path.name
            for step in job.get("steps", []):
                assert "gptcompany/progressive-deploy/dispatches" not in step.get("run", ""), path.name
