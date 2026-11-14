import importlib.util
import json
from pathlib import Path

import pytest

SCRIPT = Path(__file__).parents[1] / "scripts/check_workflow_runners.py"
spec = importlib.util.spec_from_file_location("check_workflow_runners", SCRIPT)
checker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(checker)

PUBLIC = "[self-hosted, Linux, X64, gbg-ci]"
GUARD = "github.event_name == 'push' || (github.event_name == 'pull_request' && github.event.pull_request.head.repo.full_name == github.repository)"

GENERIC = "${{ vars.CI_RUNS_ON }}"


def make_repo(tmp_path, runs_on, policy):
    workflows = tmp_path / ".github/workflows"
    workflows.mkdir(parents=True)
    (tmp_path / ".github/runner-policy.json").write_text(policy)
    (workflows / "ci.yml").write_text(f"jobs:\n  test:\n    if: {GUARD}\n    runs-on: {runs_on}\n    steps:\n      - run: true\n")
    return tmp_path


def test_repository_workflows_pass():
    assert checker.main() == 0


@pytest.mark.parametrize(
    ("runs_on", "visibility", "expected"),
    [
        (PUBLIC, "public", 0),
        ("ubuntu-latest", "public", 1),
        # Only the authorized ephemeral pool may run trusted repository code.
        (GENERIC, "public", 1),
        ("[self-hosted, sg-ci]", "public", 1),
        ("[ubuntu-latest]", "public", 1),
        ("ubuntu-24.04", "public", 1),
        (GENERIC, "private", 0),
        ("ubuntu-latest", "private", 1),
    ],
)
def test_runner_follows_declared_visibility(tmp_path, runs_on, visibility, expected):
    root = make_repo(tmp_path, runs_on, json.dumps({"visibility": visibility}))
    assert checker.main(root) == expected


@pytest.mark.parametrize(
    "policy",
    ["{", "[]", json.dumps({"visibility": "internal"}), json.dumps({"visibility": "public", "why": "x"}), "{}"],
)
def test_invalid_policy_fails(tmp_path, policy):
    root = make_repo(tmp_path, PUBLIC, policy)
    with pytest.raises(ValueError):
        checker.main(root)


@pytest.mark.parametrize(
    ("job", "expected"),
    [
        ("    uses: ./.github/workflows/reusable.yml\n", 0),
        # Review byb2-r2: a job-level reusable workflow call has no runs-on and escaped the check.
        ("    uses: ./.github/workflows/missing.yml\n", 1),
        ("    uses: gptcompany/.github/.github/workflows/_reusable-ai-review.yml@main\n", 1),
        ("    uses: ./other/reusable.yml\n", 1),
        ("    uses: ./.github/workflows/reusable.yml\n    runs-on: ubuntu-latest\n", 1),
        ("    name: empty\n", 1),
        ("    runs-on: ubuntu-latest\n    run-on: self-hosted\n", 1),
    ],
)
def test_reusable_workflows_must_be_local(tmp_path, job, expected):
    root = make_repo(tmp_path, PUBLIC, json.dumps({"visibility": "public"}))
    workflows = root / ".github/workflows"
    (workflows / "reusable.yml").write_text(
        f"on: workflow_call\njobs:\n  inner:\n    if: {GUARD}\n    runs-on: {PUBLIC}\n    steps:\n      - run: true\n"
    )
    (workflows / "caller.yml").write_text(f"jobs:\n  call:\n{job}")
    assert checker.main(root) == expected


def test_local_reusable_workflow_is_analyzed(tmp_path):
    root = make_repo(tmp_path, PUBLIC, json.dumps({"visibility": "public"}))
    workflows = root / ".github/workflows"
    (workflows / "reusable.yml").write_text("on: workflow_call\njobs:\n  inner:\n    runs-on: [self-hosted]\n")
    (workflows / "caller.yml").write_text("jobs:\n  call:\n    uses: ./.github/workflows/reusable.yml\n")
    assert checker.main(root) == 1


@pytest.mark.parametrize('guard', [None, 'true'])
def test_public_runner_rejects_untrusted_fork_execution(tmp_path, guard):
    root = make_repo(tmp_path, PUBLIC, json.dumps({'visibility': 'public'}))
    path = root / '.github/workflows/ci.yml'
    text = path.read_text()
    text = text.replace(f'    if: {GUARD}\n', '' if guard is None else f'    if: {guard}\n')
    path.write_text(text)
    assert checker.main(root) == 1
