import re
from pathlib import Path

USES_RE = re.compile(r"^\s*-\s+uses:\s+([^@\s]+)@([^\s#]+)")
SHA_RE = re.compile(r"^[0-9a-f]{40}$")


def test_github_actions_are_pinned_to_full_sha():
    repo_root = Path(__file__).resolve().parents[1]
    workflows_dir = repo_root / ".github" / "workflows"

    unpinned = []
    for workflow in sorted(workflows_dir.glob("*.y*ml")):
        for line_no, line in enumerate(workflow.read_text(encoding="utf-8").splitlines(), start=1):
            match = USES_RE.match(line)
            if not match:
                continue
            action, ref = match.groups()
            if action.startswith("./"):
                continue
            if not SHA_RE.fullmatch(ref):
                unpinned.append(f"{workflow}:{line_no}")

    assert not unpinned, f"Unpinned workflow actions found: {', '.join(unpinned)}"
