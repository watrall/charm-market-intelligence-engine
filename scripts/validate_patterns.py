"""Validate job pattern configuration for CHARM."""

from pathlib import Path
import json
import re


def main():
    base = Path(__file__).resolve().parents[1]
    cfg_path = base / "config" / "job_patterns.json"
    data = json.loads(cfg_path.read_text(encoding="utf-8"))
    problems = []

    def _compile_entries(entries):
        local = []
        for entry in entries:
            pattern = entry if isinstance(entry, str) else entry.get("pattern")
            if not pattern:
                problems.append("Missing 'pattern' in entry.")
                continue
            try:
                re.compile(pattern, re.I)
            except re.error as exc:
                problems.append(f"Invalid regex '{pattern}': {exc}")
            local.append(pattern)
        return local

    for bucket, entries in data.get("job_type", {}).items():
        _compile_entries(entries)

    for bucket, entries in data.get("seniority", {}).items():
        _compile_entries(entries)

    if problems:
        raise SystemExit("Invalid job pattern config:\n- " + "\n- ".join(problems))
    print(f"OK: {cfg_path} patterns compiled successfully.")


if __name__ == "__main__":
    main()
