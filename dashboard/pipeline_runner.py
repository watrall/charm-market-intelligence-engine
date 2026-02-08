from __future__ import annotations

import os
import sys
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from subprocess import PIPE, STDOUT, Popen


@dataclass(frozen=True)
class RunResult:
    returncode: int
    output: str


def _truthy(value: str | None) -> bool:
    if value is None:
        return False
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def allow_pipeline_run() -> bool:
    return _truthy(os.getenv("ALLOW_PIPELINE_RUN", "false"))


def lock_path(base: Path) -> Path:
    return base / "data" / "cache" / "pipeline.lock"


def acquire_lock(base: Path, stale_after_seconds: int = 6 * 60 * 60) -> tuple[bool, str]:
    """Create an exclusive lock file.

    Returns (ok, message). When ok is False, message explains why the lock could not be acquired.
    """
    path = lock_path(base)
    path.parent.mkdir(parents=True, exist_ok=True)

    try:
        stat = path.stat()
        age = time.time() - stat.st_mtime
        if age > stale_after_seconds:
            try:
                path.unlink()
            except OSError:
                pass
    except FileNotFoundError:
        pass

    try:
        fd = os.open(str(path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        return False, "A pipeline run is already in progress."
    except OSError as exc:
        return False, f"Could not create lock file. {type(exc).__name__}"

    try:
        payload = f"pid={os.getpid()}\ntime={int(time.time())}\n"
        os.write(fd, payload.encode("utf-8"))
    finally:
        os.close(fd)
    return True, ""


def release_lock(base: Path) -> None:
    path = lock_path(base)
    try:
        path.unlink()
    except FileNotFoundError:
        return
    except OSError:
        return


def run_pipeline(base: Path, env_overrides: dict[str, str] | None = None) -> RunResult:
    """Run scripts/pipeline.py as a subprocess and capture stdout.

    Uses the current interpreter so this works in local venvs and Docker.
    """
    max_output_chars = 10_000
    env = os.environ.copy()
    if env_overrides:
        env.update({k: str(v) for k, v in env_overrides.items() if v is not None})

    cmd = [sys.executable, "-u", str(base / "scripts" / "pipeline.py")]
    proc = Popen(cmd, cwd=str(base), env=env, stdout=PIPE, stderr=STDOUT, text=True, bufsize=1)

    buffer: deque[str] = deque()
    total_len = 0
    truncated = False
    if proc.stdout:
        for line in proc.stdout:
            buffer.append(line)
            total_len += len(line)
            if total_len > max_output_chars:
                # drop oldest lines to keep memory bounded
                overflow = total_len - max_output_chars
                while buffer and overflow > 0:
                    removed = buffer.popleft()
                    overflow -= len(removed)
                    total_len -= len(removed)
                    truncated = True
    returncode = proc.wait()
    output = "".join(buffer)
    if truncated:
        output = "(truncated output)\n" + output
    return RunResult(returncode=returncode, output=output)
