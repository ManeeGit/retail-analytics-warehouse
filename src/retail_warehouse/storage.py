"""Small filesystem primitives. A single writer and atomic pointer are the contract."""

from __future__ import annotations

import contextlib
import fcntl
import hashlib
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def atomic_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=".pending-")
    try:
        with os.fdopen(fd, "w") as stream:
            json.dump(value, stream, indent=2, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


@contextlib.contextmanager
def writer_lock(root: Path):
    """Advisory, nonblocking process lock, released by the OS on process death."""
    root.mkdir(parents=True, exist_ok=True)
    with (root / ".writer.lock").open("a+") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError("Another warehouse writer is running") from exc
        try:
            yield
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def current_release(root: Path) -> Path | None:
    pointer = root / "CURRENT.json"
    if not pointer.exists():
        return None
    name = json.loads(pointer.read_text())["release_id"]
    if Path(name).name != name or name.startswith("."):
        raise ValueError("Invalid release identifier")
    release = root / "releases" / name
    if not (release / "SUCCESS.json").exists():
        raise RuntimeError(f"Incomplete release: {name}")
    return release
