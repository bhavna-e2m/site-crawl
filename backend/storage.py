"""Persist crawl data on Vercel Blob (optional) or local disk."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import requests

BLOB_API = "https://blob.vercel-storage.com"
BLOB_PREFIX = "sitecrawler"


def blob_enabled() -> bool:
    return bool(os.environ.get("BLOB_READ_WRITE_TOKEN"))


def _blob_path(rel: str) -> str:
    return f"{BLOB_PREFIX}/{rel.replace(os.sep, '/')}"


def blob_put(rel_path: str, data: bytes, *, content_type: str = "application/octet-stream") -> bool:
    token = os.environ.get("BLOB_READ_WRITE_TOKEN")
    if not token:
        return False
    pathname = _blob_path(rel_path)
    try:
        response = requests.put(
            BLOB_API,
            headers={
                "authorization": f"Bearer {token}",
                "x-vercel-filename": pathname,
                "x-content-type": content_type,
            },
            data=data,
            timeout=60,
        )
        response.raise_for_status()
        return True
    except requests.RequestException:
        return False


def blob_get(rel_path: str) -> bytes | None:
    token = os.environ.get("BLOB_READ_WRITE_TOKEN")
    if not token:
        return None
    pathname = _blob_path(rel_path)
    try:
        response = requests.get(
            BLOB_API,
            params={"pathname": pathname},
            headers={"authorization": f"Bearer {token}"},
            timeout=60,
        )
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return response.content
    except requests.RequestException:
        return None


def persist_file(data_dir: Path, rel_path: str, data: bytes) -> None:
    path = data_dir / rel_path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    if os.environ.get("VERCEL"):
        blob_put(rel_path, data)


def load_file(data_dir: Path, rel_path: str) -> bytes | None:
    if os.environ.get("VERCEL"):
        remote = blob_get(rel_path)
        if remote is not None:
            path = data_dir / rel_path
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(remote)
            return remote
    path = data_dir / rel_path
    if path.is_file():
        return path.read_bytes()
    return None


def load_json(data_dir: Path, rel_path: str, default: Any) -> Any:
    raw = load_file(data_dir, rel_path)
    if not raw:
        return default
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return default


def save_json(data_dir: Path, rel_path: str, payload: Any) -> None:
    data = json.dumps(payload, indent=2).encode("utf-8") + b"\n"
    persist_file(data_dir, rel_path, data)


def sync_directory(data_dir: Path, rel_dir: str) -> None:
    root = data_dir / rel_dir
    if not root.is_dir():
        return
    for path in root.rglob("*"):
        if path.is_file():
            rel = str(path.relative_to(data_dir))
            persist_file(data_dir, rel, path.read_bytes())
