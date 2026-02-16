from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List


DATA_DIR = Path("/data")
PROJECTS_ROOT = DATA_DIR / "projects"
LEGACY_DEFAULT = "default"


def normalize_project_name(name: str | None) -> str:
    raw = (name or LEGACY_DEFAULT).strip().lower()
    raw = raw.replace(" ", "-")
    raw = re.sub(r"[^a-z0-9._-]", "-", raw)
    raw = re.sub(r"-{2,}", "-", raw).strip("-")
    return raw or LEGACY_DEFAULT


def project_dirs(project: str | None) -> Dict[str, Path]:
    p = normalize_project_name(project)
    base = PROJECTS_ROOT / p
    return {
        "name": p,
        "base": base,
        "config": base / "config",
        "raw": base / "raw",
        "chunks": base / "chunks",
        "manifests": base / "manifests",
        "exports": base / "exports",
        "app_mapping": base / "config" / "app_mapping.csv",
        "settings": base / "config" / "settings.json",
    }


def ensure_project_dirs(project: str | None) -> Dict[str, Path]:
    d = project_dirs(project)
    for k in ("base", "config", "raw", "chunks", "manifests", "exports"):
        d[k].mkdir(parents=True, exist_ok=True)
    return d


def get_project_app_mapping_path(project: str | None) -> Path:
    d = ensure_project_dirs(project)
    return d["app_mapping"]


def get_project_settings(project: str | None) -> Dict:
    d = ensure_project_dirs(project)
    settings = {"anonymize_default": False}
    p = d["settings"]
    if not p.exists():
        return settings
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
        if isinstance(raw, dict):
            settings["anonymize_default"] = bool(raw.get("anonymize_default", False))
    except Exception:
        pass
    return settings


def update_project_settings(project: str | None, anonymize_default: bool | None = None) -> Dict:
    d = ensure_project_dirs(project)
    cur = get_project_settings(d["name"])
    if anonymize_default is not None:
        cur["anonymize_default"] = bool(anonymize_default)
    d["settings"].write_text(json.dumps(cur, indent=2), encoding="utf-8")
    return cur


def list_projects() -> List[Dict]:
    PROJECTS_ROOT.mkdir(parents=True, exist_ok=True)
    out: List[Dict] = []
    for p in sorted(PROJECTS_ROOT.iterdir()):
        if not p.is_dir():
            continue
        manifests = p / "manifests"
        manifest_count = len(list(manifests.glob("manifest_*.json"))) if manifests.exists() else 0
        anonymize_default = False
        settings_p = p / "config" / "settings.json"
        if settings_p.exists():
            try:
                raw = json.loads(settings_p.read_text(encoding="utf-8"))
                anonymize_default = bool(raw.get("anonymize_default", False))
            except Exception:
                anonymize_default = False
        out.append(
            {
                "name": p.name,
                "path": str(p),
                "manifest_count": manifest_count,
                "anonymize_default": anonymize_default,
            }
        )
    return out


def build_history(project: str | None) -> List[Dict]:
    d = ensure_project_dirs(project)
    items: List[Dict] = []
    for mf in sorted(d["manifests"].glob("manifest_*.json"), key=lambda x: x.stat().st_mtime, reverse=True):
        try:
            import json

            m = json.loads(mf.read_text(encoding="utf-8"))
        except Exception:
            continue
        ts = m.get("generated_at_utc")
        if not ts:
            ts = datetime.fromtimestamp(mf.stat().st_mtime, tz=timezone.utc).isoformat()
        items.append(
            {
                "manifest_name": mf.name,
                "ingest_id": m.get("ingest_id"),
                "sheet": m.get("sheet"),
                "total_rows": m.get("total_rows"),
                "chunk_count": m.get("chunk_count"),
                "generated_at_utc": ts,
            }
        )
    return items
