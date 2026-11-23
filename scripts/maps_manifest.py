"""
maps_manifest.py — lightweight (no cartopy) builder for outputs/maps_manifest.json.

Shared by generate_maps.py (after rendering) and cleanup_repo.py (after
pruning), so the manifest always describes the GIFs that actually exist on
disk. Previously cleanup ran *after* the manifest was written, leaving the
dashboard pointing at deleted files.
"""
import json
import os
import re
from datetime import datetime

MAPS_DIR = "outputs/maps"
MANIFEST_PATH = "outputs/maps_manifest.json"

# Order matters only for readability; matching is exact-prefix + run-id regex,
# so GEFS never swallows GEFS_35D and ECMWF never swallows ECMWF_ENS.
MAP_MODELS = [
    "GFS", "GEFS", "GEFS_35D", "ECMWF", "ECMWF_ENS", "ECMWF_AIFS", "AIGFS",
    "HGEFS", "CMC_ENS", "GOOGLE_WN2", "GOOGLE_WN3", "AIGEFS", "AIFS_ENS",
    "UKMO_ENS", "EC46", "HRRR", "NAM", "NBM", "ICON", "FOURCASTNETV2-SMALL",
]

# Run IDs look like 20261122_12, 20261122_12_AI or 20261122_seamless.
RUN_RE = r"\d{8}_(?:\d{2}(?:_AI)?|seamless)"
GIF_RE = re.compile(rf"^(?P<model>[A-Z0-9_\-]+?)_(?P<kind>shift|anom)_(?P<latest>{RUN_RE})(?:_vs_(?P<prev>{RUN_RE}))?\.gif$")


def run_to_dt(run_id):
    """Parse a run ID into a naive UTC datetime (None if unparseable)."""
    try:
        d = run_id[:8]
        rest = run_id[9:11]
        hh = int(rest) if rest.isdigit() else 0
        return datetime.strptime(d, "%Y%m%d").replace(hour=hh)
    except Exception:
        return None


def hours_between(a, b):
    da, db = run_to_dt(a), run_to_dt(b)
    if da is None or db is None:
        return None
    return int(round((da - db).total_seconds() / 3600))


def parse_gif(name):
    m = GIF_RE.match(name)
    if not m or m.group("model") not in MAP_MODELS:
        return None
    return m.groupdict()


def rebuild_manifest(maps_dir=MAPS_DIR):
    """Scan maps_dir and return {model: [entries newest-first]}."""
    manifest = {}
    if not os.path.isdir(maps_dir):
        return manifest
    for f in os.listdir(maps_dir):
        info = parse_gif(f)
        if not info:
            continue
        entry = {"latest": info["latest"], "file": f, "kind": info["kind"]}
        if info["kind"] == "shift":
            entry["previous"] = info["prev"]
            entry["hours"] = hours_between(info["latest"], info["prev"])
        manifest.setdefault(info["model"], []).append(entry)
    for model, entries in manifest.items():
        # newest latest-run first; within the same latest run, shortest gap first
        entries.sort(key=lambda e: (e["latest"], -(e.get("hours") or 0)), reverse=True)
    return dict(sorted(manifest.items()))


def write_manifest(path=MANIFEST_PATH, maps_dir=MAPS_DIR):
    manifest = rebuild_manifest(maps_dir)
    with open(path, "w") as f:
        json.dump(manifest, f, indent=4)
    print(f"[OK] Wrote manifest ({sum(len(v) for v in manifest.values())} GIFs, {len(manifest)} models) to {path}")
    return manifest


if __name__ == "__main__":
    write_manifest()
