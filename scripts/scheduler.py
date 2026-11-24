"""
scheduler.py

GitHub's cron trigger is best effort: on busy days scheduled runs start hours
late or not at all (2026-11-22 the 07:00 jobs started at 15:08; 2026-11-23 no
scheduled run fired between 03:53 and 13:00 UTC). This loop makes the pipeline
independent of it.

Every CHECK_EVERY_S it looks at each workflow's slot table. When a slot has
passed and that workflow has no run created since the slot, it dispatches it
(workflow_dispatch events created with GITHUB_TOKEN do start runs). If the
delayed cron fires later anyway, the extra run finds nothing new and exits
without a Telegram message (the poller only messages on new model data).

The job runs for LOOP_SECONDS, then dispatches its own workflow so the chain
continues; a backup cron in scheduler.yml restarts the chain if it ever stops.
"""
import datetime as dt
import json
import os
import sys
import time
import urllib.error
import urllib.request

REPO = os.environ.get("GITHUB_REPOSITORY", "yieldchaser/weather-dd-tracker")
TOKEN = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN")
API = f"https://api.github.com/repos/{REPO}/actions/workflows"

try:
    LOOP_SECONDS = int(float(os.environ.get("LOOP_MINUTES") or 340) * 60)
except ValueError:
    LOOP_SECONDS = 340 * 60
CHECK_EVERY_S = 300
# A slot older than this is not dispatched any more (the next slot covers it).
MAX_LATENESS = dt.timedelta(hours=3)
# Grace period so a cron run that is only a few minutes late is not doubled.
GRACE = dt.timedelta(minutes=10)

# Same slots as each workflow's own cron (UTC "HH:MM").
SLOTS = {
    # Hourly: Open-Meteo serves only each model's newest run, so a cycle that
    # appears and is replaced between two polls is gone for good. Six slots a
    # day (gaps up to 6 h) lost WN3, AIGEFS, CMC, UKMO and WN2 cycles. A poll
    # with nothing new exits in a few minutes.
    "daily_run.yml": [f"{h:02d}:20" for h in range(24)],
    "europe_weather.yml": ["02:40", "08:40", "14:40", "20:40"],
    "teleconnections.yml": ["06:00", "18:00"],
    "system3_freeze.yml": ["06:30", "18:30"],
    "system2_regimes.yml": ["07:00"],
    "system5_wind.yml": ["07:30"],
    "system4_sensitivity.yml": ["08:00"],
    "system_composite.yml": ["08:30"],
}


def _req(method, url, body=None):
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(url, data=data, method=method, headers={
        "Authorization": f"Bearer {TOKEN}",
        "Accept": "application/vnd.github+json",
    })
    with urllib.request.urlopen(r, timeout=30) as resp:
        raw = resp.read()
        return json.loads(raw) if raw else {}


def last_slot(slots, now):
    """Most recent slot time <= now (looks back into yesterday)."""
    best = None
    for day in (now.date() - dt.timedelta(days=1), now.date()):
        for s in slots:
            h, m = map(int, s.split(":"))
            t = dt.datetime(day.year, day.month, day.day, h, m, tzinfo=dt.timezone.utc)
            if t <= now and (best is None or t > best):
                best = t
    return best


def has_run_since(wf, since):
    url = f"{API}/{wf}/runs?per_page=5&created=%3E%3D{since.strftime('%Y-%m-%dT%H:%M:%SZ')}"
    return _req("GET", url).get("total_count", 0) > 0


def dispatch(wf):
    _req("POST", f"{API}/{wf}/dispatches", {"ref": "main"})


def tick(now):
    print(f"{now:%Y-%m-%d %H:%M} tick", flush=True)
    for wf, slots in SLOTS.items():
        slot = last_slot(slots, now)
        if slot is None or now - slot < GRACE or now - slot > MAX_LATENESS:
            continue
        try:
            if has_run_since(wf, slot):
                continue
            dispatch(wf)
            print(f"{now:%H:%M} dispatched {wf} (slot {slot:%H:%M} had no run)", flush=True)
        except (urllib.error.URLError, TimeoutError, ValueError) as e:
            print(f"{now:%H:%M} [WARN] {wf}: {e}", flush=True)


def main():
    if not TOKEN:
        print("[ERR] GH_TOKEN not set")
        return 1
    start = time.time()
    while time.time() - start < LOOP_SECONDS:
        tick(dt.datetime.now(dt.timezone.utc))
        time.sleep(CHECK_EVERY_S)
    try:
        dispatch("scheduler.yml")
        print("Chain continued: scheduler.yml dispatched.", flush=True)
    except (urllib.error.URLError, TimeoutError) as e:
        print(f"[WARN] Could not re-dispatch scheduler ({e}); backup cron will restart it.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
