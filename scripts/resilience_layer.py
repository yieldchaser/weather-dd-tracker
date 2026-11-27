"""
resilience_layer.py

Shared network resilience utilities for all HTTP calls in the pipeline.

Two tools:
  get_resilient_session() — urllib3-backed requests.Session for streaming GRIB downloads.
  resilient_get()         — Decorrelated Jitter retry for JSON API calls (Open-Meteo, EIA).

Retry policy:
  Transient (retry):    429, 500, 502, 503, 504, ConnectionError, Timeout
  Hard-fail (no retry): 400, 401, 403, 404, 422

Jitter strategy: AWS-style Decorrelated Jitter (Exponential Backoff And Jitter, 2015).
  sleep = min(cap, random.uniform(base, prev_sleep * 3))
  Prevents synchronized retries from multiple callers (thundering herd).
"""

import time
import random
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

_TRANSIENT = frozenset({429, 500, 502, 503, 504})
_HARD_FAIL  = frozenset({400, 401, 403, 404, 422})

_BASE     = 1.0   # seconds — minimum sleep between retries
_CAP      = 60.0  # seconds — maximum sleep (never wait longer)
_ATTEMPTS = 6     # total attempts: 1 initial + 5 retries


def _jitter(prev: float) -> float:
    """AWS decorrelated jitter: uniform(base, prev*3), capped. Breaks synchronized retries."""
    return min(_CAP, random.uniform(_BASE, max(_BASE, prev * 3)))


def get_resilient_session() -> requests.Session:
    """
    Returns a requests.Session with urllib3 Retry for streaming GRIB byte-range downloads.

    Backoff: exponential with factor=1.5, capped internally by urllib3.
    Retries on: 429, 500, 502, 503, 504.
    Hard client errors (4xx) propagate immediately without retry.
    """
    s = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=1.5,
        status_forcelist=list(_TRANSIENT),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


# ── Circuit breaker ──────────────────────────────────────────────────────────
# 2026-11-26/27: api.eia.gov stopped answering. Every grid call retried 6x at
# 30 s, across ISOs and scripts, so each poller run hit its 2 h limit before
# saving any model data, for over a day. After _OPEN_AFTER fully exhausted
# calls to a host, further calls to it fail at once for _OPEN_FOR_S. State is
# kept in a temp file so every script in the same job shares it.
import json as _json
import os as _os
import tempfile as _tempfile
from urllib.parse import urlparse as _urlparse

# Only hosts whose loss must not stall the pipeline AND whose data is optional
# for it. Open-Meteo is excluded on purpose: two timed-out ensemble batches
# opened the breaker for the whole host and silently skipped every later
# ensemble fetch in the job (ECMWF ENS, AIFS ENS, UKMO on 2026-11-27).
_CB_HOSTS = {"api.eia.gov"}
_OPEN_AFTER = 2
_OPEN_FOR_S = 45 * 60
_CB_FILE = _os.path.join(_os.environ.get("RUNNER_TEMP") or _tempfile.gettempdir(), "wdd_circuit.json")


def _cb_load():
    try:
        with open(_CB_FILE) as f:
            return _json.load(f)
    except Exception:
        return {}


def _cb_save(state):
    try:
        with open(_CB_FILE, "w") as f:
            _json.dump(state, f)
    except Exception:
        pass


def _cb_host(url):
    return _urlparse(url).netloc.lower()


def circuit_open(url) -> bool:
    if _cb_host(url) not in _CB_HOSTS:
        return False
    st = _cb_load().get(_cb_host(url))
    return bool(st and st.get("open_until", 0) > time.time())


def _cb_record(url, ok):
    if _cb_host(url) not in _CB_HOSTS:
        return
    state = _cb_load()
    h = _cb_host(url)
    st = state.get(h, {"fails": 0, "open_until": 0})
    if ok:
        st = {"fails": 0, "open_until": 0}
    else:
        st["fails"] = st.get("fails", 0) + 1
        if st["fails"] >= _OPEN_AFTER:
            st["open_until"] = time.time() + _OPEN_FOR_S
            print(f"  [CIRCUIT] {h} failed {st['fails']} calls in a row; skipping it for {_OPEN_FOR_S // 60} min")
    state[h] = st
    _cb_save(state)


def resilient_get(
    url: str,
    *,
    params=None,
    timeout: int = 60,
    label: str = "",
) -> requests.Response:
    """
    HTTP GET with Decorrelated Jitter backoff.

    Returns the Response object on 2xx success.
    Raises requests.HTTPError immediately on hard client errors (400, 404, etc.).
    Raises RuntimeError after exhausting all retry attempts on transient errors.

    Parameters
    ----------
    url     : Request URL
    params  : Query parameters dict (passed directly to requests.get)
    timeout : Socket timeout in seconds (default 60 for slow API endpoints)
    label   : Human-readable identifier for log messages (e.g. "EIA nuclear outages")
    """
    if circuit_open(url):
        raise RuntimeError(f"[{label or url[:80]}] skipped: {_cb_host(url)} circuit open (host not answering)")
    prev_sleep = _BASE
    last_exc: Exception | None = None

    for attempt in range(1, _ATTEMPTS + 1):
        try:
            r = requests.get(url, params=params, timeout=timeout)

            if r.status_code in _HARD_FAIL:
                r.raise_for_status()                  # Propagate immediately — no retry

            if r.status_code in _TRANSIENT:
                raise requests.HTTPError(response=r)  # Treat as transient, trigger retry

            r.raise_for_status()                      # Raise on any other non-2xx
            _cb_record(url, True)
            return r

        except requests.HTTPError as e:
            status = e.response.status_code if e.response is not None else "?"
            if status in _HARD_FAIL:
                raise                                 # Re-raise hard errors without retry
            last_exc = e
        except (requests.ConnectionError, requests.Timeout) as e:
            last_exc = e

        if attempt < _ATTEMPTS:
            sleep = _jitter(prev_sleep)
            prev_sleep = sleep
            tag = label or url[:80]
            print(f"  [RETRY] {tag} — attempt {attempt}/{_ATTEMPTS} failed; sleeping {sleep:.1f}s")
            time.sleep(sleep)

    _cb_record(url, False)
    raise RuntimeError(
        f"[{label or url[:80]}] Exhausted {_ATTEMPTS} attempts. Last error: {last_exc}"
    )
