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

    raise RuntimeError(
        f"[{label or url[:80]}] Exhausted {_ATTEMPTS} attempts. Last error: {last_exc}"
    )
