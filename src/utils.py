"""Utility functions for rate limiting, retries, HTTP fetching, and geocoding.

HTTP layer uses curl_cffi (libcurl-impersonate) instead of httpx so that our
TLS handshake matches real Chrome. Google's Maps backend inspects the TLS
fingerprint and serves a stripped, result-less response to clients whose
fingerprint doesn't match a real browser — which is what was happening with
plain httpx.
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
from typing import Any

import httpx  # kept for Nominatim geocoding (doesn't need impersonation)
from curl_cffi.requests import AsyncSession

logger = logging.getLogger(__name__)

# Google Maps tolerates faster scraping than LinkedIn/Reddit when using
# residential proxies, but we stay conservative to avoid soft-blocks.
REQUEST_INTERVAL = 3.0

# Retry settings — low ceiling so blocked IPs don't stall the whole run.
MAX_RETRIES = 2
RETRY_BASE_DELAY = 5.0  # seconds

BASE_URL = "https://www.google.com"
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"

# Chrome version curl_cffi will impersonate at the TLS layer
IMPERSONATE = "chrome124"


async def geocode_location(location: str) -> tuple[float, float] | None:
    """Geocode a text location to (lat, lng) using Nominatim (free, no API key).

    Google Maps only serves real search results when the URL includes map
    coordinates (/@lat,lng,zoom); with just a text query it returns the Maps
    home page. Nominatim is rate-limited to 1 req/sec per their policy —
    plenty for our per-location geocoding since the results are cached.
    """
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.get(
                NOMINATIM_URL,
                params={"q": location, "format": "json", "limit": 1},
                headers={
                    "User-Agent": "apify-google-maps-scraper/1.0",
                    "Accept": "application/json",
                },
            )
        if r.status_code != 200:
            logger.warning(f"Nominatim HTTP {r.status_code} for '{location}'")
            return None
        hits = r.json()
        if not hits:
            logger.warning(f"Nominatim returned no match for '{location}'")
            return None
        return float(hits[0]["lat"]), float(hits[0]["lon"])
    except Exception as e:
        logger.warning(f"Geocoding '{location}' failed: {e}")
        return None


class RateLimiter:
    """Async rate limiter enforcing a minimum interval between requests."""

    def __init__(self, interval: float = REQUEST_INTERVAL) -> None:
        self._interval = interval
        self._last_request: float = 0.0
        self._lock = asyncio.Lock()

    async def wait(self) -> None:
        """Block until the next request is allowed."""
        async with self._lock:
            now = asyncio.get_event_loop().time()
            elapsed = now - self._last_request
            if elapsed < self._interval:
                await asyncio.sleep(self._interval - elapsed)
            self._last_request = asyncio.get_event_loop().time()


# Kept minimal: curl_cffi's impersonate mode sets Chrome's User-Agent,
# Accept, Accept-Language, Accept-Encoding, Sec-Ch-Ua-*, and Sec-Fetch-*
# headers matching the impersonated version. Overriding them weakens the
# impersonation, so we only add Referer and Cookie (session-specific).
_CONSENT_COOKIE = (
    "CONSENT=YES+cb.20240101-00-p0.en+FX+667; "
    "SOCS=CAESEwgDEgk0ODE3Nzk3MjQaAmVuIAEaBgiA_LyaBg; "
    "NID=511=placeholder"
)


def get_headers(referer: str = "https://www.google.com/") -> dict[str, str]:
    """Return minimal browser headers; curl_cffi fills the Chrome defaults."""
    return {"Referer": referer, "Cookie": _CONSENT_COOKIE}


def get_api_headers(referer: str = "https://www.google.com/maps") -> dict[str, str]:
    """Headers for XHR calls to Google Maps protobuf endpoints."""
    return {
        "Referer": referer,
        "Cookie": _CONSENT_COOKIE,
        "X-Requested-With": "XMLHttpRequest",
    }


def _make_proxy_session(proxy_url: str | None) -> AsyncSession:
    """Build an AsyncSession configured with Chrome TLS impersonation."""
    kwargs: dict[str, Any] = {"impersonate": IMPERSONATE}
    if proxy_url:
        kwargs["proxies"] = {"https": proxy_url, "http": proxy_url}
    return AsyncSession(**kwargs)


async def fetch_html(
    client: AsyncSession,
    url: str,
    rate_limiter: RateLimiter,
    params: dict[str, Any] | None = None,
    api_request: bool = False,
    proxy_config: Any = None,
    referer: str | None = None,
) -> str | None:
    """Fetch a URL and return response text, with retry/backoff and proxy rotation.

    Args:
        api_request: Use XHR-style headers (for protobuf endpoints) instead of
                     document-navigation headers.
        proxy_config: Apify ProxyConfiguration — when provided, retries request
                     a fresh proxy URL so Google sees a new IP after a block.
        referer: Override the default Referer header (e.g. when paginating).

    Returns the response body as a string, or None if all retries fail.
    """
    default_referer = "https://www.google.com/maps" if api_request else "https://www.google.com/"
    headers_ref = referer or default_referer

    for attempt in range(MAX_RETRIES):
        await rate_limiter.wait()

        active_client: AsyncSession = client
        temp_client: AsyncSession | None = None

        # On retries, rotate to a fresh proxy IP to escape the block.
        if attempt > 0 and proxy_config is not None:
            try:
                new_proxy_url = await proxy_config.new_url()
                temp_client = _make_proxy_session(new_proxy_url)
                active_client = temp_client
                logger.debug(f"Proxy rotated for retry {attempt + 1}/{MAX_RETRIES}")
            except Exception as e:
                logger.warning(f"Proxy rotation failed: {e} — reusing existing client")

        try:
            response = await active_client.get(
                url,
                params=params,
                headers=get_api_headers(headers_ref) if api_request else get_headers(headers_ref),
                timeout=30.0,
            )

            status = response.status_code

            if status == 200:
                final_url = str(response.url)
                if final_url and final_url.split("?")[0] != url.split("?")[0]:
                    logger.info(
                        f"Redirected: {url[:80]} -> {final_url[:120]}"
                    )
                return response.text

            if status in (429, 403):
                delay = RETRY_BASE_DELAY * (2 ** attempt)
                logger.warning(
                    f"Blocked ({status}) on {url[:100]} — "
                    f"rotating proxy, retry in {delay}s ({attempt + 1}/{MAX_RETRIES})"
                )
                await asyncio.sleep(delay)
                continue

            if status == 404:
                logger.warning(f"Not found (404): {url[:100]}")
                return None

            if status == 400:
                logger.info(f"Bad request (400): {url[:100]}")
                return None

            if status >= 500:
                delay = 10.0 * (attempt + 1)
                logger.warning(
                    f"Server error ({status}) — retrying in {delay}s "
                    f"({attempt + 1}/{MAX_RETRIES})"
                )
                await asyncio.sleep(delay)
                continue

            logger.warning(f"Unexpected status {status} on {url[:100]}")
            return None

        except asyncio.TimeoutError:
            delay = 10.0 * (attempt + 1)
            logger.warning(f"Timeout — retrying in {delay}s ({attempt + 1}/{MAX_RETRIES})")
            await asyncio.sleep(delay)
            continue

        except Exception as e:
            delay = 10.0 * (attempt + 1)
            logger.warning(f"Request error {type(e).__name__}: {e} — retrying in {delay}s")
            await asyncio.sleep(delay)
            continue

        finally:
            if temp_client is not None:
                try:
                    await temp_client.close()
                except Exception:
                    pass

    logger.error(f"All {MAX_RETRIES} retries exhausted for {url[:100]}")
    return None


async def fetch_json(
    client: AsyncSession,
    url: str,
    rate_limiter: RateLimiter,
    params: dict[str, Any] | None = None,
    proxy_config: Any = None,
    referer: str | None = None,
) -> Any | None:
    """Fetch a Google Maps protobuf/JSON endpoint and parse the body.

    Google's internal JSON endpoints prefix responses with `)]}'` XSSI guard
    or wrap in `/*""*/` markers — strip both before parsing.
    """
    text = await fetch_html(
        client, url, rate_limiter,
        params=params,
        api_request=True,
        proxy_config=proxy_config,
        referer=referer,
    )
    if text is None:
        return None

    body = text.lstrip()
    if body.startswith(")]}'"):
        body = body[4:].lstrip()
    if body.startswith("/*") and body.endswith("*/"):
        body = body[2:-2].strip('""').strip()

    try:
        return json.loads(body)
    except json.JSONDecodeError as e:
        logger.warning(f"JSON decode failed: {e} — body starts: {body[:80]!r}")
        return None


def safe_get(data: Any, *keys: int | str, default: Any = None) -> Any:
    """Safely walk nested dict/list with integer or string keys."""
    cur = data
    for key in keys:
        try:
            cur = cur[key]
        except (IndexError, KeyError, TypeError):
            return default
        if cur is None:
            return default
    return cur


# Backwards-compat export: older code paths imported USER_AGENTS
USER_AGENTS: list[str] = [
    # Unused now that curl_cffi handles UA via impersonate, but keeping
    # the name exported avoids breakage for any external reference.
]
