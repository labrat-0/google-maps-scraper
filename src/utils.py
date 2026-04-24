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
from urllib.parse import urlparse

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
# Only consent-bypass cookies — NID is a session-signed Google cookie and
# a fake value flags the request as non-browser. Let curl_cffi's AsyncSession
# acquire a real NID via the warmup request before searching.
_CONSENT_COOKIE = (
    "CONSENT=YES+cb.20240101-00-p0.en+FX+667; "
    "SOCS=CAESEwgDEgk0ODE3Nzk3MjQaAmVuIAEaBgiA_LyaBg"
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
    timeout: float = 30.0,
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
                timeout=timeout,
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


# ------------------------------------------------------------------ #
# Browser-based fetching (Playwright)                                 #
# ------------------------------------------------------------------ #
#
# Google Maps is a pure-JS SPA: the initial HTML response contains only
# a shell (query echo + viewport metadata) with no place data. Results
# are populated into the DOM by JS running XHR calls. HTTP-only clients
# never see those populated results no matter how well they fake a
# browser's TLS, cookies, or headers — nothing triggers the XHRs.
#
# Playwright launches a real Chromium, navigates to the search URL,
# waits for result elements to appear in the DOM, then hands the
# rendered HTML back to our existing regex/FID-scan extractors.


_BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)


def _parse_proxy_for_playwright(proxy_url: str | None) -> dict[str, str] | None:
    """Convert an Apify proxy URL into Playwright's proxy dict shape."""
    if not proxy_url:
        return None
    try:
        u = urlparse(proxy_url)
        server = f"{u.scheme or 'http'}://{u.hostname}:{u.port}"
        return {
            "server": server,
            "username": u.username or "",
            "password": u.password or "",
        }
    except Exception as e:
        logger.warning(f"Failed to parse proxy URL for Playwright: {e}")
        return None


class BrowserFetcher:
    """Playwright-backed HTML fetcher for JS-rendered pages.

    Starts a single Chromium + context and reuses it across fetches.
    A new page is opened per request so navigation state stays clean.
    Call `start()` before first `fetch()` and `close()` at shutdown.
    """

    def __init__(self, proxy_url: str | None = None) -> None:
        self.proxy_url = proxy_url
        self._playwright: Any = None
        self._browser: Any = None
        self._context: Any = None

    async def start(self) -> None:
        """Launch Chromium and create a reusable context."""
        from playwright.async_api import async_playwright

        self._playwright = await async_playwright().start()

        launch_kwargs: dict[str, Any] = {
            "headless": True,
            "args": [
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
            ],
        }
        proxy_cfg = _parse_proxy_for_playwright(self.proxy_url)
        if proxy_cfg:
            launch_kwargs["proxy"] = proxy_cfg

        self._browser = await self._playwright.chromium.launch(**launch_kwargs)
        self._context = await self._browser.new_context(
            user_agent=_BROWSER_UA,
            locale="en-US",
            viewport={"width": 1280, "height": 900},
            extra_http_headers={
                "Accept-Language": "en-US,en;q=0.9",
            },
        )
        # Bypass consent wall upfront instead of waiting for Google to redirect
        await self._context.add_cookies([
            {
                "name": "CONSENT",
                "value": "YES+cb.20240101-00-p0.en+FX+667",
                "domain": ".google.com",
                "path": "/",
            },
            {
                "name": "SOCS",
                "value": "CAESEwgDEgk0ODE3Nzk3MjQaAmVuIAEaBgiA_LyaBg",
                "domain": ".google.com",
                "path": "/",
            },
        ])
        logger.info("Browser context ready (Playwright + Chromium)")

    async def fetch(
        self,
        url: str,
        wait_selector: str = 'a[href*="/maps/place/"]',
        nav_timeout_ms: int = 45000,
        selector_timeout_ms: int = 30000,
        scroll_target: int = 0,
    ) -> str | None:
        """Navigate to URL, wait for results, optionally scroll for more, return DOM.

        Args:
            scroll_target: Number of place URLs to aim for by scrolling the
                results sidebar. Google Maps lazy-loads more places as the
                sidebar scrolls, capped at ~120. 0 disables scrolling.
        """
        if self._context is None:
            logger.error("BrowserFetcher.fetch called before start()")
            return None

        page = await self._context.new_page()

        # Block heavy non-essential resources (images, fonts, media). Google
        # Maps ships ~20MB of these; blocking them cuts navigation time by
        # ~70% without affecting the JS that populates search results.
        async def _route(route: Any) -> None:
            rtype = route.request.resource_type
            if rtype in ("image", "font", "media"):
                try:
                    await route.abort()
                except Exception:
                    await route.continue_()
            else:
                await route.continue_()

        try:
            await page.route("**/*", _route)
        except Exception:
            pass

        try:
            try:
                await page.goto(url, wait_until="commit", timeout=nav_timeout_ms)
            except Exception as e:
                logger.warning(f"Browser goto issue on {url[:80]}: {e}")

            try:
                await page.wait_for_selector(wait_selector, timeout=selector_timeout_ms)
                logger.info(f"Result selector matched on {url[:80]}")
            except Exception:
                logger.info(
                    f"Result selector '{wait_selector}' did not appear within "
                    f"{selector_timeout_ms}ms on {url[:80]} — returning current DOM"
                )

            # Scroll the results sidebar to trigger lazy-loading of more places.
            # This is THE feature that gets us from 7 to 20+ results per search.
            if scroll_target > 0:
                await self._scroll_for_more_results(page, scroll_target)

            try:
                html = await asyncio.wait_for(
                    page.evaluate("document.documentElement.outerHTML"),
                    timeout=10,
                )
                return html
            except Exception as e:
                logger.warning(f"DOM extract failed on {url[:80]}: {e}")
                return None
        except Exception as e:
            logger.warning(f"Browser fetch failed for {url[:80]}: {e}")
            return None
        finally:
            try:
                await page.close()
            except Exception:
                pass

    async def _scroll_for_more_results(
        self,
        page: Any,
        target_count: int,
        max_scrolls: int = 15,
        wait_between_ms: int = 1200,
    ) -> None:
        """Scroll Google Maps' results sidebar until target hit or list is exhausted.

        Google Maps packs results into a scrollable `div[role="feed"]`. Scrolling
        that container to its bottom fires XHRs that append more places to the
        DOM. We stop when any of: target reached, "You've reached the end"
        marker appears, two consecutive scrolls produce no new results, or
        max_scrolls hit (safety).
        """
        feed_js_present = """
            (() => {
                const feed = document.querySelector('div[role="feed"]');
                return feed !== null;
            })()
        """
        try:
            has_feed = await page.evaluate(feed_js_present)
        except Exception:
            has_feed = False

        if not has_feed:
            logger.info("No scrollable feed found — returning first batch only")
            return

        place_count_js = """
            document.querySelectorAll('a[href*="/maps/place/"]').length
        """
        scroll_js = """
            (() => {
                const feed = document.querySelector('div[role="feed"]');
                if (feed) { feed.scrollTop = feed.scrollHeight; return true; }
                return false;
            })()
        """
        end_marker_js = r"""
            Boolean(
                [...document.querySelectorAll('p, span, div')]
                .some(el => /you['’]ve reached the end/i.test(el.textContent || ''))
            )
        """

        last_count = 0
        stuck = 0
        for i in range(max_scrolls):
            try:
                current_count = await page.evaluate(place_count_js)
            except Exception:
                break

            if current_count >= target_count:
                logger.info(
                    f"Scroll: target reached ({current_count}/{target_count}) "
                    f"after {i} scrolls"
                )
                return

            try:
                at_end = await page.evaluate(end_marker_js)
                if at_end:
                    logger.info(
                        f"Scroll: end marker found at {current_count} results"
                    )
                    return
            except Exception:
                pass

            try:
                await page.evaluate(scroll_js)
            except Exception as e:
                logger.debug(f"Scroll evaluate failed: {e}")
                break

            await asyncio.sleep(wait_between_ms / 1000)

            if current_count == last_count:
                stuck += 1
                if stuck >= 2:
                    logger.info(
                        f"Scroll: no new results for 2 rounds — stopping at {current_count}"
                    )
                    return
            else:
                stuck = 0
            last_count = current_count

        try:
            final = await page.evaluate(place_count_js)
            logger.info(f"Scroll: max {max_scrolls} reached at {final} results")
        except Exception:
            pass

    async def close(self) -> None:
        """Tear down browser + playwright."""
        try:
            if self._context is not None:
                await self._context.close()
            if self._browser is not None:
                await self._browser.close()
            if self._playwright is not None:
                await self._playwright.stop()
        except Exception as e:
            logger.debug(f"BrowserFetcher close error: {e}")
        self._context = self._browser = self._playwright = None
