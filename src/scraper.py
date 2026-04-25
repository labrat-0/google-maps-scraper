"""Core Google Maps scraping logic.

Pure HTTP approach — parses the `APP_INITIALIZATION_STATE` JSON blob embedded
in `/maps/search/<query>` HTML pages, then fetches per-place HTML for reviews
and full metadata. No browser, no API keys.

Supports:
- Batch search (keywordsList × locationsList)
- Direct place URLs
- Custom geolocation (GeoJSON point/polygon)
- Review extraction with light sentiment analysis
- Contact enrichment (email/socials from place website)
- LinkedIn search URL generation (pairs with labrat011/linkedin-jobs-scraper)
- Global deduplication by placeId + googleCid
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import unicodedata
from collections.abc import AsyncIterator
from typing import Any
from urllib.parse import quote_plus, unquote_plus, urlparse

from bs4 import BeautifulSoup, Tag
from curl_cffi.requests import AsyncSession

from .models import OutputView, ScraperInput
from .utils import (
    BASE_URL,
    BrowserFetcher,
    RateLimiter,
    fetch_html,
    geocode_location,
    safe_get,
    _make_proxy_session,
)

logger = logging.getLogger(__name__)

# --- regex patterns ---
APP_INIT_STATE_RE = re.compile(
    r"window\.APP_INITIALIZATION_STATE\s*=\s*(\[.+?\]);window\.APP_FLAGS",
    re.DOTALL,
)
CID_RE = re.compile(r"0x[0-9a-fA-F]+:0x[0-9a-fA-F]+")
# Modern Google Place ID format, used alongside or instead of legacy FIDs
PLACE_ID_RE = re.compile(r"ChIJ[A-Za-z0-9_-]{20,27}")
PLACE_URL_RE = re.compile(r"/maps/place/[^/?\s\"'<>]+(?:/@[\-\d.,]+[^/?\s\"'<>]*)?(?:/data=[^?\s\"'<>]+)?")
EMAIL_RE = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}")
PHONE_RE = re.compile(r"\+?[\d][\d\s().\-]{7,}")
LAT_LNG_RE = re.compile(r"@(-?\d+\.\d+),(-?\d+\.\d+)")

# Social media domains we look for in contact enrichment
SOCIAL_DOMAINS = {
    "facebook.com": "facebook",
    "fb.com": "facebook",
    "instagram.com": "instagram",
    "twitter.com": "twitter",
    "x.com": "twitter",
    "linkedin.com": "linkedin",
    "youtube.com": "youtube",
    "tiktok.com": "tiktok",
    "pinterest.com": "pinterest",
}


class GoogleMapsScraper:
    """Google Maps scraper using the embedded APP_INITIALIZATION_STATE JSON."""

    def __init__(
        self,
        client: AsyncSession,
        rate_limiter: RateLimiter,
        config: ScraperInput,
        proxy_config: Any = None,
        browser: BrowserFetcher | None = None,
    ) -> None:
        self.client = client
        self.rate_limiter = rate_limiter
        self.config = config
        self.proxy_config = proxy_config
        self.browser = browser
        self._seen_place_ids: set[str] = set()
        self._seen_cids: set[str] = set()
        self._website_cache: dict[str, dict[str, Any]] = {}
        self._geocode_cache: dict[str, tuple[float, float] | None] = {}
        self._session_warmed: bool = False

    # ------------------------------------------------------------------ #
    # Public entry point                                                 #
    # ------------------------------------------------------------------ #

    async def _warmup_session(self) -> None:
        """Prime the HTTP session with real Google cookies for enrichment requests.

        Only runs when browser is absent — the browser handles its own cookies
        via Playwright's context, so HTTP warmup is unnecessary (and slow).
        """
        if self._session_warmed or self.browser is not None:
            self._session_warmed = True
            return
        self._session_warmed = True
        try:
            logger.info("Warming session at https://www.google.com/maps/")
            await self.client.get(
                f"{BASE_URL}/maps/",
                headers={"Referer": "https://www.google.com/"},
                timeout=15.0,
            )
        except Exception as e:
            logger.warning(f"Session warmup failed: {e} — continuing anyway")

    async def scrape(self) -> AsyncIterator[dict[str, Any]]:
        """Main dispatcher — runs all search combos and direct URL fetches."""
        # Warm the session once so Google sets real NID/session cookies
        await self._warmup_session()

        # Mode 1: direct place URLs
        if self.config.place_urls:
            for url in self.config.place_urls:
                async for item in self._scrape_place_url(url, "", ""):
                    yield item

        # Mode 2: search combos
        combos = self.config.get_search_combos()
        if combos:
            logger.info(f"Running {len(combos)} search combination(s)")
            for keywords, location in combos:
                label = f"'{keywords}' @ '{location or 'global'}'"
                logger.info(f"Searching Google Maps: {label}")
                async for item in self._scrape_search(keywords, location):
                    yield item

    # ------------------------------------------------------------------ #
    # Search mode                                                        #
    # ------------------------------------------------------------------ #

    async def _scrape_search(
        self,
        keywords: str,
        location: str,
    ) -> AsyncIterator[dict[str, Any]]:
        """Fetch Google Maps search page and yield places."""
        query = keywords.strip()
        if location:
            query = f"{query} {location}".strip()

        lat, lng, zoom = self._resolve_geolocation(location)

        # Google Maps only returns a real search results page when the URL
        # contains map coordinates. With just text we get the Maps home page.
        # Geocode the location text via Nominatim (free, no API key) so we
        # can build the coordinate-URL format that competitors use.
        if lat is None and location:
            coords = await self._geocode(location)
            if coords:
                lat, lng, zoom = coords[0], coords[1], 13
                logger.info(f"Geocoded '{location}' -> ({lat:.4f}, {lng:.4f})")

        keyword_enc = quote_plus(keywords.strip())
        combined_enc = quote_plus(query)

        # URL variants in order of reliability:
        # 1. Keyword-only path + /@lat,lng,zoom (the format that actually works)
        # 2. Combined path + coords (fallback when keyword alone is too vague)
        # 3. Plain combined path (last-resort — often returns home page)
        url_variants: list[str] = []
        if lat is not None and lng is not None:
            url_variants.append(
                f"{BASE_URL}/maps/search/{keyword_enc}/@{lat},{lng},{zoom}z"
            )
            url_variants.append(
                f"{BASE_URL}/maps/search/{combined_enc}/@{lat},{lng},{zoom}z"
            )
        url_variants.append(f"{BASE_URL}/maps/search/{combined_enc}")
        url_variants.append(f"{BASE_URL}/maps?q={combined_enc}")

        params = {"hl": self.config.language}

        # Fetch via real browser so JS runs and populates search results
        # into the DOM. Iterate URL variants if the first doesn't yield
        # extractable results (Google sometimes treats variants differently).
        html: str | None = None
        state: Any = None
        places: list[dict[str, Any]] = []
        max_attempts = len(url_variants)

        for attempt in range(max_attempts):
            url = url_variants[attempt]
            logger.info(f"Attempt {attempt + 1}/{max_attempts}: {url[:120]}")

            if self.browser is not None:
                # Scroll to load up to the per-search cap; Google caps ~120
                # results per single search so bump target a little over the
                # user's limit to ensure we don't stop at a lazy-load stall.
                scroll_target = min(self.config.max_results_per_search + 5, 120)
                html = await self.browser.fetch(url, scroll_target=scroll_target)

                # If the proxy rejected HTTPS tunneling, every other URL
                # variant will fail identically — bail out immediately.
                if self.browser.proxy_tunnel_failed:
                    logger.error(
                        "Aborting search: proxy does not support HTTPS tunneling. "
                        "Reconfigure with RESIDENTIAL proxy group and re-run."
                    )
                    return
            else:
                # Fallback to HTTP fetch (local testing without browser)
                html = await fetch_html(
                    self.client, url, self.rate_limiter,
                    params=params, proxy_config=self.proxy_config,
                )

            if not html:
                logger.warning(f"No HTML returned for search: {query}")
                continue

            # Structural extraction first — browser-rendered pages often still
            # have APP_INITIALIZATION_STATE populated after JS runs.
            state = self._parse_app_init_state(html)
            places = self._extract_places_from_state(state, html)
            if places:
                break

            # JS feed extraction: Playwright read place cards directly from
            # div[role="feed"] — only actual results, not ads or sidebar panels.
            # Gives name, rating, category, address without any HTTP round-trips.
            if self.browser is not None and self.browser.last_extracted_places:
                js_places = self._parse_js_places(self.browser.last_extracted_places)
                if js_places:
                    places = js_places
                    logger.info(
                        f"JS feed extraction yielded {len(js_places)} places"
                    )
                    break

            # Last-ditch HTML regex on the full rendered DOM — prone to picking
            # up off-location ads; kept only as final safety net.
            html_places = self._fallback_extract_from_html(html)
            if html_places:
                places = html_places
                break

            logger.warning(
                f"0 places extracted on attempt {attempt + 1}/{max_attempts} — "
                "trying next URL variant"
            )

        logger.info(f"Extracted {len(places)} raw place entries for '{query}'")

        _ENRICH_BATCH = 4

        async def _process_candidate(place: dict[str, Any]) -> dict[str, Any]:
            # Detail enrichment (phone, website, hours, images) requires a
            # separate page load per place. Only run it when:
            #   - reviews are requested, OR
            #   - contact enrichment is requested, OR
            #   - place has no name yet (came from bare URL fallback)
            # Feed card data (name, rating, category, address, coords) is
            # already good enough for basic place output without this step.
            needs_detail = (
                not place.get("name")
                or self.config.max_reviews_per_place > 0
                or self.config.enrich_contacts
                or self.config.enrich_details
            )
            if needs_detail and place.get("placeUrl"):
                place = await self._enrich_place_details(place)
            if self.config.enrich_contacts and place.get("website"):
                place = await self._enrich_contacts(place)
            if self.config.enrich_linkedin and place.get("name"):
                place["linkedinSearchUrl"] = self._build_linkedin_search_url(
                    place["name"],
                )
            return place

        # Pre-filter: dedup + annotate, collect all candidates for this page
        candidates: list[tuple[dict[str, Any], str, str]] = []
        for place in places:
            place_id = place.get("placeId") or ""
            cid = place.get("googleCid") or ""
            if place_id and place_id in self._seen_place_ids:
                continue
            if cid and cid in self._seen_cids:
                continue
            # Annotate with which search combo found it
            place["searchKeywords"] = keywords
            place["searchLocation"] = location
            candidates.append((place, place_id, cid))

        # Enrich in parallel batches of _ENRICH_BATCH, yield as each batch lands
        per_search_count = 0
        for batch_start in range(0, len(candidates), _ENRICH_BATCH):
            if per_search_count >= self.config.max_results_per_search:
                break
            batch = candidates[batch_start : batch_start + _ENRICH_BATCH]
            enriched_batch = await asyncio.gather(
                *[_process_candidate(p) for p, _, _ in batch]
            )
            for (_, place_id, cid), enriched in zip(batch, enriched_batch):
                if per_search_count >= self.config.max_results_per_search:
                    break
                # Apply rating / closed filters
                if not self.config.should_include_place(
                    rating=enriched.get("rating"),
                    is_closed=bool(enriched.get("permanentlyClosed")),
                ):
                    continue
                if place_id:
                    self._seen_place_ids.add(place_id)
                if cid:
                    self._seen_cids.add(cid)
                # Apply output view preset
                yield self._apply_output_view(enriched)
                per_search_count += 1

    # ------------------------------------------------------------------ #
    # Direct place URL mode                                              #
    # ------------------------------------------------------------------ #

    async def _scrape_place_url(
        self,
        url: str,
        keywords: str,
        location: str,
    ) -> AsyncIterator[dict[str, Any]]:
        """Fetch a single Google Maps place URL and yield the place."""
        base = {
            "placeUrl": url,
            "name": "",
            "searchKeywords": keywords,
            "searchLocation": location,
        }
        cid_match = CID_RE.search(url)
        if cid_match:
            base["googleCid"] = cid_match.group(0)

        enriched = await self._enrich_place_details(base)

        if self.config.enrich_contacts and enriched.get("website"):
            enriched = await self._enrich_contacts(enriched)
        if self.config.enrich_linkedin and enriched.get("name"):
            enriched["linkedinSearchUrl"] = self._build_linkedin_search_url(
                enriched["name"],
            )

        if self.config.should_include_place(
            rating=enriched.get("rating"),
            is_closed=bool(enriched.get("permanentlyClosed")),
        ):
            place_id = enriched.get("placeId", "")
            cid = enriched.get("googleCid", "")
            if place_id:
                self._seen_place_ids.add(place_id)
            if cid:
                self._seen_cids.add(cid)
            yield self._apply_output_view(enriched)

    # ------------------------------------------------------------------ #
    # APP_INITIALIZATION_STATE parsing                                   #
    # ------------------------------------------------------------------ #

    def _parse_app_init_state(self, html: str) -> Any:
        """Extract and parse the APP_INITIALIZATION_STATE blob from the page."""
        html_lower = html.lower()

        # Detect known non-results pages so the caller can rotate proxy and retry
        if "recaptcha" in html_lower or "unusual traffic" in html_lower:
            logger.warning("Google CAPTCHA detected — will rotate proxy and retry")
            return None
        if "before you continue" in html_lower or "consent.google.com" in html_lower:
            logger.warning("Google consent page detected — will rotate proxy and retry")
            return None
        if 'id="gsr"' in html_lower and "maps" not in html_lower[:2000]:
            # Plain Google Search page rather than Maps
            logger.warning("Got Google Search page instead of Maps — will rotate proxy and retry")
            return None

        title_m = re.search(r"<title>([^<]+)</title>", html, re.IGNORECASE)
        if title_m:
            page_title = title_m.group(1)
            if "maps" not in page_title.lower() and "google" not in page_title.lower():
                logger.warning(f"Unexpected page title: '{page_title[:80]}' — will rotate proxy")
                return None

        # Try primary regex, then a looser pattern, then bracket-balance extraction
        blob: str | None = None
        m = APP_INIT_STATE_RE.search(html)
        if m:
            blob = m.group(1)
        else:
            m2 = re.search(
                r"APP_INITIALIZATION_STATE\s*=\s*(\[.+?\]);(?:window|\s)",
                html, re.DOTALL,
            )
            if m2:
                blob = m2.group(1)
            else:
                pos = html.find("APP_INITIALIZATION_STATE")
                if pos != -1:
                    start = html.find("[", pos)
                    if start != -1:
                        depth, end = 0, start
                        for i in range(start, min(start + 2_000_000, len(html))):
                            ch = html[i]
                            if ch == "[":
                                depth += 1
                            elif ch == "]":
                                depth -= 1
                                if depth == 0:
                                    end = i + 1
                                    break
                        blob = html[start:end] if end > start else None

        if not blob:
            logger.warning(
                "APP_INITIALIZATION_STATE not found — Google returned a non-Maps page. "
                "Enable RESIDENTIAL (US region) proxies in Proxy Configuration."
            )
            logger.warning(f"Page preview: {html[:1000]!r}")
            return None

        try:
            return json.loads(blob)
        except json.JSONDecodeError as e:
            logger.warning(f"Failed to decode APP_INITIALIZATION_STATE: {e}")
            return None

    def _extract_places_from_state(
        self,
        state: Any,
        raw_html: str,
    ) -> list[dict[str, Any]]:
        """Walk the nested state blob and extract place entries.

        Google's search results live inside a nested JSON string. The exact
        path varies by rollout — we try several known locations, then fall
        back to a recursive walk that picks up any list-of-place-records shape.
        """
        places: list[dict[str, Any]] = []

        # Return empty when state is missing — the outer loop will try the
        # JS feed extraction (which reads the actual rendered DOM cards)
        # before falling back to the HTML URL regex.
        if not state:
            return places

        # state[3] is often a list of length 2 where one element is the
        # XSSI-prefixed results JSON. Try direct indices first (matches
        # the shape our diagnostic revealed), then deeper known paths.
        candidate_indices: list[tuple[int, ...]] = [
            (3, 0), (3, 1), (3, 2), (3, 6),
            (3, 0, 2), (0, 2), (6, 2), (0, 0, 2),
        ]

        for idx in candidate_indices:
            raw_results = safe_get(state, *idx)
            parsed = self._parse_xssi_string(raw_results)
            if parsed is None:
                continue

            # Try the many known index paths for the result list
            result_list_candidates: list[Any] = [
                safe_get(parsed, 64),
                safe_get(parsed, 0, 1),
                safe_get(parsed, 0, 0, 1),
                safe_get(parsed, 1, 1),
                safe_get(parsed, 11),   # observed in response with result count at [6]
                safe_get(parsed, 12),
                safe_get(parsed, 1),
            ]
            for rl in result_list_candidates:
                if isinstance(rl, list) and len(rl) > 0:
                    for entry in rl:
                        place = self._entry_to_place(entry)
                        if place and place.get("name"):
                            places.append(place)
                    if places:
                        return places

            # Recursive walk: find any nested list whose entries look like
            # business records (has a reasonable name field)
            recursive = self._recursive_find_places(parsed)
            if recursive:
                return recursive

        # Structural extraction failed for every candidate. Scan the ENTIRE
        # state tree for FIDs — Google embeds place feature IDs (0xhex:0xhex)
        # throughout the response regardless of the outer layout, so this
        # catches places even when indices we know about don't match.
        fid_places = self._extract_places_by_fid_scan(state)
        if fid_places:
            logger.info(
                f"Extracted {len(fid_places)} places via full-state FID scan"
            )
            return fid_places

        # Nothing worked. Don't fallback here — let the outer loop try
        # the JS feed extraction first (which has real card data) before
        # the dumber HTML regex fallback.
        logger.warning(
            f"State parsed but no places found. "
            f"Top-level shape: {self._describe_state(state)}"
        )
        return places

    def _extract_places_by_fid_scan(self, parsed: Any) -> list[dict[str, Any]]:
        """Scan the serialized parsed JSON for Google place identifiers.

        Maps places appear as either legacy FIDs (`0xHEX:0xHEX`) or modern
        Place IDs (`ChIJ...`). When structural extraction fails, we can
        still discover places by these patterns and build minimal records
        that the detail-page fetcher will enrich.
        """
        try:
            blob = json.dumps(parsed)
        except (TypeError, ValueError):
            return []

        fids = list(dict.fromkeys(CID_RE.findall(blob)))  # preserve order, dedupe
        place_ids = list(dict.fromkeys(PLACE_ID_RE.findall(blob)))

        if not fids and not place_ids:
            return []

        places: list[dict[str, Any]] = []
        for fid in fids:
            try:
                cid_decimal = str(int(fid.split(":")[1], 16))
            except (ValueError, IndexError):
                cid_decimal = ""
            place_url = (
                f"{BASE_URL}/maps/place/?q=place_id:{fid}"
                if not cid_decimal
                else f"{BASE_URL}/maps?cid={cid_decimal}"
            )
            places.append({
                "placeId": "",
                "googleCid": fid,
                "name": "",
                "placeUrl": place_url,
            })
        for pid in place_ids:
            places.append({
                "placeId": pid,
                "googleCid": "",
                "name": "",
                "placeUrl": f"{BASE_URL}/maps/place/?q=place_id:{pid}",
            })
        return places

    def _parse_xssi_string(self, raw: Any) -> Any:
        """Parse a Google-style JSON string that may have a )]}' XSSI guard."""
        if not isinstance(raw, str):
            return None
        body = raw.lstrip()
        if body.startswith(")]}'"):
            body = body[4:].lstrip()
        try:
            return json.loads(body)
        except json.JSONDecodeError:
            return None

    def _recursive_find_places(
        self,
        node: Any,
        depth: int = 0,
        max_depth: int = 8,
    ) -> list[dict[str, Any]]:
        """Walk arbitrary nested lists, returning any business-like records found."""
        if depth > max_depth or not isinstance(node, list):
            return []

        # A result list looks like: [[...record...], [...record...], ...]
        # where each record is a list with a name string at index 11 or 2
        if len(node) >= 2 and all(isinstance(e, list) for e in node[:3]):
            found: list[dict[str, Any]] = []
            for entry in node:
                place = self._entry_to_place(entry)
                if place and place.get("name"):
                    found.append(place)
            if len(found) >= 2:  # require at least 2 to avoid false positives
                return found

        # Recurse into children
        for child in node:
            if isinstance(child, list):
                result = self._recursive_find_places(child, depth + 1, max_depth)
                if result:
                    return result
        return []

    def _describe_state(self, state: Any) -> str:
        """One-line structural summary for diagnostic logging."""
        if not isinstance(state, list):
            return f"type={type(state).__name__}"
        parts = [f"len={len(state)}"]
        for i, e in enumerate(state[:8]):
            if isinstance(e, str):
                parts.append(f"[{i}]=str({len(e)}ch)")
            elif isinstance(e, list):
                parts.append(f"[{i}]=list({len(e)})")
            elif e is None:
                parts.append(f"[{i}]=None")
            else:
                parts.append(f"[{i}]={type(e).__name__}")
        # Dump first string found anywhere in the top levels (often reveals
        # whether Google sent a search response, home page, or consent).
        for i, e in enumerate(state[:8]):
            sample = self._find_first_string(e, max_depth=4)
            if sample:
                parts.append(f"[{i}].str={sample[:400]!r}")
                break
        return " ".join(parts)

    def _find_first_string(self, node: Any, max_depth: int = 4) -> str | None:
        """Walk a nested structure, return the first non-trivial string found."""
        if max_depth < 0:
            return None
        if isinstance(node, str) and len(node) > 4:
            return node
        if isinstance(node, list):
            for child in node:
                s = self._find_first_string(child, max_depth - 1)
                if s:
                    return s
        return None

    def _entry_to_place(self, entry: Any) -> dict[str, Any] | None:
        """Convert a raw APP_INIT result entry into a flat place dict."""
        if not isinstance(entry, list):
            return None

        # The business record lives at varying indices depending on Google's
        # rollout — try the common ones then fall back to the entry itself.
        record: Any = None
        for rec_idx in (14, 13, 12, 0):
            candidate = safe_get(entry, rec_idx)
            if isinstance(candidate, list) and len(candidate) > 5:
                record = candidate
                break
        if record is None:
            record = entry

        # Name can appear at several indices; pick the first plausible string
        name: Any = None
        for name_idx in (11, 2, 1, 3):
            candidate = safe_get(record, name_idx)
            if (
                isinstance(candidate, str)
                and 1 < len(candidate) < 200
                and not candidate.startswith("0x")  # skip feature IDs
                and not candidate.startswith("http")  # skip URLs
                and not candidate.startswith("/")
            ):
                name = candidate
                break
        if not name:
            return None

        place_id = safe_get(record, 78) or ""
        cid_raw = safe_get(record, 10) or ""
        address = (
            safe_get(record, 18)
            or safe_get(record, 39)
            or ""
        )
        categories = safe_get(record, 13) or []
        if not isinstance(categories, list):
            categories = []

        rating = safe_get(record, 4, 7)
        review_count = safe_get(record, 4, 8)
        phone = safe_get(record, 178, 0, 0) or safe_get(record, 3, 0)
        website = safe_get(record, 7, 0) or ""
        price_level = safe_get(record, 4, 2)

        lat = safe_get(record, 9, 2)
        lng = safe_get(record, 9, 3)

        opening_hours = self._extract_opening_hours(record)
        thumbnail = safe_get(record, 37, 0, 6, 0) or safe_get(record, 51, 0, 0, 6, 0)

        place_url_path = safe_get(record, 42) or ""
        if place_url_path and not place_url_path.startswith("http"):
            place_url = f"{BASE_URL}{place_url_path}"
        else:
            place_url = place_url_path

        closed_status = safe_get(record, 203, 1, 4, 0)
        permanently_closed = bool(
            closed_status and "permanent" in str(closed_status).lower()
        )
        temporarily_closed = bool(
            closed_status and "temporar" in str(closed_status).lower()
        )

        # CID — try multiple shapes
        google_cid = ""
        if isinstance(cid_raw, list) and len(cid_raw) > 0:
            google_cid = str(cid_raw[0])
        elif isinstance(cid_raw, str):
            google_cid = cid_raw

        return {
            "placeId": place_id if isinstance(place_id, str) else str(place_id or ""),
            "googleCid": google_cid,
            "name": name.strip(),
            "address": address.strip() if isinstance(address, str) else "",
            "categories": [str(c) for c in categories if c],
            "categoryMain": str(categories[0]) if categories else "",
            "rating": float(rating) if isinstance(rating, (int, float)) else None,
            "reviewCount": int(review_count) if isinstance(review_count, (int, float)) else 0,
            "phone": str(phone).strip() if phone else "",
            "website": website.strip() if isinstance(website, str) else "",
            "priceLevel": str(price_level) if isinstance(price_level, int) else (price_level if isinstance(price_level, str) else None),
            "latitude": float(lat) if isinstance(lat, (int, float)) else None,
            "longitude": float(lng) if isinstance(lng, (int, float)) else None,
            "openingHours": opening_hours,
            "thumbnailUrl": thumbnail if isinstance(thumbnail, str) else "",
            "placeUrl": place_url,
            "permanentlyClosed": permanently_closed,
            "temporarilyClosed": temporarily_closed,
            "scrapedAt": "",
        }

    def _extract_opening_hours(self, record: Any) -> dict[str, str]:
        """Extract a simple day→hours dict from the place record."""
        raw = safe_get(record, 34, 1)
        if not isinstance(raw, list):
            return {}

        hours: dict[str, str] = {}
        for day_entry in raw:
            if not isinstance(day_entry, list) or len(day_entry) < 2:
                continue
            day = day_entry[0]
            times = day_entry[1]
            if not isinstance(day, str) or not isinstance(times, list):
                continue
            time_strings = [t for t in times if isinstance(t, str)]
            hours[day] = ", ".join(time_strings) if time_strings else "Closed"
        return hours

    def _fallback_extract_from_html(self, html: str) -> list[dict[str, Any]]:
        """Last-ditch parse — scrape place URLs out of the raw HTML.

        When the APP_INIT schema changes on us, we can still surface results
        by harvesting `/maps/place/...` hrefs and metadata encoded in the URL.
        Name and coordinates are decoded from the URL so places are usable
        without requiring an additional HTTP fetch per result.
        """
        places: list[dict[str, Any]] = []
        seen = set()
        for match in PLACE_URL_RE.finditer(html):
            url_path = match.group(0)
            if url_path in seen:
                continue
            seen.add(url_path)
            full_url = f"{BASE_URL}{url_path}"
            cid_m = CID_RE.search(url_path)

            # Decode place name from URL: /maps/place/Business+Name/@...
            name_m = re.search(r"/maps/place/([^/@?+\s][^/@?]*)", url_path)
            name = unquote_plus(name_m.group(1)).strip() if name_m else ""

            # Decode coordinates from URL: /@lat,lng,zoom
            coord_m = LAT_LNG_RE.search(url_path)
            lat = float(coord_m.group(1)) if coord_m else None
            lng = float(coord_m.group(2)) if coord_m else None

            street_view_url = (
                f"https://www.google.com/maps/@?api=1&map_action=pano"
                f"&viewpoint={lat},{lng}"
                if lat is not None and lng is not None
                else ""
            )
            places.append({
                "placeId": "",
                "googleCid": cid_m.group(0) if cid_m else "",
                "name": name,
                "address": "",
                "categories": [],
                "categoryMain": "",
                "rating": None,
                "reviewCount": 0,
                "phone": "",
                "website": "",
                "priceLevel": None,
                "latitude": lat,
                "longitude": lng,
                "openingHours": {},
                "thumbnailUrl": "",
                "images": [],
                "placeUrl": full_url,
                "streetViewUrl": street_view_url,
                "permanentlyClosed": False,
                "temporarilyClosed": False,
                "scrapedAt": "",
            })
            if len(places) >= 40:  # sane cap for fallback
                break
        logger.info(f"Fallback HTML scan found {len(places)} place URLs")
        return places

    # ------------------------------------------------------------------ #
    # JS feed extraction helpers                                         #
    # ------------------------------------------------------------------ #

    def _parse_js_places(
        self,
        js_cards: list[dict],
    ) -> list[dict[str, Any]]:
        """Convert JS-extracted feed card dicts into standard place dicts."""
        places: list[dict[str, Any]] = []
        seen: set[str] = set()

        for card in js_cards:
            href = card.get("href", "")
            if not href or href in seen:
                continue
            seen.add(href)

            # Name: prefer decoded URL path segment, fall back to first text line
            name_m = re.search(r"/maps/place/([^/@?+\s][^/@?]*)", href)
            if name_m:
                name = unquote_plus(name_m.group(1)).strip()
            else:
                name = (card.get("text", "").split("\n")[0] or "").strip()

            card_fields = self._parse_card_text(
                card.get("text", ""),
                card.get("ariaLabel", ""),
                name,
            )

            full_url = (
                f"{BASE_URL}{href}" if href.startswith("/") else href
            )

            thumbnail = card.get("thumbnail", "") or ""
            lat = card.get("lat")
            lng = card.get("lng")

            # Street View deep link — works without an API key since it just
            # opens Google Maps Street View viewer for the coordinates. No
            # image download, but gives users a direct path to exterior photos.
            street_view_url = (
                f"https://www.google.com/maps/@?api=1&map_action=pano"
                f"&viewpoint={lat},{lng}"
                if lat is not None and lng is not None
                else ""
            )

            places.append({
                "placeId": card.get("placeId", ""),
                "googleCid": card.get("cid", ""),
                "name": name,
                "address": card_fields["address"],
                "categories": card_fields["categories"],
                "categoryMain": card_fields["categoryMain"],
                "rating": card_fields["rating"],
                "reviewCount": card_fields["reviewCount"],
                "phone": "",
                "website": "",
                "priceLevel": card_fields["priceLevel"],
                "latitude": lat,
                "longitude": lng,
                "openingHours": {},
                "thumbnailUrl": thumbnail,
                "images": [thumbnail] if thumbnail else [],
                "placeUrl": full_url,
                "streetViewUrl": street_view_url,
                "permanentlyClosed": False,
                "temporarilyClosed": False,
                "scrapedAt": "",
            })

        return places

    def _parse_card_text(
        self,
        text: str,
        aria_label: str,
        name: str,
    ) -> dict[str, Any]:
        """Parse rating, reviewCount, category, address from card text/aria-label."""
        result: dict[str, Any] = {
            "rating": None,
            "reviewCount": 0,
            "categoryMain": "",
            "categories": [],
            "address": "",
            "priceLevel": None,
        }

        def _to_count(raw: str) -> int:
            """Parse '1,234', '1.2K', '2.5M' into an int. Returns 0 on failure."""
            raw = raw.strip().replace(",", "")
            m = re.match(r"^([\d.]+)\s*([KkMm]?)$", raw)
            if not m:
                return 0
            try:
                num = float(m.group(1))
            except ValueError:
                return 0
            suffix = m.group(2).upper()
            if suffix == "K":
                num *= 1_000
            elif suffix == "M":
                num *= 1_000_000
            return int(num)

        # aria-label is the most structured source. Google's modern format
        # varies: "Name · 4.5 stars 1,234 Reviews · ..." OR "Name, 4.5 (1,234)".
        # Try both rating+count combined and standalone patterns.
        if aria_label:
            m = re.search(r"(\d+\.\d+)\s*stars?", aria_label, re.IGNORECASE)
            if m:
                result["rating"] = float(m.group(1))
            # "1,234 Reviews" / "1.2K reviews" — explicit word
            m = re.search(
                r"([\d,.]+\s*[KkMm]?)\s+reviews?", aria_label, re.IGNORECASE
            )
            if m:
                result["reviewCount"] = _to_count(m.group(1))
            # "4.5 (1,234)" / "4.5 stars (1,234)" — combined parens form,
            # tolerating an optional "stars" word between rating and count.
            if not result["reviewCount"]:
                m = re.search(
                    r"\d+\.\d+\s*(?:stars?\s*)?\(([\d,.]+\s*[KkMm]?)\)",
                    aria_label,
                    re.IGNORECASE,
                )
                if m:
                    result["reviewCount"] = _to_count(m.group(1))

        # Combined "rating(count)" pattern in raw card text BEFORE splitting,
        # since Google often renders both on a single line: "4.8(1,234)".
        # "stars" tolerated optionally between, e.g. "4.5 stars (1,234)".
        if result["rating"] is None or not result["reviewCount"]:
            m = re.search(
                r"(\d+\.\d+)\s*(?:stars?\s*)?\(([\d,.]+\s*[KkMm]?)\)",
                text,
                re.IGNORECASE,
            )
            if m:
                if result["rating"] is None:
                    try:
                        result["rating"] = float(m.group(1))
                    except ValueError:
                        pass
                if not result["reviewCount"]:
                    result["reviewCount"] = _to_count(m.group(2))

        # "1,234 reviews" / "1.2K reviews" anywhere in the raw card text.
        # Same pattern already applied to aria_label above; needed here for
        # cards where the count follows a · separator in the innerText field.
        if not result["reviewCount"]:
            m = re.search(
                r"([\d,.]+\s*[KkMm]?)\s+reviews?", text, re.IGNORECASE
            )
            if m:
                result["reviewCount"] = _to_count(m.group(1))

        # Split on newlines and middle-dot separators Google uses in cards
        lines = [
            l.strip()
            for l in re.split(r"[\n·•]", text)
            if l.strip() and l.strip() != name
        ]

        category_candidates: list[str] = []
        for line in lines:
            line = line.strip("() ")
            if not line:
                continue

            # Rating: bare float "4.5"
            if re.match(r"^\d\.\d$", line):
                if result["rating"] is None:
                    try:
                        result["rating"] = float(line)
                    except ValueError:
                        pass
                continue

            # Review count: bare integer/decimal with optional K/M suffix:
            # "(1,234)", "1,234", "(1.2K)", "2.5M"
            if re.match(r"^\(?[\d,.]+\s*[KkMm]?\)?$", line):
                if not result["reviewCount"]:
                    result["reviewCount"] = _to_count(line.strip("()"))
                continue

            # Price level: dollar signs only
            if re.match(r"^\${1,4}$", line):
                result["priceLevel"] = line
                continue

            # Skip hours/open-closed status lines
            if re.search(
                r"\b(open|closed|closes|opens|hours)\b", line, re.IGNORECASE
            ):
                continue

            if len(line) < 2 or len(line) > 120:
                continue

            # Address: has a digit AND a street-type word
            if re.search(r"\d", line) and re.search(
                r"\b(St|Ave|Blvd|Rd|Dr|Ln|Way|Ct|Hwy|Pkwy|Pl|"
                r"Ter|Cir|Loop|Expy|Fwy|Beltway|NW|NE|SW|SE)\b",
                line,
                re.IGNORECASE,
            ):
                if not result["address"]:
                    result["address"] = line
                continue

            # Category candidate: must start with a letter (filters out
            # addresses, ratings, review counts). Digits inside are OK so
            # categories like "24-hour service" or "3D printing service"
            # aren't dropped.
            if re.match(r"^[A-Za-z]", line):
                category_candidates.append(line)

        if category_candidates:
            result["categoryMain"] = category_candidates[0]
            result["categories"] = category_candidates[:3]

        return result

    # ------------------------------------------------------------------ #
    # Place detail enrichment                                            #
    # ------------------------------------------------------------------ #

    async def _enrich_place_details(
        self,
        place: dict[str, Any],
    ) -> dict[str, Any]:
        """Fill in phone, website, opening hours, images, and reviews.

        Uses the browser (Playwright) when available — the JS extractor reads
        stable data-item-id attributes from the detail panel, giving reliable
        phone/website/hours/images without HTTP timeouts. Falls back to HTTP
        when running without a browser.
        """
        url = place.get("placeUrl", "")
        if not url:
            return place

        html: str | None = None

        if self.browser is not None:
            # Browser path: runs JS on the live page — far more reliable.
            # Only pull outerHTML when reviews are requested (for regex
            # parsing); skipping it shaves ~1-2s per place.
            need_html = self.config.max_reviews_per_place > 0
            detail, html = await self.browser.fetch_place_detail(
                url, need_html=need_html
            )

            if detail.get("phone") and not place.get("phone"):
                place["phone"] = detail["phone"]
            if detail.get("website") and not place.get("website"):
                place["website"] = detail["website"]
            # Detail-page address is canonical (full street + city + ZIP);
            # the feed card only shows a truncated version. Always prefer
            # detail when available rather than falling back to the partial.
            if detail.get("address"):
                place["address"] = detail["address"]
            if detail.get("openingHours"):
                place["openingHours"] = detail["openingHours"]
            # Detail-page images are richer (gallery photos) than the feed
            # thumbnail. Replace rather than ignore — we want the gallery.
            if detail.get("images"):
                place["images"] = detail["images"]

        else:
            # HTTP fallback path (used when browser is unavailable)
            html = await fetch_html(
                self.client, url, self.rate_limiter,
                proxy_config=self.proxy_config,
                timeout=10.0,
            )
            if html:
                state = self._parse_app_init_state(html)
                detail_record = safe_get(state, 3, 6)
                if detail_record is None:
                    detail_record = self._find_place_record_in_state(state)
                if detail_record:
                    enriched = self._entry_to_place([None] * 14 + [detail_record])
                    if enriched:
                        for key, val in enriched.items():
                            if val and not place.get(key):
                                place[key] = val

        # Reviews come from the rendered HTML regardless of path
        if html and self.config.max_reviews_per_place > 0:
            reviews = self._extract_reviews_from_html(
                html, self.config.max_reviews_per_place,
            )
            place["reviews"] = reviews
            if self.config.include_review_sentiment and reviews:
                place["reviewSentiment"] = self._compute_review_sentiment(reviews)

        # Lat/lng from URL data params if still missing
        if place.get("latitude") is None:
            m = re.search(r"!3d(-?\d+\.\d+)", url)
            if m:
                place["latitude"] = float(m.group(1))
            m2 = re.search(r"!4d(-?\d+\.\d+)", url)
            if m2:
                place["longitude"] = float(m2.group(1))

        return place

    def _find_place_record_in_state(self, state: Any) -> Any:
        """Heuristically locate a place record inside the state blob."""
        # Walk a few known paths; return the first list that looks like a record
        candidates = [
            safe_get(state, 3, 6),
            safe_get(state, 3, 2),
            safe_get(state, 6, 0),
        ]
        for c in candidates:
            if isinstance(c, list) and len(c) > 14:
                return c
        return None

    def _extract_reviews_from_html(
        self,
        html: str,
        max_reviews: int,
    ) -> list[dict[str, Any]]:
        """Extract visible review cards from a place detail page."""
        reviews: list[dict[str, Any]] = []

        # Reviews often live in script tags as a JSON array. Pull more than
        # max_reviews so duplicates (the same review appears in both a JSPB
        # block and a structured-data island) can be filtered before slicing.
        review_blocks = re.findall(
            r'\[\\?"(.+?)\\?",\s*\[?\[?\d+,\\?"([1-5]) stars?\\?"',
            html,
        )
        for text, rating_str in review_blocks[: max_reviews * 4]:
            try:
                rating = int(rating_str)
            except ValueError:
                continue
            reviews.append({
                "text": text.replace("\\n", " ").strip(),
                "rating": rating,
            })

        # Fallback: parse aria-label patterns in the DOM
        if not reviews:
            soup = BeautifulSoup(html, "lxml")
            for card in soup.find_all(attrs={"data-review-id": True})[:max_reviews]:
                if not isinstance(card, Tag):
                    continue
                rating_tag = card.find(attrs={"aria-label": re.compile(r"[1-5] star")})
                rating = 0
                if isinstance(rating_tag, Tag):
                    label = rating_tag.get("aria-label")
                    if isinstance(label, str):
                        m = re.search(r"([1-5]) star", label)
                        if m:
                            rating = int(m.group(1))
                text_el = card.find(attrs={"class": re.compile(r"review.*text|MyEned")})
                text = text_el.get_text(strip=True) if isinstance(text_el, Tag) else ""
                if text or rating:
                    reviews.append({"text": text, "rating": rating})

        # Dedupe by text — Google often emits the same review in two places
        # (rendered card + JSON island), and we don't want users charged for
        # duplicates via result_review.
        seen_texts: set[str] = set()
        deduped: list[dict[str, Any]] = []
        for r in reviews:
            txt = r.get("text", "")
            if txt and txt in seen_texts:
                continue
            if txt:
                seen_texts.add(txt)
            deduped.append(r)

        return deduped[:max_reviews]

    def _compute_review_sentiment(
        self,
        reviews: list[dict[str, Any]],
    ) -> dict[str, int]:
        """Simple sentiment count based on star rating — no LLM, no extra cost."""
        positive = sum(1 for r in reviews if r.get("rating", 0) >= 4)
        neutral = sum(1 for r in reviews if r.get("rating", 0) == 3)
        negative = sum(1 for r in reviews if 0 < r.get("rating", 0) <= 2)
        return {
            "positive": positive,
            "neutral": neutral,
            "negative": negative,
            "total": len(reviews),
        }

    # ------------------------------------------------------------------ #
    # Contact enrichment                                                 #
    # ------------------------------------------------------------------ #

    async def _enrich_contacts(self, place: dict[str, Any]) -> dict[str, Any]:
        """Fetch the place's website and harvest email + social profile links."""
        website = place.get("website", "")
        if not website or not website.startswith(("http://", "https://")):
            return place

        if website in self._website_cache:
            for k, v in self._website_cache[website].items():
                if not place.get(k):
                    place[k] = v
            return place

        html = await fetch_html(
            self.client, website, self.rate_limiter,
            proxy_config=self.proxy_config,
        )
        enrichment: dict[str, Any] = {
            "emails": [],
            "socialProfiles": {},
        }

        if html:
            # Also try /contact page for better email hit rate
            soup = BeautifulSoup(html, "lxml")
            text = soup.get_text(" ", strip=True)
            emails = set()
            for m in EMAIL_RE.finditer(text):
                email = m.group(0).lower()
                # Filter out common false positives
                if not any(bad in email for bad in ("@sentry.io", "@example.com", ".png", ".jpg")):
                    emails.add(email)
            enrichment["emails"] = sorted(emails)[:5]

            socials: dict[str, str] = {}
            for a in soup.find_all("a", href=True):
                if not isinstance(a, Tag):
                    continue
                href = a.get("href", "")
                if not isinstance(href, str):
                    continue
                for domain, name in SOCIAL_DOMAINS.items():
                    if domain in href.lower() and name not in socials:
                        socials[name] = href
                        break
            enrichment["socialProfiles"] = socials

        self._website_cache[website] = enrichment
        for k, v in enrichment.items():
            if not place.get(k):
                place[k] = v
        return place

    # ------------------------------------------------------------------ #
    # LinkedIn integration                                               #
    # ------------------------------------------------------------------ #

    def _build_linkedin_search_url(self, company_name: str) -> str:
        """Build a ready-to-use LinkedIn company search URL.

        Feed this into labrat011/linkedin-jobs-scraper's `companyFilter` field
        or use directly for manual research. Unicode characters are first
        decomposed to ASCII (Café -> Cafe, Crème -> Creme) so the slug isn't
        gutted by the non-ASCII strip.
        """
        ascii_name = (
            unicodedata.normalize("NFKD", company_name)
            .encode("ascii", "ignore")
            .decode("ascii")
        )
        slug = re.sub(r"[^a-zA-Z0-9\s]", "", ascii_name).strip()
        slug = re.sub(r"\s+", "%20", slug)
        return f"https://www.linkedin.com/search/results/companies/?keywords={slug}"

    # ------------------------------------------------------------------ #
    # Geolocation resolution                                             #
    # ------------------------------------------------------------------ #

    async def _geocode(self, location: str) -> tuple[float, float] | None:
        """Geocode a location string, cached for the life of the scraper."""
        key = location.strip().lower()
        if not key:
            return None
        if key in self._geocode_cache:
            return self._geocode_cache[key]
        coords = await geocode_location(location)
        self._geocode_cache[key] = coords
        return coords

    def _resolve_geolocation(
        self,
        location: str,
    ) -> tuple[float | None, float | None, int]:
        """Return (lat, lng, zoom) from customGeolocation, else (None, None, 13)."""
        geo = self.config.custom_geolocation
        if isinstance(geo, dict):
            geom = geo.get("geometry", geo)
            gtype = geom.get("type", "")
            coords = geom.get("coordinates")

            if gtype == "Point" and isinstance(coords, list) and len(coords) >= 2:
                # GeoJSON = [lng, lat]
                return float(coords[1]), float(coords[0]), 14

            if gtype == "Polygon" and isinstance(coords, list) and coords:
                ring = coords[0]
                if isinstance(ring, list) and ring:
                    # centroid
                    lats = [pt[1] for pt in ring if isinstance(pt, list) and len(pt) >= 2]
                    lngs = [pt[0] for pt in ring if isinstance(pt, list) and len(pt) >= 2]
                    if lats and lngs:
                        return sum(lats) / len(lats), sum(lngs) / len(lngs), 12

        return None, None, 13

    # ------------------------------------------------------------------ #
    # Output view filtering                                              #
    # ------------------------------------------------------------------ #

    def _apply_output_view(self, place: dict[str, Any]) -> dict[str, Any]:
        """Filter the place dict down to the requested output view preset."""
        view = self.config.output_view

        if view == OutputView.PLACES:
            # Full place record, but drop heavy review text for slim output
            out = dict(place)
            if "reviews" in out and len(out["reviews"]) > 3:
                out["reviews"] = out["reviews"][:3]
            return out

        if view == OutputView.REVIEWS:
            # Flatten: emit the place once per review when reviews exist
            # (caller iterates — we just return a review-focused slice)
            return {
                "placeId": place.get("placeId"),
                "googleCid": place.get("googleCid"),
                "name": place.get("name"),
                "rating": place.get("rating"),
                "reviewCount": place.get("reviewCount"),
                "reviews": place.get("reviews", []),
                "reviewSentiment": place.get("reviewSentiment"),
                "placeUrl": place.get("placeUrl"),
                "searchKeywords": place.get("searchKeywords"),
                "searchLocation": place.get("searchLocation"),
            }

        if view == OutputView.LEADS:
            # Contact-focused slice — ideal for sales / outreach pipelines
            return {
                "placeId": place.get("placeId"),
                "googleCid": place.get("googleCid"),
                "name": place.get("name"),
                "address": place.get("address"),
                "phone": place.get("phone"),
                "website": place.get("website"),
                "emails": place.get("emails", []),
                "socialProfiles": place.get("socialProfiles", {}),
                "linkedinSearchUrl": place.get("linkedinSearchUrl"),
                "categoryMain": place.get("categoryMain"),
                "rating": place.get("rating"),
                "reviewCount": place.get("reviewCount"),
                "latitude": place.get("latitude"),
                "longitude": place.get("longitude"),
                "placeUrl": place.get("placeUrl"),
            }

        return place
