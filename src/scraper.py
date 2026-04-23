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

import json
import logging
import re
from collections.abc import AsyncIterator
from typing import Any
from urllib.parse import quote_plus, urlparse

import httpx
from bs4 import BeautifulSoup, Tag

from .models import OutputView, ScraperInput
from .utils import BASE_URL, RateLimiter, fetch_html, safe_get

logger = logging.getLogger(__name__)

# --- regex patterns ---
APP_INIT_STATE_RE = re.compile(
    r"window\.APP_INITIALIZATION_STATE\s*=\s*(\[.+?\]);window\.APP_FLAGS",
    re.DOTALL,
)
CID_RE = re.compile(r"0x[0-9a-fA-F]+:0x[0-9a-fA-F]+")
PLACE_URL_RE = re.compile(r"/maps/place/[^/]+/@[\-\d.,]+/data=[^?\"]+")
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
        client: httpx.AsyncClient,
        rate_limiter: RateLimiter,
        config: ScraperInput,
        proxy_config: Any = None,
    ) -> None:
        self.client = client
        self.rate_limiter = rate_limiter
        self.config = config
        self.proxy_config = proxy_config
        self._seen_place_ids: set[str] = set()
        self._seen_cids: set[str] = set()
        self._website_cache: dict[str, dict[str, Any]] = {}

    # ------------------------------------------------------------------ #
    # Public entry point                                                 #
    # ------------------------------------------------------------------ #

    async def scrape(self) -> AsyncIterator[dict[str, Any]]:
        """Main dispatcher — runs all search combos and direct URL fetches."""
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

        # Apply customGeolocation if provided
        lat, lng, zoom = self._resolve_geolocation(location)

        url = f"{BASE_URL}/maps/search/{quote_plus(query)}"
        if lat is not None and lng is not None:
            url += f"/@{lat},{lng},{zoom}z"

        params = {
            "hl": self.config.language,
            "gl": self.config.country_code,
        }

        html = await fetch_html(
            self.client, url, self.rate_limiter,
            params=params,
            proxy_config=self.proxy_config,
        )
        if not html:
            logger.warning(f"No HTML returned for search: {query}")
            return

        state = self._parse_app_init_state(html)
        places = self._extract_places_from_state(state, html)
        logger.info(f"Extracted {len(places)} raw place entries for '{query}'")

        per_search_count = 0
        for place in places:
            if per_search_count >= self.config.max_results_per_search:
                break

            place_id = place.get("placeId") or ""
            cid = place.get("googleCid") or ""
            if place_id and place_id in self._seen_place_ids:
                continue
            if cid and cid in self._seen_cids:
                continue

            # Annotate with which search combo found it
            place["searchKeywords"] = keywords
            place["searchLocation"] = location

            # Enrich with place detail page (reviews, full hours, etc.)
            if place.get("placeUrl"):
                place = await self._enrich_place_details(place)

            # Optional contact enrichment (email/social from website)
            if self.config.enrich_contacts and place.get("website"):
                place = await self._enrich_contacts(place)

            # LinkedIn search URL for lead-gen pipelines
            if self.config.enrich_linkedin and place.get("name"):
                place["linkedinSearchUrl"] = self._build_linkedin_search_url(
                    place["name"],
                )

            # Apply rating / closed filters
            if not self.config.should_include_place(
                rating=place.get("rating"),
                is_closed=bool(place.get("permanentlyClosed")),
            ):
                continue

            if place_id:
                self._seen_place_ids.add(place_id)
            if cid:
                self._seen_cids.add(cid)

            # Apply output view preset
            yield self._apply_output_view(place)
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
        # Debug: check if we got a real Maps page or an error/challenge page
        if "recaptcha" in html.lower() or "unusual traffic" in html.lower():
            logger.error("Google returned a CAPTCHA challenge — proxy required or IP blocked")
            return None
        if "<title>" in html:
            title = re.search(r"<title>([^<]+)</title>", html, re.IGNORECASE)
            if title and "maps" not in title.group(1).lower():
                logger.warning(f"Got non-Maps page: {title.group(1)[:60]}")

        match = APP_INIT_STATE_RE.search(html)
        if not match:
            # Fallback to a looser pattern — the blob sometimes ends differently
            m2 = re.search(
                r"APP_INITIALIZATION_STATE\s*=\s*(\[.+?\]);(?:window|\s)",
                html,
                re.DOTALL,
            )
            if not m2:
                logger.warning(
                    "APP_INITIALIZATION_STATE not found in HTML. "
                    "This usually means Google returned a non-results page (captcha, login, etc.). "
                    "Enable RESIDENTIAL proxies in the Proxy Configuration."
                )
                # Log first 500 chars of HTML for debugging
                logger.debug(f"HTML starts: {html[:500]}")
                return None
            blob = m2.group(1)
        else:
            blob = match.group(1)

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

        Google packs results under `state[3][2]` as a string that itself is a
        JSON array prefixed with `)]}'`. Each entry in that array is a deeply
        nested list where index 14 typically holds the business record.
        """
        places: list[dict[str, Any]] = []

        if not state:
            return self._fallback_extract_from_html(raw_html)

        # The pb-encoded results sit in a string at state[3][2]
        raw_results = safe_get(state, 3, 2)
        if isinstance(raw_results, str):
            body = raw_results.lstrip()
            if body.startswith(")]}'"):
                body = body[4:].lstrip()
            try:
                parsed = json.loads(body)
            except json.JSONDecodeError:
                parsed = None

            if parsed is not None:
                # The results live under parsed[64] (list) or parsed[0][1]
                result_list = safe_get(parsed, 64) or safe_get(parsed, 0, 1)
                if isinstance(result_list, list):
                    for entry in result_list:
                        place = self._entry_to_place(entry)
                        if place:
                            places.append(place)

        if not places:
            places = self._fallback_extract_from_html(raw_html)

        return places

    def _entry_to_place(self, entry: Any) -> dict[str, Any] | None:
        """Convert a raw APP_INIT result entry into a flat place dict."""
        if not isinstance(entry, list):
            return None

        # The business record is conventionally at entry[14]
        record = safe_get(entry, 14)
        if not isinstance(record, list):
            record = entry  # fall back to top level

        name = safe_get(record, 11) or safe_get(record, 2)
        if not name or not isinstance(name, str):
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
            "priceLevel": price_level if isinstance(price_level, (int, str)) else None,
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
        by harvesting `/maps/place/...` hrefs and lat/lng from the HTML and
        enriching each via its detail page.
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
            places.append({
                "placeId": "",
                "googleCid": cid_m.group(0) if cid_m else "",
                "name": "",
                "placeUrl": full_url,
            })
            if len(places) >= 40:  # sane cap for fallback
                break
        logger.info(f"Fallback HTML scan found {len(places)} place URLs")
        return places

    # ------------------------------------------------------------------ #
    # Place detail enrichment                                            #
    # ------------------------------------------------------------------ #

    async def _enrich_place_details(
        self,
        place: dict[str, Any],
    ) -> dict[str, Any]:
        """Fetch the place detail page to fill in missing fields + reviews."""
        url = place.get("placeUrl", "")
        if not url:
            return place

        html = await fetch_html(
            self.client, url, self.rate_limiter,
            proxy_config=self.proxy_config,
        )
        if not html:
            return place

        # Parse APP_INIT from the place page (has richer record)
        state = self._parse_app_init_state(html)
        detail_record = safe_get(state, 3, 6)  # common location for place detail
        if detail_record is None:
            detail_record = self._find_place_record_in_state(state)

        if detail_record:
            enriched_from_state = self._entry_to_place([None] * 14 + [detail_record])
            if enriched_from_state:
                for key, val in enriched_from_state.items():
                    if val and not place.get(key):
                        place[key] = val

        # Reviews from HTML (top N visible)
        if self.config.max_reviews_per_place > 0:
            reviews = self._extract_reviews_from_html(
                html, self.config.max_reviews_per_place,
            )
            place["reviews"] = reviews
            if self.config.include_review_sentiment and reviews:
                place["reviewSentiment"] = self._compute_review_sentiment(reviews)

        # Lat/lng from URL if still missing
        if place.get("latitude") is None:
            m = LAT_LNG_RE.search(url)
            if m:
                place["latitude"] = float(m.group(1))
                place["longitude"] = float(m.group(2))

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

        # Reviews often live in script tags as a JSON array. Look for them.
        review_blocks = re.findall(
            r'\[\\?"(.+?)\\?",\s*\[?\[?\d+,\\?"([1-5]) stars?\\?"',
            html,
        )
        for text, rating_str in review_blocks[:max_reviews]:
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

        return reviews[:max_reviews]

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
        or use directly for manual research.
        """
        slug = re.sub(r"[^a-zA-Z0-9\s]", "", company_name).strip()
        slug = re.sub(r"\s+", "%20", slug)
        return f"https://www.linkedin.com/search/results/companies/?keywords={slug}"

    # ------------------------------------------------------------------ #
    # Geolocation resolution                                             #
    # ------------------------------------------------------------------ #

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
