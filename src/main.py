"""Google Maps Scraper -- Apify Actor entry point.

Pay-per-event pricing:
  result_place     — $0.80 / 1,000 places (base rate, beats compass at $2.10)
  result_review    — $0.40 / 1,000 reviews (cheap review enrichment)
  result_lead      — $1.50 / 1,000 contact-enriched leads

Free tier: 25 results max, reviews disabled, contact enrichment disabled.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone

from apify import Actor
from curl_cffi.requests import AsyncSession

from .models import OutputView, ScraperInput
from .scraper import GoogleMapsScraper
from .utils import IMPERSONATE, BrowserFetcher, RateLimiter

logger = logging.getLogger(__name__)

FREE_TIER_LIMIT = 25


class _SilencePPENotice(logging.Filter):
    """Suppress Apify SDK's 'Ignored attempt to charge' warning.

    The warning fires on every item when the actor's pricing model isn't
    set to PAY_PER_EVENT on the Apify console. For development / free
    tier runs, spamming the log with 25 copies of this message is noise.
    When the operator enables PPE pricing, charges will start working
    automatically — the _charge_place calls stay in place, so we just
    hide the noise from the log without disabling the functionality.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        return "Ignored attempt to charge" not in record.getMessage()


logging.getLogger("apify").addFilter(_SilencePPENotice())

PPE_EVENTS = {
    "place": "result_place",
    "review": "result_review",
    "lead": "result_lead",
}


async def main() -> None:
    """Main actor function."""
    async with Actor:
        # 1. Get and validate input
        raw_input = await Actor.get_input() or {}
        config = ScraperInput.from_actor_input(raw_input)

        validation_error = config.validate_input()
        if validation_error:
            await Actor.fail(status_message=validation_error)
            return

        # 2. Apply free-tier limits when running on Apify
        is_on_apify = os.environ.get("APIFY_IS_AT_HOME") == "1"
        is_paying = is_on_apify and os.environ.get("APIFY_USER_IS_PAYING") == "1"

        if is_on_apify and not is_paying:
            config.max_results = min(config.max_results, FREE_TIER_LIMIT)
            config.max_results_per_search = min(
                config.max_results_per_search, FREE_TIER_LIMIT,
            )
            config.max_reviews_per_place = 0
            config.enrich_contacts = False
            config.enrich_linkedin = False
            Actor.log.info(
                f"Free tier: limited to {FREE_TIER_LIMIT} places "
                "(no reviews, no contact/LinkedIn enrichment). "
                "Subscribe for up to 10,000 results with full enrichment."
            )

        combos = config.get_search_combos()
        batch_mode = len(combos) > 1

        Actor.log.info(
            f"Starting Google Maps Scraper | "
            f"searches={len(combos)} | batch_mode={batch_mode} | "
            f"direct_urls={len(config.place_urls)} | "
            f"reviews={config.max_reviews_per_place} | "
            f"contacts={config.enrich_contacts} | "
            f"linkedin={config.enrich_linkedin} | "
            f"output_view={config.output_view.value} | "
            f"max_results={config.max_results}"
        )

        # 3. Proxy setup. RESIDENTIAL is required — the browser (Playwright)
        # needs HTTPS CONNECT tunneling, which GOOGLE_SERP does NOT support
        # (GOOGLE_SERP is HTTP-only and returns pre-rendered HTML).
        proxy_config = None
        proxy_url = None
        try:
            proxy_config = await Actor.create_proxy_configuration(
                actor_proxy_input=raw_input.get("proxyConfiguration"),
            )
            if proxy_config:
                proxy_url = await proxy_config.new_url()
        except Exception as e:
            Actor.log.warning(f"Failed to create proxy configuration: {e}")

        if not proxy_url and is_on_apify:
            await Actor.fail(
                status_message=(
                    "Proxy required. Enable Apify Proxy with RESIDENTIAL group "
                    "(country US recommended) in Proxy Configuration. "
                    "Do NOT use GOOGLE_SERP — it is HTTP-only and breaks "
                    "browser-based scraping."
                ),
            )
            return
        if not proxy_url:
            Actor.log.warning(
                "No proxy configured. Google may block direct connections. "
                "Continuing for local testing only.",
            )

        # Guard against the common misconfiguration that breaks browser mode:
        # GOOGLE_SERP proxy is HTTP-only and causes ERR_TUNNEL_CONNECTION_FAILED
        # on every Playwright navigation. Fail fast with a clear message.
        proxy_input = raw_input.get("proxyConfiguration") or {}
        requested_groups = proxy_input.get("apifyProxyGroups") or []
        if is_on_apify and "GOOGLE_SERP" in requested_groups:
            await Actor.fail(
                status_message=(
                    "GOOGLE_SERP proxy is not compatible with this actor. "
                    "It only supports plain HTTP requests (no HTTPS tunneling), "
                    "so the headless browser cannot reach Google Maps. "
                    "Switch Proxy Configuration to RESIDENTIAL (country US) "
                    "and re-run."
                ),
            )
            return

        # 4. State persistence (survives migrations)
        state = await Actor.use_state(
            default_value={"scraped": 0, "reviews": 0, "leads": 0, "failed": 0},
        )

        await Actor.set_status_message("Connecting to Google Maps...")

        session_kwargs: dict = {"impersonate": IMPERSONATE}
        if proxy_url:
            session_kwargs["proxies"] = {"https": proxy_url, "http": proxy_url}

        # Browser is used for the search fetch (Google Maps requires JS to
        # populate result data). HTTP via curl_cffi is still used for place
        # detail pages, website enrichment, etc. — those work fine over HTTP.
        browser = BrowserFetcher(proxy_url=proxy_url)
        try:
            await browser.start()
        except Exception as e:
            Actor.log.error(
                f"Failed to start browser: {e}. Falling back to HTTP-only mode "
                "(results are unlikely — Google Maps search requires JS).",
            )
            browser = None

        async with AsyncSession(**session_kwargs) as client:
            rate_limiter = RateLimiter()
            scraper = GoogleMapsScraper(
                client, rate_limiter, config,
                proxy_config=proxy_config,
                browser=browser,
            )

            count = state["scraped"]
            review_count = state["reviews"]
            lead_count = state["leads"]
            batch: list[dict] = []
            batch_size = 5

            try:
                async for item in scraper.scrape():
                    if count >= config.max_results:
                        break

                    # Stamp the timestamp at write time
                    item["scrapedAt"] = datetime.now(timezone.utc).isoformat()

                    # Apify schema validation rejects `null` for typed fields
                    # (e.g. "type": "number"). Omit None values — fields are
                    # optional in the schema so absence is always valid.
                    item = {k: v for k, v in item.items() if v is not None}

                    batch.append(item)
                    count += 1
                    state["scraped"] = count

                    # Count reviews for PPE billing
                    item_reviews = item.get("reviews", [])
                    if isinstance(item_reviews, list) and item_reviews:
                        review_count += len(item_reviews)
                        state["reviews"] = review_count

                    # Count leads (contact-enriched output) for PPE billing
                    if (
                        config.enrich_contacts
                        and (item.get("emails") or item.get("socialProfiles"))
                    ):
                        lead_count += 1
                        state["leads"] = lead_count

                    # Charge PPE events on Apify platform
                    if is_on_apify:
                        await _charge_place(item, config)

                    if len(batch) >= batch_size:
                        await Actor.push_data(batch)
                        batch = []
                        await Actor.set_status_message(
                            f"Scraped {count}/{config.max_results} places "
                            f"({review_count} reviews, {lead_count} leads)",
                        )

                if batch:
                    await Actor.push_data(batch)

            except Exception as e:
                state["failed"] += 1
                error_msg = str(e).lower()
                if "403" in error_msg or "forbidden" in error_msg:
                    Actor.log.error(
                        "Google blocked the request (403). "
                        "Use RESIDENTIAL proxies and lower request rate.",
                    )
                elif "429" in error_msg or "rate" in error_msg:
                    Actor.log.error(
                        "Google rate limited (429). Wait a few minutes and retry.",
                    )
                elif "timeout" in error_msg:
                    Actor.log.error(
                        "Request timed out. Try RESIDENTIAL proxies.",
                    )
                else:
                    Actor.log.error(f"Scraping error: {e}")
                if batch:
                    await Actor.push_data(batch)

        # Close browser now that scraping is done
        if browser is not None:
            await browser.close()

        # 5. Final status
        msg = (
            f"Done. Scraped {count} places, {review_count} reviews, "
            f"{lead_count} contact-enriched leads."
        )
        if state["failed"] > 0:
            msg += f" {state['failed']} errors encountered."
        if is_on_apify and not is_paying and count >= FREE_TIER_LIMIT:
            msg += (
                f" Free tier limit ({FREE_TIER_LIMIT}) reached. "
                "Subscribe for unlimited scraping + full enrichment."
            )

        Actor.log.info(msg)
        await Actor.set_status_message(msg)


async def _charge_place(item: dict, config: ScraperInput) -> None:
    """Charge PPE events for a single item based on the active output view."""
    try:
        # Always charge the base place event
        await Actor.charge(event_name=PPE_EVENTS["place"])

        # Charge per review when reviews are included
        reviews = item.get("reviews", [])
        if isinstance(reviews, list) and reviews:
            for _ in reviews:
                await Actor.charge(event_name=PPE_EVENTS["review"])

        # Charge lead event only when contact enrichment yielded something
        if config.enrich_contacts and (
            item.get("emails") or item.get("socialProfiles")
        ):
            await Actor.charge(event_name=PPE_EVENTS["lead"])
    except Exception as e:
        # Charging is best-effort — never fail the run over billing hiccups
        logger.debug(f"PPE charge skipped: {e}")
