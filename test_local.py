"""Local end-to-end test — run without Apify platform.

Usage:
    python -m venv .venv && source .venv/bin/activate
    pip install -r requirements.txt
    python test_local.py
"""

from __future__ import annotations

import asyncio
import json
import logging

from curl_cffi.requests import AsyncSession

from src.models import ScraperInput
from src.scraper import GoogleMapsScraper
from src.utils import IMPERSONATE, RateLimiter

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s %(message)s")
logger = logging.getLogger(__name__)


async def test_search() -> None:
    """Basic search: 3 coffee shops in Austin."""
    logger.info("=== Test 1: Basic search ===")
    config = ScraperInput(
        keywords="coffee shops",
        location="Austin, TX",
        max_results=3,
        max_results_per_search=3,
        max_reviews_per_place=2,
        include_review_sentiment=True,
        enrich_linkedin=True,
    )

    async with AsyncSession(impersonate=IMPERSONATE) as client:
        rate_limiter = RateLimiter(interval=2.0)
        scraper = GoogleMapsScraper(client, rate_limiter, config)

        results = []
        async for place in scraper.scrape():
            results.append(place)
            logger.info(
                f"  [{len(results)}] {place.get('name', 'N/A')} — "
                f"★{place.get('rating', '?')} ({place.get('reviewCount', 0)} reviews)"
            )

        logger.info(f"Total: {len(results)} places")
        if results:
            print("\nFirst result:")
            print(json.dumps(results[0], indent=2, default=str, ensure_ascii=False)[:2000])


async def test_batch() -> None:
    """Batch: 2 keywords × 2 cities = 4 searches."""
    logger.info("=== Test 2: Batch mode ===")
    config = ScraperInput(
        keywords_list=["dentist", "orthodontist"],
        locations_list=["Austin, TX", "Dallas, TX"],
        max_results=8,
        max_results_per_search=2,
        max_reviews_per_place=0,
    )

    async with AsyncSession(impersonate=IMPERSONATE) as client:
        rate_limiter = RateLimiter(interval=2.0)
        scraper = GoogleMapsScraper(client, rate_limiter, config)

        results = []
        async for place in scraper.scrape():
            results.append(place)
            logger.info(
                f"  {place.get('name', 'N/A')} — "
                f"[{place.get('searchKeywords')} @ {place.get('searchLocation')}]"
            )

        logger.info(f"Batch total: {len(results)} unique places across 4 searches")


async def main() -> None:
    await test_search()
    await test_batch()


if __name__ == "__main__":
    asyncio.run(main())
