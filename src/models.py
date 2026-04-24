"""Validated input model for the Google Maps Scraper actor."""

from __future__ import annotations

import itertools
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field, field_validator


class OutputView(str, Enum):
    """Pre-filtered output presets."""

    PLACES = "places"
    REVIEWS = "reviews"
    LEADS = "leads"


class ScraperInput(BaseModel):
    """Validated input for the Google Maps Scraper."""

    # Single search (backward-compatible entry points)
    keywords: str = ""
    location: str = ""

    # Batch search — Cartesian product when both lists are provided
    keywords_list: list[str] = Field(default_factory=list)
    locations_list: list[str] = Field(default_factory=list)

    # Direct place ingestion — skip search, go straight to details
    place_urls: list[str] = Field(default_factory=list)

    # Custom geolocation (GeoJSON point / polygon)
    custom_geolocation: dict[str, Any] | None = None

    # Language / region
    language: str = "en"
    country_code: str = "us"

    # Filters
    min_rating: float = 0.0
    include_closed: bool = True

    # Reviews
    max_reviews_per_place: int = 10
    review_language: str = "en"
    include_review_sentiment: bool = True

    # Enrichment
    enrich_contacts: bool = False
    enrich_linkedin: bool = False
    enrich_details: bool = False  # visit each place page for phone/website/hours/images

    # Scraper settings
    max_results: int = 100
    max_results_per_search: int = 100
    output_view: OutputView = OutputView.PLACES

    @field_validator("keywords_list", "locations_list", "place_urls", mode="before")
    @classmethod
    def clean_list(cls, v: list[str] | None) -> list[str]:
        if not v:
            return []
        return [item.strip() for item in v if item and str(item).strip()]

    @field_validator("min_rating", mode="before")
    @classmethod
    def clamp_rating(cls, v: Any) -> float:
        try:
            return max(0.0, min(5.0, float(v)))
        except (TypeError, ValueError):
            return 0.0

    @classmethod
    def from_actor_input(cls, raw: dict[str, Any]) -> ScraperInput:
        """Map Apify camelCase input to Pydantic snake_case fields."""
        # minRating comes as string from the select dropdown, convert to float
        min_rating_str = raw.get("minRating", "0")
        try:
            min_rating = float(min_rating_str)
        except (ValueError, TypeError):
            min_rating = 0.0

        return cls(
            keywords=raw.get("keywords", ""),
            location=raw.get("location", ""),
            keywords_list=raw.get("keywordsList", []),
            locations_list=raw.get("locationsList", []),
            place_urls=raw.get("placeUrls", []),
            custom_geolocation=raw.get("customGeolocation"),
            language=raw.get("language", "en") or "en",
            country_code=raw.get("countryCode", "us") or "us",
            min_rating=min_rating,
            include_closed=raw.get("includeClosed", True),
            max_reviews_per_place=raw.get("maxReviewsPerPlace", 10),
            review_language=raw.get("reviewLanguage", "en") or "en",
            include_review_sentiment=raw.get("includeReviewSentiment", True),
            enrich_contacts=raw.get("enrichContacts", False),
            enrich_linkedin=raw.get("enrichLinkedIn", False),
            enrich_details=raw.get("enrichDetails", False),
            max_results=raw.get("maxResults", 100),
            max_results_per_search=raw.get("maxResultsPerSearch", 100),
            output_view=raw.get("outputView", "places"),
        )

    def validate_input(self) -> str | None:
        """Return an error message if input is invalid."""
        has_keywords = self.keywords or self.keywords_list
        has_location = self.location or self.locations_list or self.custom_geolocation
        has_urls = self.place_urls

        if not has_keywords and not has_urls and not has_location:
            return (
                "At least one input is required: "
                "'keywords' / 'keywordsList', 'placeUrls', or 'customGeolocation'."
            )

        if self.max_results < 1:
            return "maxResults must be at least 1."
        if self.max_results_per_search < 1:
            return "maxResultsPerSearch must be at least 1."

        return None

    def get_search_combos(self) -> list[tuple[str, str]]:
        """Return all (keyword, location) pairs for batch search.

        Uses Cartesian product when both lists are provided; otherwise
        falls back to single keyword/location fields.
        """
        kws = self.keywords_list if self.keywords_list else ([self.keywords] if self.keywords else [])
        locs = self.locations_list if self.locations_list else ([self.location] if self.location else [""])

        if not kws:
            return []
        return list(itertools.product(kws, locs))

    def should_include_place(
        self,
        rating: float | None,
        is_closed: bool,
    ) -> bool:
        """Apply input filters to a candidate place."""
        if self.min_rating > 0 and (rating is None or rating < self.min_rating):
            return False
        if not self.include_closed and is_closed:
            return False
        return True
