# Google Maps Scraper — Places, Reviews & Business Leads

Scrape Google Maps places, reviews, and business leads at scale. **No API keys. No login. No browser.** Pure HTTP — fast, cheap, and reliable. Built for lead generation, local SEO research, competitor intelligence, and AI agents.

**Cheaper than [compass/crawler-google-places](https://apify.com/compass/crawler-google-places).** Starts at **$0.80 per 1,000 places** — 62% cheaper than the #1 competitor, with native batch search and LinkedIn lead enrichment built in.

---

## What it does

This actor extracts structured business data from Google Maps' public search pages. It supports single searches, massive batch searches across many keywords and locations in one run, direct place URL ingestion, and GeoJSON polygon targeting. Output is flat JSON, ready to feed into your CRM, lead-gen pipeline, or AI agent.

**Key data extracted:**
- Business name, address, phone, website, categories
- Star rating, review count, price level (`$` to `$$$$`)
- Latitude / longitude coordinates
- Opening hours (day-by-day)
- Permanently / temporarily closed flags
- Review text + star rating (up to 500 per place)
- **Light review sentiment** — positive / neutral / negative counts (no LLM cost)
- **Email + social profile enrichment** (optional) — pulled from the place's website
- **LinkedIn search URL** (optional) — pairs perfectly with [labrat011/linkedin-jobs-scraper](https://apify.com/labrat011/linkedin-jobs-scraper) for full B2B lead pipelines
- Google `placeId` + `googleCid` for cross-referencing with the Places API

---

## Why this scraper

| | This actor | [compass/crawler-google-places](https://apify.com/compass/crawler-google-places) |
|---|---|---|
| **Price per 1k places** | **$0.80** | $2.10 |
| **Price per 1k reviews** | **$0.40** | Bundled / variable |
| **Memory** | **512 MB–1 GB** | Up to 8 GB |
| **Tech** | Pure HTTP (no browser) | Headless Chrome |
| **Batch keywords × locations** | ✅ Native Cartesian product | ⚠️ One search per run |
| **Global deduplication** | ✅ By `placeId` + `googleCid` | Within run only |
| **GeoJSON input** | ✅ Point + Polygon | ✅ Polygon only |
| **Email + social enrichment** | ✅ Optional (`$1.50/1k leads`) | 💰 Paid add-on |
| **LinkedIn integration** | ✅ Ready-to-use search URL | ❌ |
| **Output views** | `places`, `reviews`, `leads` | Single schema |
| **MCP-ready for AI agents** | ✅ | ⚠️ Limited |

---

## Use cases

### 🎯 Lead Generation
Turn Google Maps into a prospect pipeline:
- Find every dentist, coffee shop, plumber, or SaaS company in your target city
- Extract emails and social profile links from their websites (optional)
- Qualify by rating and review count
- Auto-generate LinkedIn search URLs for decision-maker research
- Feed into HubSpot, Salesforce, Clay, or any CRM via Apify integrations

### 📈 Local SEO Research
- Audit every competitor in a geographic area
- Compare ratings, review counts, and category saturation
- Track which businesses dominate the local pack for a given keyword
- Spot gaps and opportunities in underserved neighborhoods

### 🔍 Competitor Intelligence
- Snapshot entire categories (e.g. "all yoga studios in Austin")
- Monitor reviews and sentiment over time (re-run weekly)
- Identify newcomers, closures, and rating trends
- Export to BI tools for dashboarding

### 🤖 AI Agent Integration (MCP)
Use this actor as a live data source for AI agents:
- Ask Claude, GPT, or any MCP-compatible agent to search Google Maps in real time
- Build grounded RAG pipelines with current business data
- Let agents pull restaurant hours, phone numbers, or reviews on demand
- No authentication required — works out of the box with Apify's hosted MCP server

### 📊 Market Research
- Study category density across cities and countries
- Analyze price level distributions, rating distributions, category taxonomies
- Extract phone numbers and websites for outbound campaigns

---

## Key features

- **No API key, login, or cookies** — scrapes public pages only
- **No browser / no Playwright** — pure HTTP, lower cost, faster execution
- **Batch search** — cross-multiply keywords × locations in one run
- **Direct place URL ingestion** — skip search, feed your existing list
- **Custom GeoJSON** — Point or Polygon for precise area targeting
- **Global deduplication** — by `placeId` and `googleCid` across the entire run
- **Review sentiment** — rating-based positive/neutral/negative counts (zero LLM cost)
- **Contact enrichment** — email + social profile scraping from place websites (optional)
- **LinkedIn integration** — auto-generated LinkedIn search URL per result (optional, free)
- **Output views** — `places`, `reviews`, `leads` presets for clean downstream use
- **Rating + closed-status filters** — drop low-quality or inactive places
- **Residential proxy rotation** — fresh IP on 429/403 to survive blocks
- **Resume on migration** — Apify state survives actor migrations mid-run
- **MCP-ready** — works as an AI agent tool via Apify's hosted MCP server

---

## Output format

### Places view (default)

```json
{
  "placeId": "ChIJN1t_tDeuEmsRUsoyG83frY4",
  "googleCid": "0x89c25855c6480299:0x55194eb0b1bb2a3c",
  "name": "Blue Bottle Coffee",
  "address": "2001 E Cesar Chavez St, Austin, TX 78702",
  "categories": ["Coffee shop", "Cafe", "Breakfast restaurant"],
  "categoryMain": "Coffee shop",
  "rating": 4.6,
  "reviewCount": 512,
  "phone": "+1 512-524-1544",
  "website": "https://bluebottlecoffee.com/",
  "priceLevel": "$$",
  "latitude": 30.2592,
  "longitude": -97.7242,
  "openingHours": {
    "Monday": "7:00 AM – 6:00 PM",
    "Tuesday": "7:00 AM – 6:00 PM"
  },
  "thumbnailUrl": "https://lh5.googleusercontent.com/...",
  "placeUrl": "https://www.google.com/maps/place/Blue+Bottle+Coffee/@30.2592,-97.7242,15z",
  "permanentlyClosed": false,
  "temporarilyClosed": false,
  "searchKeywords": "coffee shops",
  "searchLocation": "Austin, TX",
  "reviews": [
    { "rating": 5, "text": "Amazing espresso. Friendly staff." }
  ],
  "reviewSentiment": { "positive": 8, "neutral": 1, "negative": 1, "total": 10 },
  "scrapedAt": "2026-04-22T18:34:00+00:00"
}
```

### Leads view (contact-ready)

```json
{
  "name": "Blue Bottle Coffee",
  "categoryMain": "Coffee shop",
  "phone": "+1 512-524-1544",
  "website": "https://bluebottlecoffee.com/",
  "emails": ["hello@bluebottlecoffee.com"],
  "socialProfiles": {
    "instagram": "https://instagram.com/bluebottle",
    "facebook": "https://facebook.com/bluebottlecoffee"
  },
  "linkedinSearchUrl": "https://www.linkedin.com/search/results/companies/?keywords=Blue%20Bottle%20Coffee",
  "address": "2001 E Cesar Chavez St, Austin, TX 78702",
  "rating": 4.6,
  "reviewCount": 512
}
```

---

## Input reference

| Field | Type | Default | Description |
|---|---|---|---|
| `keywords` | string | — | Single keyword search (e.g. `coffee shops`) |
| `location` | string | — | Single location (e.g. `Austin, TX`) |
| `keywordsList` | string[] | — | Batch keywords (overrides `keywords`) |
| `locationsList` | string[] | — | Batch locations (overrides `location`) |
| `placeUrls` | string[] | — | Skip search — scrape these Google Maps URLs directly |
| `customGeolocation` | object | — | GeoJSON Point or Polygon for precise targeting |
| `language` | select | `en` | Language code (ISO 639-1) |
| `countryCode` | select | `us` | Country code (ISO 3166-1 alpha-2) |
| `minRating` | select | 0 | `0`, `3`, `4`, `5` — drop places below this rating |
| `includeClosed` | boolean | `true` | Include permanently / temporarily closed places |
| `maxReviewsPerPlace` | integer | `10` | 0 to skip reviews entirely |
| `includeReviewSentiment` | boolean | `true` | Adds positive/neutral/negative review counts |
| `enrichContacts` | boolean | `false` | Scrape email + socials from each place's website |
| `enrichLinkedIn` | boolean | `false` | Adds LinkedIn search URL per result |
| `outputView` | select | `places` | `places`, `reviews`, or `leads` |
| `maxResults` | integer | `100` | Total cap across all searches (free: 25, paid: 10,000) |
| `maxResultsPerSearch` | integer | `100` | Cap per keyword × location combo |
| `proxyConfiguration` | object | RESIDENTIAL | Residential strongly recommended |

---

## Batch search example

Scrape three categories across three cities in one run:

```json
{
  "keywordsList": ["coffee shops", "coworking spaces", "yoga studios"],
  "locationsList": ["Austin, TX", "Dallas, TX", "Houston, TX"],
  "maxReviewsPerPlace": 5,
  "minRating": 4,
  "enrichLinkedIn": true,
  "maxResultsPerSearch": 20
}
```

This runs 9 searches (3 × 3), returns up to 180 places, deduplicates by `placeId` + `googleCid` across the whole run, and tags each result with `searchKeywords` + `searchLocation`.

---

## Lead-generation example

Find every high-rated dental practice in Dallas and extract their emails + social profiles:

```json
{
  "keywords": "dentist",
  "location": "Dallas, TX",
  "minRating": 4,
  "enrichContacts": true,
  "enrichLinkedIn": true,
  "outputView": "leads",
  "maxResults": 250
}
```

Every result includes phone, website, scraped emails, Instagram/Facebook/LinkedIn URLs, and a LinkedIn company search URL — pipe directly into Clay, HubSpot, or your cold-email tool.

---

## Pairs perfectly with [labrat011/linkedin-jobs-scraper](https://apify.com/labrat011/linkedin-jobs-scraper)

Run this actor first to get a list of target companies with `enrichLinkedIn: true`, then feed the `linkedinSearchUrl` or business names into `labrat011/linkedin-jobs-scraper`'s `companyFilter` field to pull current hiring managers, recruiters, and employee counts. Full B2B lead pipeline with **zero browser infrastructure**, at a fraction of the typical Clay / ZoomInfo cost.

---

## Pricing (Pay-Per-Event)

| Event | Price | Triggered when |
|---|---|---|
| `result_place` | **$0.0008** ($0.80 / 1k) | Every place returned |
| `result_review` | **$0.0004** ($0.40 / 1k) | Every review attached to a place |
| `result_lead` | **$0.0015** ($1.50 / 1k) | When `enrichContacts` produces an email or social |

You only pay for what you get. No monthly fees. Proxy traffic billed separately (~$12.50/GB for residential, as priced by Apify).

**Free tier:** 25 places per run, no reviews, no contact enrichment. Try before you subscribe.

**Example cost** — 1,000 places with 5 reviews each and contact enrichment yielding leads on 60% of them:
`1,000 × $0.0008 + 5,000 × $0.0004 + 600 × $0.0015 = $0.80 + $2.00 + $0.90 = $3.70`

Compass charges roughly `1,000 × $0.0021 = $2.10` for places only — and extra for reviews and leads.

---

## MCP Integration

Use this actor as a real-time tool for AI agents — no custom MCP server needed.

- **Endpoint:** `https://mcp.apify.com?tools=labrat011/google-maps-scraper`
- **Auth:** `Authorization: Bearer <APIFY_TOKEN>`
- **Transport:** Streamable HTTP
- **Compatible with:** Claude Desktop, Cursor, VS Code, Windsurf, Warp, Gemini CLI

**Claude Desktop / Cursor config:**

```json
{
  "mcpServers": {
    "google-maps-scraper": {
      "url": "https://mcp.apify.com?tools=labrat011/google-maps-scraper",
      "headers": {
        "Authorization": "Bearer <APIFY_TOKEN>"
      }
    }
  }
}
```

Once connected, your AI agent can search Google Maps by keyword + location, pull reviews, extract business contact info, and generate LinkedIn research URLs — all from a natural language prompt.

---

## Proxy guidance

Google aggressively blocks datacenter IPs on Maps. **Residential proxies are strongly recommended.** The actor defaults to Apify's RESIDENTIAL proxy group and will fail on Apify if no proxy is configured — this saves you compute on a run that would never succeed.

With residential proxies, the actor reliably handles runs of thousands of places. On retry, a fresh proxy IP is requested automatically so Google sees a different IP instead of the blocked one.

---

## Timeout & memory guidance

The actor enforces a 3-second delay between requests to stay under Google's soft rate limits. With reviews or contact enrichment enabled, each place requires additional requests, so runtime scales with your result count.

| Max results | Reviews | Contacts | Est. runtime | Recommended timeout |
|---|---|---|---|---|
| 25 (free tier) | 0 | false | ~2 min | 180 s |
| 100 | 10 | false | ~10 min | 900 s |
| 250 | 10 | true | ~40 min | 3000 s |
| 1,000 | 5 | false | ~1 hr | 4500 s |
| 5,000 | 0 | false | ~3 hr | 14400 s |

**Memory:** 512 MB is enough for most runs. Use 1 GB for runs > 500 results or with contact enrichment enabled.

---

## Limitations

- Google Maps caps per-area results at ~120 places — use multiple tight locations or GeoJSON polygons to drill into dense areas
- Review extraction is limited to what Google renders on the place page HTML — typically up to 500 reviews per place, sorted by relevance
- Email / social enrichment depends on the business website linking socials publicly and showing emails in HTML (not hidden behind obfuscation)
- LinkedIn search URLs are built from business names — accuracy depends on the name being unique (a company named "Acme" will return many false matches)
- Contact enrichment adds one request per unique website, cached within the run
- Opening hours follow Google's localized format (day names match the `language` setting)

---

## Legality

This actor only accesses publicly visible Google Maps data — the same information any browser visitor can see. It does not bypass login walls, solve CAPTCHAs, or access private data. Web scraping of publicly available data is generally legal, as established by the **hiQ Labs v. LinkedIn** ruling. However:

- Always check Google's Terms of Service before using scraped data commercially
- GDPR / CCPA apply when scraping personal data — handle emails and phone numbers responsibly
- Do not re-publish scraped content verbatim — use it for research, enrichment, and private internal workflows

You are responsible for your own compliance.

---

## FAQ

### Do I need a Google Maps API key?
No. This actor scrapes public HTML pages and requires no Google credentials.

### Will I get blocked by Google?
Not with residential proxies. Datacenter IPs get blocked within a few requests. The actor defaults to RESIDENTIAL proxy group and rotates IPs automatically on 429/403.

### Why is my run returning fewer results than maxResults?
Google caps per-area results at ~120 places. To get more, use tighter locations (city neighborhoods, ZIP codes) or custom GeoJSON polygons that subdivide large areas.

### How do I scrape only 5-star places?
Set `minRating: 5` in your input. The actor will skip any place rated below 5 stars.

### Can I use this for B2B lead generation?
Yes — enable `enrichContacts` for emails + social profiles, and `enrichLinkedIn` for LinkedIn search URLs. Pair with [labrat011/linkedin-jobs-scraper](https://apify.com/labrat011/linkedin-jobs-scraper) for hiring-manager / recruiter research.

### What's the difference between `placeId` and `googleCid`?
`placeId` is Google's canonical place identifier (what the Places API uses). `googleCid` is an older hex-encoded ID still embedded in Google Maps URLs. Both are included for cross-referencing with other tools.

### Does the actor support pagination beyond 120 results per query?
Not directly — Google enforces a hard cap. Use batch mode (`keywordsList` × `locationsList`) or GeoJSON tiles to cover large areas.

### Can I resume a failed run?
Yes. Apify state survives actor migrations; the actor picks up where it left off. For very large runs, split across multiple smaller jobs for resilience.

---

## Support

Open an issue on the [GitHub repo](https://github.com/labrat-0/google-maps-scraper) or message me through the Apify platform. PRs welcome.
