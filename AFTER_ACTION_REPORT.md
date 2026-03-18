# Congressional Witness Database: After-Action Report

## Project Overview

The Congressional Witness Database is a searchable web application that indexes every witness who has testified before House and Senate committees. It was built to solve a practical problem: congressional staff, journalists, and researchers currently have no easy way to answer questions like "Who has served as FAA Administrator and testified before Congress?" or "How many times has this person appeared before the Judiciary Committee?"

The solution is a Flask web application backed by SQLite, with data sourced entirely from the Congress.gov and GovInfo APIs. It is live at port 8095 with a Cloudflare tunnel for public access.

**Final stats:** 6,713 witnesses | 3,298 hearings | 9,812 appearances | 118th–119th Congress

---

## What I Built

### Core Features
- **Full-text search** with abbreviation expansion (e.g., "FAA Administrator" automatically expands to "Federal Aviation Administration Administrator" and finds the right people)
- **Witness profiles** with unique IDs (W-00001), title history, hearing appearances, and links to written statements and biographies
- **Hearing detail pages** with witness lists, committee info, and links to Congress.gov and GovInfo
- **Organization pages** showing all witnesses from a given agency (e.g., GAO's 78 witnesses across 105 hearings)
- **Statistics dashboard** with top organizations, most active committees, and repeat witnesses
- **Committee pages** with hearing history and top witnesses
- **Title tracking** to see who has held a given position over time
- **CSV export** for offline analysis
- **JSON API** with 5 endpoints for programmatic access

### Data Pipeline
- Congress.gov Committee Meeting API for structured witness data
- Congress.gov Hearing API for hearing metadata
- Automated name deduplication with nickname mapping (Bill/William, etc.)
- Organization name normalization (286 variants consolidated)
- Rate limiting with exponential backoff (20,000 requests/day limit)

---

## Process and Obstacles

### 1. Data Collection and API Rate Limits

**Challenge:** The Congress.gov API has a 20,000 request/day limit and occasionally returns 429 (rate limited) responses. Collecting witness data for 3,298 hearings across two Congresses required thousands of API calls.

**What I tried:**
- Initially used ThreadPoolExecutor with 3 workers for parallel collection. Hit rate limits quickly.
- Reduced to 1 worker and added exponential backoff with `Retry-After` header support.
- Built a `fast_collect.py` script that processes hearings in batches, handles interruptions gracefully, and can resume from where it left off.

**Resolution:** Single-threaded collection with 0.3s delays between requests. Collection of 118th Congress House data (2,149 hearings) took approximately 35 minutes. The script runs in the background while I work on other improvements.

### 2. Database Destruction from Bad Deduplication

**Challenge:** An early version of the deduplication script was too aggressive, merging witnesses who shared last names but were clearly different people (e.g., Caroline Miller and David Miller ending up in the same connected component).

**What I tried:**
- First approach: Simple normalized-name matching merged too many false positives.
- Added connected-component analysis to find groups, but this created chains where A matches B and B matches C, even when A and C are clearly different people.

**Resolution:** Rewrote the dedup to require exact last name match PLUS first name match via nickname lookup or prefix matching. Added a conservative threshold (both names must have 2+ parts). The current version found 390 legitimate duplicate groups across 7,131 witnesses. A small number of edge-case false merges remain (documented as a known limitation).

### 3. Organization Name Fragmentation

**Challenge:** The same organization appeared under many variants in the source data:
- "U.S. Government Accountability Office" vs "Government Accountability Office" vs "United States Government Accountability Office (GAO)"
- "U.S. Department of Veterans Affairs" vs "Department of Veterans Affairs"

This made GAO appear to have 22 witnesses when it actually had 78.

**What I tried:**
- First approach: Regex-based normalization in the dedup script caught some variants but missed many.
- The DC agent review correctly identified this as "the single most damaging credibility issue."

**Resolution:** Built a dedicated `normalize_orgs.py` script that:
1. Strips "U.S." / "United States" / "The" prefixes
2. Strips parenthetical abbreviations like "(GAO)"
3. Strips " of the United States" suffixes
4. Normalizes 286 organization name variants across both the `witness_appearances` and `witness_titles` tables

### 4. Search Relevance for Agency+Title Queries

**Challenge:** Searching "FAA Administrator" initially returned 25+ irrelevant results because expanding "FAA" to "Federal Aviation Administration" then searching each word ("Federal", "Aviation", "Administration", "Administrator") individually matched any organization with "Administration" in its name.

**What I tried:**
- First approach: Simple word-by-word AND matching against concatenated title+org fields. Too many false positives.

**Resolution:** Implemented a smart search strategy: when the query contains a recognized abbreviation, the expanded organization name is matched as a phrase against the organization field, while the remaining non-abbreviation words are matched against the title field. "FAA Administrator" now returns exactly 4 relevant people (Jodi Baker, Wayne Heibeck, Bryan Bedford, Chris Rocheleau) and 3 relevant hearings.

### 5. Senate Witness Data Gap

**Challenge:** The Congress.gov Committee Meeting API does not provide structured witness information for Senate hearings. This means roughly 44% of hearings in the database have no witness data.

**What I tried:**
- Confirmed that the Senate hearing API endpoints consistently lack witness arrays in the response.
- Investigated parsing witnesses from GovInfo hearing transcripts, but this requires HTML parsing with complex formatting and was deferred.

**Current status:** Senate hearings are listed with metadata (date, committee, topic) but without witness lists. The About page transparently documents this limitation. Transcript parsing for Senate witnesses is the single most impactful future improvement.

### 6. Name Display with Military Ranks

**Challenge:** Witness names like "Rear Admiral Upper Half Mark Montgomery" were showing the full military rank in the display. The initial `display_name` function only handled simple prefixes.

**Resolution:** Rewrote the display function to handle compound military ranks iteratively:
- "Rear Admiral Upper Half" / "Rear Admiral Lower Half"
- "Chief Master Sergeant" / "Master Chief Petty Officer"
- "Lieutenant General" / "Major General" / "Brigadier General"
- Applied repeatedly to handle multi-level prefixes

### 7. Search False Positives for Name Queries

**Challenge:** Searching "Pete Hegseth" returned unrelated people like "Shanker Singham" (CEO of Competere) because "Pete" partially matched "Competere" in the OR-based title search fallback.

**Resolution:**
1. Skip the OR title fallback when witness name matches are already found (the person was found, no need for loose title matching)
2. When OR fallback is needed, require words to be at least 5 characters long to prevent substring false positives

---

## Team and Methodology

I worked autonomously with a DC agent (a simulated Daniel Schuman persona) providing feedback after each round of improvements. The DC agent reviewed the site from a Capitol Hill staff perspective, checking search relevance, data quality, and practical usability.

### Review Progression:
- **Review 1 (6.5/10):** Identified org name duplication, missing pagination, search relevance issues, homepage inaccuracies
- **Review 2 (7.5/10):** Confirmed fixes for org normalization, search relevance, homepage text. Identified remaining name artifacts, title display issues, about page inaccuracies
- **Review 3:** All previously identified issues addressed. Remaining gaps are structural (Senate data, testimony text extraction)

---

## Current State and Future Work

### What Works Well
- Searching by name, agency abbreviation, topic, or title returns relevant results quickly
- Every witness has a stable unique ID and complete appearance history
- Written statement PDFs are linked for 3,547+ appearances
- The statistics dashboard reveals patterns (top organizations, repeat witnesses, committee activity)
- The site is clean, professional, and responsive

### Known Limitations
1. **Senate witnesses:** Not yet extracted (API limitation)
2. **Testimony text:** PDFs are linked but not searchable
3. **QFRs:** Schema exists but extraction not implemented
4. **Some dedup edge cases:** A small number of name-sharing but different individuals may have been incorrectly merged
5. **117th Congress:** API currently returns errors for this congress

### Recommended Next Steps
1. Parse Senate witness names from GovInfo hearing transcripts
2. Extract testimony text from written statement PDFs for full-text search
3. Add QFR extraction from hearing transcripts
4. Expand to 116th Congress and earlier
5. Add email alerts for new appearances by tracked witnesses

---

## Technical Architecture

```
witness-database/
├── app/
│   ├── web.py          # Flask web app (1,135 lines)
│   └── database.py     # SQLite schema and initialization
├── scripts/
│   ├── fast_collect.py  # Congress.gov API data collector
│   ├── deduplicate.py   # Conservative witness deduplication
│   └── normalize_orgs.py # Organization name normalization
├── templates/           # 12 Jinja2 templates
├── static/style.css     # Professional CSS (630 lines)
└── data/witnesses.db    # SQLite database (~25MB)
```

**Key design decisions:**
- SQLite with FTS5 for full-text search (porter tokenizer, unicode61)
- WAL journal mode for concurrent read/write during collection
- Conservative deduplication: better to have duplicates than false merges
- Cloudflare tunnel for zero-config public access
