#!/usr/bin/env python3
"""
Fast parallel data collector for congressional witness database.
Uses concurrent requests to speed up collection.
"""

import json
import os
import re
import sqlite3
import sys
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from app.database import get_db, init_db, DB_PATH

CONGRESS_API_KEY = "CONGRESS_API_KEY"
CONGRESS_API_BASE = "https://api.congress.gov/v3"

session = requests.Session()
session.params = {"api_key": CONGRESS_API_KEY, "format": "json"}

# Rate limiting with semaphore
import threading
rate_lock = threading.Lock()
last_request_time = 0
MIN_DELAY = 0.06  # ~16 req/sec, well within 20K/day limit


def rate_limited_get(url, params=None, retries=3):
    """Thread-safe rate-limited GET."""
    global last_request_time
    for attempt in range(retries):
        with rate_lock:
            now = time.time()
            wait = MIN_DELAY - (now - last_request_time)
            if wait > 0:
                time.sleep(wait)
            last_request_time = time.time()

        try:
            resp = session.get(url, params=params, timeout=30)
            if resp.status_code == 429:
                print(f"  Rate limited, waiting 30s...")
                time.sleep(30)
                continue
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.RequestException as e:
            if attempt < retries - 1:
                time.sleep(2)
            else:
                print(f"  Error fetching {url}: {e}")
                return None
    return None


def normalize_name(name):
    """Normalize a witness name for deduplication."""
    name = re.sub(
        r'\b(The Honorable|Hon\.|Dr\.|Mr\.|Mrs\.|Ms\.|Prof\.|Admiral|General|'
        r'Colonel|Major|Captain|Lieutenant|Sergeant|Rear Admiral|Vice Admiral|'
        r'Ambassador|Secretary|Commissioner|Director|Chairman|Chairwoman|'
        r'Senator|Representative|Congressman|Congresswoman)\b',
        '', name, flags=re.IGNORECASE
    ).strip()
    name = re.sub(r'\s+', ' ', name).strip()
    name = re.sub(r',?\s*(Jr\.?|Sr\.?|III|IV|II|Esq\.?|Ph\.?D\.?|M\.?D\.?|J\.?D\.?)$', '', name).strip()
    name = unicodedata.normalize('NFKD', name)
    return name.strip().title()


def parse_name_parts(name):
    normalized = normalize_name(name)
    parts = normalized.split()
    if len(parts) == 0:
        return None, None
    if len(parts) == 1:
        return None, parts[0]
    return parts[0], parts[-1]


def get_all_meeting_ids(congress, chamber):
    """Get all meeting event IDs for a congress/chamber."""
    all_ids = []
    offset = 0
    while True:
        data = rate_limited_get(
            f"{CONGRESS_API_BASE}/committee-meeting/{congress}/{chamber}",
            {"offset": offset, "limit": 250}
        )
        if not data or "committeeMeetings" not in data:
            break
        meetings = data["committeeMeetings"]
        if not meetings:
            break
        for m in meetings:
            eid = m.get("eventId")
            if eid:
                all_ids.append((str(eid), m.get("chamber", chamber).lower()))
        offset += 250
        if len(meetings) < 250:
            break
    return all_ids


def fetch_meeting_detail(congress, chamber, event_id):
    """Fetch a single meeting's details."""
    data = rate_limited_get(
        f"{CONGRESS_API_BASE}/committee-meeting/{congress}/{chamber}/{event_id}"
    )
    if not data:
        return None
    return data.get("committeeMeeting", data)


def collect_all_meetings(congresses):
    """Collect all meetings for given congresses using parallel requests."""
    db = get_db()

    # Get existing event IDs
    existing = set(
        r[0] for r in db.execute("SELECT event_id FROM hearings WHERE event_id IS NOT NULL").fetchall()
    )
    print(f"Already have {len(existing)} meetings in DB")

    total_new_hearings = 0
    total_new_witnesses = 0
    total_new_appearances = 0

    for congress in congresses:
        for chamber in ["house", "senate"]:
            print(f"\n--- Congress {congress} {chamber.title()} ---")

            # Get all meeting IDs
            meeting_ids = get_all_meeting_ids(congress, chamber)
            print(f"  Found {len(meeting_ids)} meetings total")

            # Filter out already-collected
            new_ids = [(eid, ch) for eid, ch in meeting_ids if eid not in existing]
            print(f"  {len(new_ids)} new meetings to collect")

            if not new_ids:
                continue

            # Fetch details in parallel
            results = []
            with ThreadPoolExecutor(max_workers=4) as executor:
                futures = {}
                for eid, ch in new_ids:
                    f = executor.submit(fetch_meeting_detail, congress, ch, eid)
                    futures[f] = (eid, ch)

                done = 0
                for future in as_completed(futures):
                    eid, ch = futures[future]
                    try:
                        detail = future.result()
                        if detail:
                            results.append((eid, ch, detail))
                    except Exception as e:
                        print(f"  Error for {eid}: {e}")
                    done += 1
                    if done % 100 == 0:
                        print(f"  Fetched {done}/{len(new_ids)} meeting details...")

            print(f"  Got {len(results)} meeting details")

            # Insert into database
            for eid, ch, md in results:
                witnesses = md.get("witnesses", [])
                meeting_type = md.get("type", "Hearing")

                # Include hearings and hearing-like meetings
                title = md.get("title", "")
                is_hearing = (
                    meeting_type == "Hearing"
                    or bool(witnesses)
                    or "hearing" in title.lower()
                    or "nomination" in title.lower()
                )
                if not is_hearing:
                    continue

                hearing_date = md.get("date", "")
                if hearing_date:
                    hearing_date = hearing_date[:10]

                loc = md.get("location", {}) or {}

                try:
                    db.execute("""
                        INSERT OR IGNORE INTO hearings
                        (congress, chamber, title, date, hearing_type, location_building,
                         location_room, event_id)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        congress,
                        ch.title(),
                        md.get("title", "Untitled"),
                        hearing_date,
                        meeting_type,
                        loc.get("building"),
                        loc.get("room"),
                        eid
                    ))
                    db.commit()
                except sqlite3.IntegrityError:
                    continue

                hearing_id = db.execute(
                    "SELECT id FROM hearings WHERE event_id = ?", (eid,)
                ).fetchone()
                if not hearing_id:
                    continue
                hearing_id = hearing_id["id"]
                total_new_hearings += 1

                # Link committees
                for comm in md.get("committees", []):
                    code = comm.get("systemCode", "")
                    if code:
                        row = db.execute("SELECT id FROM committees WHERE system_code = ?", (code,)).fetchone()
                        if row:
                            comm_id = row["id"]
                        else:
                            db.execute(
                                "INSERT INTO committees (system_code, name, chamber) VALUES (?, ?, ?)",
                                (code, comm.get("name", ""), ch.title())
                            )
                            db.commit()
                            comm_id = db.execute("SELECT last_insert_rowid()").fetchone()[0]
                        try:
                            db.execute(
                                "INSERT OR IGNORE INTO hearing_committees VALUES (?, ?)",
                                (hearing_id, comm_id)
                            )
                        except sqlite3.IntegrityError:
                            pass

                # Process witnesses
                for w in witnesses:
                    w_name = w.get("name", "").strip()
                    if not w_name:
                        continue

                    w_position = w.get("position", "")
                    w_org = w.get("organization", "")
                    normalized = normalize_name(w_name)
                    first_name, last_name = parse_name_parts(w_name)

                    # Get or create witness
                    row = db.execute(
                        "SELECT id FROM witnesses WHERE normalized_name = ?", (normalized,)
                    ).fetchone()
                    if row:
                        witness_id = row["id"]
                    else:
                        db.execute(
                            "INSERT INTO witnesses (name, normalized_name, first_name, last_name) VALUES (?, ?, ?, ?)",
                            (w_name, normalized, first_name, last_name)
                        )
                        db.commit()
                        witness_id = db.execute("SELECT last_insert_rowid()").fetchone()[0]
                        total_new_witnesses += 1

                    # Add title
                    if w_position:
                        try:
                            db.execute(
                                "INSERT OR IGNORE INTO witness_titles (witness_id, title, organization, start_date) VALUES (?, ?, ?, ?)",
                                (witness_id, w_position, w_org, hearing_date)
                            )
                        except sqlite3.IntegrityError:
                            pass

                    # Get document URLs
                    bio_url = stmt_url = tit_url = None
                    for doc in md.get("witnessDocuments", []):
                        doc_type = doc.get("documentType", "")
                        doc_url = doc.get("url", "")
                        if "Biography" in doc_type:
                            bio_url = doc_url
                        elif "Statement" in doc_type:
                            stmt_url = doc_url
                        elif "Truth" in doc_type:
                            tit_url = doc_url

                    try:
                        db.execute("""
                            INSERT OR IGNORE INTO witness_appearances
                            (witness_id, hearing_id, position, organization,
                             biography_url, statement_url, truth_in_testimony_url)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                        """, (witness_id, hearing_id, w_position, w_org,
                              bio_url, stmt_url, tit_url))
                        total_new_appearances += 1
                    except sqlite3.IntegrityError:
                        pass

                db.commit()

            print(f"  Done: +{total_new_hearings} hearings, +{total_new_witnesses} witnesses")

    # Update aggregate counts
    print("\nUpdating witness appearance counts...")
    db.execute("""
        UPDATE witnesses SET
            appearance_count = (SELECT COUNT(*) FROM witness_appearances WHERE witness_id = witnesses.id),
            first_appearance_date = (
                SELECT MIN(h.date) FROM witness_appearances wa
                JOIN hearings h ON wa.hearing_id = h.id
                WHERE wa.witness_id = witnesses.id
            ),
            last_appearance_date = (
                SELECT MAX(h.date) FROM witness_appearances wa
                JOIN hearings h ON wa.hearing_id = h.id
                WHERE wa.witness_id = witnesses.id
            )
    """)
    db.commit()

    # Print final stats
    stats = {
        "hearings": db.execute("SELECT COUNT(*) FROM hearings").fetchone()[0],
        "witnesses": db.execute("SELECT COUNT(*) FROM witnesses").fetchone()[0],
        "appearances": db.execute("SELECT COUNT(*) FROM witness_appearances").fetchone()[0],
        "titles": db.execute("SELECT COUNT(*) FROM witness_titles").fetchone()[0],
        "committees": db.execute("SELECT COUNT(*) FROM committees").fetchone()[0],
    }
    db.close()

    print(f"\n=== COLLECTION COMPLETE ===")
    for k, v in stats.items():
        print(f"  {k}: {v:,}")


def collect_hearing_transcripts(congresses):
    """Link hearings to GovInfo packages for transcript access."""
    db = get_db()
    count = 0

    for congress in congresses:
        for chamber in ["house", "senate"]:
            print(f"\n--- Linking transcripts: Congress {congress} {chamber.title()} ---")
            offset = 0
            while True:
                data = rate_limited_get(
                    f"{CONGRESS_API_BASE}/hearing/{congress}/{chamber}",
                    {"offset": offset, "limit": 250}
                )
                if not data or "hearings" not in data:
                    break

                hearing_list = data["hearings"]
                if not hearing_list:
                    break

                # Fetch details in parallel
                details_to_fetch = []
                for h in hearing_list:
                    jacket = h.get("jacketNumber")
                    if jacket:
                        details_to_fetch.append((jacket, chamber))

                with ThreadPoolExecutor(max_workers=4) as executor:
                    futures = {}
                    for jacket, ch in details_to_fetch:
                        f = executor.submit(
                            rate_limited_get,
                            f"{CONGRESS_API_BASE}/hearing/{congress}/{ch}/{jacket}"
                        )
                        futures[f] = (jacket, ch)

                    for future in as_completed(futures):
                        jacket, ch = futures[future]
                        detail = future.result()
                        if not detail:
                            continue

                        hd = detail.get("hearing", detail)
                        chamber_code = "shrg" if ch == "senate" else "hhrg"
                        package_id = f"CHRG-{congress}{chamber_code}{jacket}"

                        dates = hd.get("dates", [])
                        hearing_date = dates[0].get("date", "")[:10] if dates else ""

                        formats = hd.get("formats", [])
                        transcript_url = pdf_url = None
                        for fmt in formats:
                            ftype = fmt.get("type", "")
                            if "Formatted Text" in ftype or "HTML" in ftype.upper():
                                transcript_url = fmt.get("url", "")
                            elif "PDF" in ftype.upper():
                                pdf_url = fmt.get("url", "")

                        # Try to match to existing hearing
                        existing = db.execute(
                            "SELECT id FROM hearings WHERE govinfo_package_id = ?",
                            (package_id,)
                        ).fetchone()

                        if existing:
                            continue

                        # Try date+title match
                        title = hd.get("title", "")
                        matched = None
                        if hearing_date and title:
                            matched = db.execute("""
                                SELECT id FROM hearings
                                WHERE congress = ? AND chamber = ?
                                AND date = ? AND govinfo_package_id IS NULL
                                LIMIT 1
                            """, (congress, ch.title(), hearing_date)).fetchone()

                        if matched:
                            db.execute("""
                                UPDATE hearings SET
                                    govinfo_package_id = ?,
                                    transcript_url = ?,
                                    transcript_pdf_url = ?
                                WHERE id = ?
                            """, (package_id, transcript_url, pdf_url, matched["id"]))
                        else:
                            # Create new hearing record
                            try:
                                db.execute("""
                                    INSERT INTO hearings
                                    (congress, chamber, title, date, hearing_type,
                                     govinfo_package_id, transcript_url, transcript_pdf_url)
                                    VALUES (?, ?, ?, ?, 'Hearing', ?, ?, ?)
                                """, (
                                    congress, ch.title(), title, hearing_date,
                                    package_id, transcript_url, pdf_url
                                ))
                            except sqlite3.IntegrityError:
                                pass

                            # Link committees
                            hearing_id = db.execute("SELECT last_insert_rowid()").fetchone()[0]
                            for comm in hd.get("committees", []):
                                code = comm.get("systemCode", "")
                                if code:
                                    row = db.execute("SELECT id FROM committees WHERE system_code = ?", (code,)).fetchone()
                                    if row:
                                        try:
                                            db.execute(
                                                "INSERT OR IGNORE INTO hearing_committees VALUES (?, ?)",
                                                (hearing_id, row["id"])
                                            )
                                        except:
                                            pass

                        db.commit()
                        count += 1

                offset += 250
                if len(hearing_list) < 250:
                    break

            print(f"  Linked {count} transcript records so far")

    db.close()
    print(f"\n=== Linked {count} hearing transcripts ===")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--congress", default="119,118,117,116")
    parser.add_argument("--skip-meetings", action="store_true")
    parser.add_argument("--skip-transcripts", action="store_true")
    args = parser.parse_args()

    congresses = [int(c.strip()) for c in args.congress.split(",")]

    print("=== Fast Congressional Witness Data Collection ===")
    print(f"Congresses: {congresses}")
    print()

    init_db()

    if not args.skip_meetings:
        collect_all_meetings(congresses)

    if not args.skip_transcripts:
        collect_hearing_transcripts(congresses)
