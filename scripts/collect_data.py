#!/usr/bin/env python3
"""
Collect congressional hearing and witness data from Congress.gov and GovInfo APIs.

Usage:
    python scripts/collect_data.py [--congress 119] [--chamber house|senate] [--limit N]
"""

import argparse
import json
import os
import re
import sqlite3
import sys
import time
import unicodedata
from datetime import datetime

import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from app.database import get_db, init_db, DB_PATH

CONGRESS_API_KEY = "CONGRESS_API_KEY"
CONGRESS_API_BASE = "https://api.congress.gov/v3"
GOVINFO_API_BASE = "https://api.govinfo.gov"

# Rate limiting - 20,000 requests/day = ~14/sec max
CONGRESS_DELAY = 0.08  # seconds between congress.gov requests
GOVINFO_DELAY = 0.1   # seconds between govinfo requests


def normalize_name(name):
    """Normalize a witness name for deduplication."""
    # Remove honorifics
    name = re.sub(
        r'\b(The Honorable|Hon\.|Dr\.|Mr\.|Mrs\.|Ms\.|Prof\.|Admiral|General|'
        r'Colonel|Major|Captain|Lieutenant|Sergeant|Rear Admiral|Vice Admiral|'
        r'Ambassador|Secretary|Commissioner|Director|Chairman|Chairwoman|'
        r'Senator|Representative|Congressman|Congresswoman)\b',
        '', name, flags=re.IGNORECASE
    ).strip()
    # Remove extra whitespace
    name = re.sub(r'\s+', ' ', name).strip()
    # Remove trailing suffixes like Jr., Sr., III, Esq.
    name = re.sub(r',?\s*(Jr\.?|Sr\.?|III|IV|II|Esq\.?|Ph\.?D\.?|M\.?D\.?|J\.?D\.?)$', '', name).strip()
    # Normalize unicode
    name = unicodedata.normalize('NFKD', name)
    return name.strip().title()


def parse_name_parts(name):
    """Extract first and last name from a full name."""
    normalized = normalize_name(name)
    parts = normalized.split()
    if len(parts) == 0:
        return None, None
    if len(parts) == 1:
        return None, parts[0]
    return parts[0], parts[-1]


class CongressAPIClient:
    """Client for the Congress.gov API."""

    def __init__(self):
        self.session = requests.Session()
        self.session.params = {"api_key": CONGRESS_API_KEY, "format": "json"}
        self.last_request = 0

    def _get(self, url, params=None):
        """Make a rate-limited GET request."""
        elapsed = time.time() - self.last_request
        if elapsed < CONGRESS_DELAY:
            time.sleep(CONGRESS_DELAY - elapsed)

        try:
            resp = self.session.get(url, params=params, timeout=30)
            self.last_request = time.time()
            if resp.status_code == 429:
                print("  Rate limited, waiting 60s...")
                time.sleep(60)
                return self._get(url, params)
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.RequestException as e:
            print(f"  Request error: {e}")
            return None

    def get_committee_meetings(self, congress, chamber=None, offset=0, limit=250):
        """Fetch committee meetings list."""
        if chamber:
            url = f"{CONGRESS_API_BASE}/committee-meeting/{congress}/{chamber}"
        else:
            url = f"{CONGRESS_API_BASE}/committee-meeting/{congress}"
        return self._get(url, {"offset": offset, "limit": limit})

    def get_meeting_detail(self, congress, chamber, event_id):
        """Fetch detailed info for a specific committee meeting."""
        url = f"{CONGRESS_API_BASE}/committee-meeting/{congress}/{chamber}/{event_id}"
        return self._get(url)

    def get_hearings(self, congress, chamber=None, offset=0, limit=250):
        """Fetch hearings list."""
        if chamber:
            url = f"{CONGRESS_API_BASE}/hearing/{congress}/{chamber}"
        else:
            url = f"{CONGRESS_API_BASE}/hearing/{congress}"
        return self._get(url, {"offset": offset, "limit": limit})

    def get_hearing_detail(self, congress, chamber, jacket_number):
        """Fetch detailed info for a specific hearing."""
        url = f"{CONGRESS_API_BASE}/hearing/{congress}/{chamber}/{jacket_number}"
        return self._get(url)


class GovInfoClient:
    """Client for the GovInfo API."""

    def __init__(self):
        self.session = requests.Session()
        self.session.params = {"api_key": CONGRESS_API_KEY}
        self.last_request = 0

    def _get(self, url, params=None):
        elapsed = time.time() - self.last_request
        if elapsed < GOVINFO_DELAY:
            time.sleep(GOVINFO_DELAY - elapsed)
        try:
            resp = self.session.get(url, params=params, timeout=60)
            self.last_request = time.time()
            if resp.status_code == 429:
                print("  GovInfo rate limited, waiting 60s...")
                time.sleep(60)
                return self._get(url, params)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            return resp.json() if 'json' in resp.headers.get('content-type', '') else resp.text
        except requests.exceptions.RequestException as e:
            print(f"  GovInfo request error: {e}")
            return None

    def get_hearing_text(self, package_id):
        """Get the full HTML text of a hearing transcript."""
        url = f"{GOVINFO_API_BASE}/packages/{package_id}/htm"
        return self._get(url)

    def search_hearings(self, query, page_size=25, offset_mark="*"):
        """Search hearings via GovInfo search endpoint."""
        url = f"{GOVINFO_API_BASE}/search"
        payload = {
            "query": query,
            "pageSize": page_size,
            "offsetMark": offset_mark,
            "resultLevel": "default"
        }
        try:
            elapsed = time.time() - self.last_request
            if elapsed < GOVINFO_DELAY:
                time.sleep(GOVINFO_DELAY - elapsed)
            resp = self.session.post(url, json=payload, timeout=30)
            self.last_request = time.time()
            if resp.status_code == 429:
                time.sleep(60)
                return self.search_hearings(query, page_size, offset_mark)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            print(f"  GovInfo search error: {e}")
            return None

    def get_package_summary(self, package_id):
        """Get package summary metadata."""
        url = f"{GOVINFO_API_BASE}/packages/{package_id}/summary"
        return self._get(url)


def get_or_create_committee(db, system_code, name, chamber=None, url=None):
    """Get or create a committee record."""
    row = db.execute("SELECT id FROM committees WHERE system_code = ?", (system_code,)).fetchone()
    if row:
        return row["id"]
    db.execute(
        "INSERT INTO committees (system_code, name, chamber, url) VALUES (?, ?, ?, ?)",
        (system_code, name, chamber, url)
    )
    db.commit()
    return db.execute("SELECT last_insert_rowid()").fetchone()[0]


def get_or_create_witness(db, name, position=None, organization=None):
    """Get or create a witness record, with name normalization for deduplication."""
    normalized = normalize_name(name)
    first_name, last_name = parse_name_parts(name)

    # Try exact normalized match
    row = db.execute(
        "SELECT id FROM witnesses WHERE normalized_name = ?", (normalized,)
    ).fetchone()
    if row:
        return row["id"]

    # Try last name + fuzzy match (for "John Smith" vs "John A. Smith")
    if last_name:
        candidates = db.execute(
            "SELECT id, normalized_name FROM witnesses WHERE last_name = ?", (last_name,)
        ).fetchall()
        for c in candidates:
            # Check if names are similar enough
            if first_name and c["normalized_name"].startswith(first_name[:3]):
                return c["id"]

    # Create new witness
    db.execute(
        "INSERT INTO witnesses (name, normalized_name, first_name, last_name) VALUES (?, ?, ?, ?)",
        (name.strip(), normalized, first_name, last_name)
    )
    db.commit()
    return db.execute("SELECT last_insert_rowid()").fetchone()[0]


def add_witness_title(db, witness_id, title, organization=None, date=None):
    """Record a title/position for a witness."""
    if not title:
        return
    try:
        db.execute(
            "INSERT OR IGNORE INTO witness_titles (witness_id, title, organization, start_date) VALUES (?, ?, ?, ?)",
            (witness_id, title, organization, date)
        )
        db.commit()
    except sqlite3.IntegrityError:
        pass


def collect_committee_meetings(congress_list, chamber=None, limit=None):
    """Collect hearing data from Congress.gov committee-meeting endpoint."""
    client = CongressAPIClient()
    db = get_db()

    total_meetings = 0
    total_witnesses = 0

    for congress in congress_list:
        print(f"\n=== Congress {congress} ===")
        offset = 0
        meeting_count = 0

        while True:
            print(f"  Fetching meetings offset={offset}...")
            data = client.get_committee_meetings(congress, chamber, offset=offset, limit=250)
            if not data or "committeeMeetings" not in data:
                break

            meetings = data["committeeMeetings"]
            if not meetings:
                break

            for meeting in meetings:
                if limit and meeting_count >= limit:
                    break

                event_id = meeting.get("eventId")
                if not event_id:
                    continue

                # Check if already collected
                existing = db.execute(
                    "SELECT id FROM hearings WHERE event_id = ?", (str(event_id),)
                ).fetchone()
                if existing:
                    meeting_count += 1
                    continue

                # Get meeting detail
                mchamber = meeting.get("chamber", "").lower()
                if mchamber not in ("house", "senate"):
                    continue

                detail = client.get_meeting_detail(congress, mchamber, event_id)
                if not detail:
                    continue

                md = detail.get("committeeMeeting", detail)

                # Skip non-hearings if they have no witnesses
                meeting_type = md.get("type", "Hearing")
                witnesses = md.get("witnesses", [])

                if not witnesses and meeting_type != "Hearing":
                    meeting_count += 1
                    continue

                # Insert hearing record
                hearing_date = md.get("date", "")
                if hearing_date:
                    hearing_date = hearing_date[:10]  # Just the date part

                loc = md.get("location", {}) or {}

                db.execute("""
                    INSERT OR IGNORE INTO hearings
                    (congress, chamber, title, date, hearing_type, location_building,
                     location_room, event_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    congress,
                    mchamber.title(),
                    md.get("title", "Untitled"),
                    hearing_date,
                    meeting_type,
                    loc.get("building"),
                    loc.get("room"),
                    str(event_id)
                ))
                db.commit()

                hearing_id = db.execute(
                    "SELECT id FROM hearings WHERE event_id = ?", (str(event_id),)
                ).fetchone()["id"]

                # Link committees
                for comm in md.get("committees", []):
                    code = comm.get("systemCode", "")
                    if code:
                        comm_id = get_or_create_committee(
                            db, code, comm.get("name", ""), mchamber.title(),
                            comm.get("url")
                        )
                        try:
                            db.execute(
                                "INSERT OR IGNORE INTO hearing_committees VALUES (?, ?)",
                                (hearing_id, comm_id)
                            )
                            db.commit()
                        except sqlite3.IntegrityError:
                            pass

                # Process witnesses
                for w in witnesses:
                    w_name = w.get("name", "").strip()
                    if not w_name:
                        continue

                    w_position = w.get("position", "")
                    w_org = w.get("organization", "")

                    witness_id = get_or_create_witness(db, w_name, w_position, w_org)
                    add_witness_title(db, witness_id, w_position, w_org, hearing_date)

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
                        db.commit()
                        total_witnesses += 1
                    except sqlite3.IntegrityError:
                        pass

                meeting_count += 1
                total_meetings += 1

                if meeting_count % 25 == 0:
                    print(f"  Processed {meeting_count} meetings, {total_witnesses} witness appearances so far")

            if limit and meeting_count >= limit:
                break

            offset += 250
            if len(meetings) < 250:
                break

        print(f"  Congress {congress}: {meeting_count} meetings processed")

    # Update witness appearance counts
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

    print(f"\n=== TOTAL: {total_meetings} meetings, {total_witnesses} witness appearances ===")
    db.close()


def find_govinfo_package(congress, chamber, jacket_number):
    """Construct a GovInfo package ID from Congress.gov hearing data."""
    chamber_code = "shrg" if chamber.lower() == "senate" else "hhrg"
    return f"CHRG-{congress}{chamber_code}{jacket_number}"


def collect_hearing_transcripts(congress_list, limit=None):
    """Match hearings to GovInfo packages and fetch transcript text."""
    client = CongressAPIClient()
    govinfo = GovInfoClient()
    db = get_db()

    count = 0
    for congress in congress_list:
        print(f"\n=== Collecting transcripts for Congress {congress} ===")

        # Get hearings from Congress.gov that have jacket numbers (published transcripts)
        offset = 0
        while True:
            data = client.get_hearings(congress, offset=offset, limit=250)
            if not data or "hearings" not in data:
                break

            for h in data["hearings"]:
                if limit and count >= limit:
                    break

                jacket = h.get("jacketNumber")
                hchamber = h.get("chamber", "").lower()
                if not jacket or not hchamber:
                    continue

                package_id = find_govinfo_package(congress, hchamber, jacket)

                # Check if we already have this hearing with a transcript
                existing = db.execute(
                    "SELECT id, has_transcript FROM hearings WHERE govinfo_package_id = ?",
                    (package_id,)
                ).fetchone()
                if existing and existing["has_transcript"]:
                    count += 1
                    continue

                # Get hearing detail from congress.gov for metadata
                detail = client.get_hearing_detail(congress, hchamber, jacket)
                if not detail:
                    continue
                hd = detail.get("hearing", detail)

                # Extract dates
                dates = hd.get("dates", [])
                hearing_date = dates[0].get("date", "")[:10] if dates else ""

                # Extract transcript URLs
                formats = hd.get("formats", [])
                transcript_url = None
                pdf_url = None
                for fmt in formats:
                    ftype = fmt.get("type", "")
                    if "HTML" in ftype.upper() or "Formatted Text" in ftype:
                        transcript_url = fmt.get("url", "")
                    elif "PDF" in ftype.upper():
                        pdf_url = fmt.get("url", "")

                # Try to match with existing hearing record by title/date
                hearing_id = None
                if existing:
                    hearing_id = existing["id"]
                else:
                    title = hd.get("title", "")
                    # Try to find matching meeting record
                    if hearing_date and title:
                        match = db.execute(
                            """SELECT id FROM hearings
                            WHERE congress = ? AND chamber = ? AND date = ?
                            AND title LIKE ?
                            LIMIT 1""",
                            (congress, hchamber.title(), hearing_date,
                             f"%{title[:50]}%")
                        ).fetchone()
                        if match:
                            hearing_id = match["id"]

                if hearing_id:
                    # Update existing record with GovInfo info
                    db.execute("""
                        UPDATE hearings SET
                            govinfo_package_id = ?,
                            transcript_url = ?,
                            transcript_pdf_url = ?
                        WHERE id = ?
                    """, (package_id, transcript_url, pdf_url, hearing_id))
                else:
                    # Create new hearing record
                    committees = hd.get("committees", [])
                    db.execute("""
                        INSERT INTO hearings
                        (congress, chamber, title, date, hearing_type,
                         govinfo_package_id, transcript_url, transcript_pdf_url)
                        VALUES (?, ?, ?, ?, 'Hearing', ?, ?, ?)
                    """, (
                        congress, hchamber.title(),
                        hd.get("title", "Untitled"), hearing_date,
                        package_id, transcript_url, pdf_url
                    ))
                    hearing_id = db.execute("SELECT last_insert_rowid()").fetchone()[0]

                    for comm in committees:
                        code = comm.get("systemCode", "")
                        if code:
                            comm_id = get_or_create_committee(
                                db, code, comm.get("name", ""),
                                hchamber.title()
                            )
                            try:
                                db.execute(
                                    "INSERT OR IGNORE INTO hearing_committees VALUES (?, ?)",
                                    (hearing_id, comm_id)
                                )
                            except sqlite3.IntegrityError:
                                pass

                db.commit()
                count += 1

                if count % 50 == 0:
                    print(f"  Linked {count} hearings to GovInfo packages")

            if limit and count >= limit:
                break
            offset += 250
            if len(data.get("hearings", [])) < 250:
                break

    print(f"\n=== Linked {count} hearings to GovInfo packages ===")
    db.close()


def parse_transcript_witnesses(limit=None):
    """Parse hearing transcripts to extract witness information."""
    govinfo = GovInfoClient()
    db = get_db()

    # Get hearings that have GovInfo packages but haven't been parsed
    hearings = db.execute("""
        SELECT id, govinfo_package_id, congress, chamber, title, date
        FROM hearings
        WHERE govinfo_package_id IS NOT NULL
        AND has_parsed_witnesses = 0
        ORDER BY date DESC
        LIMIT ?
    """, (limit or 10000,)).fetchall()

    print(f"Found {len(hearings)} hearings to parse")

    parsed = 0
    for h in hearings:
        package_id = h["govinfo_package_id"]
        print(f"  Parsing {package_id}: {h['title'][:60]}...")

        text = govinfo.get_hearing_text(package_id)
        if not text or isinstance(text, dict):
            db.execute("UPDATE hearings SET has_parsed_witnesses = -1 WHERE id = ?", (h["id"],))
            db.commit()
            continue

        # Check if hearing already has witnesses from committee-meeting data
        existing_witnesses = db.execute(
            "SELECT COUNT(*) as cnt FROM witness_appearances WHERE hearing_id = ?",
            (h["id"],)
        ).fetchone()["cnt"]

        if existing_witnesses == 0:
            # Extract witnesses from transcript
            witnesses = extract_witnesses_from_transcript(text)
            for w in witnesses:
                witness_id = get_or_create_witness(db, w["name"], w.get("position"), w.get("organization"))
                add_witness_title(db, witness_id, w.get("position"), w.get("organization"), h["date"])
                try:
                    db.execute("""
                        INSERT OR IGNORE INTO witness_appearances
                        (witness_id, hearing_id, position, organization)
                        VALUES (?, ?, ?, ?)
                    """, (witness_id, h["id"], w.get("position"), w.get("organization")))
                except sqlite3.IntegrityError:
                    pass

        # Extract testimony and QFR
        appearances = db.execute("""
            SELECT wa.id, wa.witness_id, w.name, w.normalized_name
            FROM witness_appearances wa
            JOIN witnesses w ON wa.witness_id = w.id
            WHERE wa.hearing_id = ?
        """, (h["id"],)).fetchall()

        for app in appearances:
            # Try to extract written statement from transcript
            statement = extract_witness_statement(text, app["name"], app["normalized_name"])
            if statement:
                try:
                    db.execute("""
                        INSERT INTO testimony (appearance_id, testimony_type, content, source)
                        VALUES (?, 'written_statement', ?, 'transcript_parse')
                    """, (app["id"], statement))
                except sqlite3.IntegrityError:
                    pass

            # Extract QFR
            qfrs = extract_qfr(text, app["name"], app["normalized_name"])
            for qfr in qfrs:
                try:
                    db.execute("""
                        INSERT INTO questions_for_record
                        (appearance_id, questioner_name, question_text, answer_text, source)
                        VALUES (?, ?, ?, ?, 'transcript_parse')
                    """, (app["id"], qfr.get("questioner"), qfr.get("question"), qfr.get("answer")))
                except sqlite3.IntegrityError:
                    pass

        db.execute("""
            UPDATE hearings SET has_transcript = 1, has_parsed_witnesses = 1 WHERE id = ?
        """, (h["id"],))
        db.commit()
        parsed += 1

        if parsed % 10 == 0:
            print(f"  Parsed {parsed}/{len(hearings)} transcripts")

    # Update counts
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

    print(f"\n=== Parsed {parsed} transcripts ===")
    db.close()


def extract_witnesses_from_transcript(text):
    """Extract witness names and titles from hearing transcript text."""
    witnesses = []
    seen = set()

    # Pattern 1: "STATEMENT OF NAME, TITLE, ORGANIZATION"
    pattern1 = re.compile(
        r'STATEMENT\s+OF\s+(.+?)(?:\n|$)',
        re.IGNORECASE
    )
    for match in pattern1.finditer(text):
        line = match.group(1).strip()
        # Clean up
        line = re.sub(r'\s+', ' ', line)
        # Split on comma to get name, title, org
        parts = [p.strip() for p in line.split(',')]
        if parts:
            name = parts[0]
            position = parts[1] if len(parts) > 1 else None
            org = ', '.join(parts[2:]) if len(parts) > 2 else None
            norm = normalize_name(name)
            if norm and norm not in seen and len(norm) > 2:
                seen.add(norm)
                witnesses.append({"name": name, "position": position, "organization": org})

    # Pattern 2: Table of contents witness listings
    # e.g., "Smith, John, Director, Agency Name.......4"
    pattern2 = re.compile(
        r'^([A-Z][a-z]+,\s+[A-Z][a-z]+(?:\s+[A-Z]\.)?),\s*(.+?)\.{2,}\d+\s*$',
        re.MULTILINE
    )
    for match in pattern2.finditer(text[:5000]):  # TOC is at the start
        name_parts = match.group(1).strip()
        rest = match.group(2).strip()
        # Reverse "Last, First" to "First Last"
        np = name_parts.split(',', 1)
        if len(np) == 2:
            name = f"{np[1].strip()} {np[0].strip()}"
        else:
            name = name_parts
        norm = normalize_name(name)
        if norm and norm not in seen:
            seen.add(norm)
            # Try to parse position and org from rest
            rparts = [p.strip() for p in rest.split(',')]
            position = rparts[0] if rparts else None
            org = ', '.join(rparts[1:]) if len(rparts) > 1 else None
            witnesses.append({"name": name, "position": position, "organization": org})

    return witnesses


def extract_witness_statement(text, name, normalized_name):
    """Extract a witness's prepared statement from transcript text."""
    # Look for "STATEMENT OF [NAME]" followed by content until next section
    name_upper = name.upper()
    last_name = normalized_name.split()[-1].upper() if normalized_name else ""

    # Try to find their statement section
    patterns = [
        rf'STATEMENT\s+OF\s+{re.escape(name_upper)}.*?\n(.*?)(?=\n\s*(?:STATEMENT\s+OF|PREPARED\s+STATEMENT|The\s+(?:Chairman|Chair)\.|Senator\s+\w+\.|Representative\s+\w+\.))',
        rf'PREPARED\s+STATEMENT\s+OF\s+.*?{re.escape(last_name)}.*?\n(.*?)(?=\n\s*(?:STATEMENT\s+OF|PREPARED\s+STATEMENT|The\s+(?:Chairman|Chair)\.))',
    ]

    for pattern in patterns:
        match = re.search(pattern, text, re.DOTALL | re.IGNORECASE)
        if match:
            statement = match.group(1).strip()
            # Limit length and clean up
            if len(statement) > 200:
                return statement[:50000]  # Cap at 50K chars
    return None


def extract_qfr(text, name, normalized_name):
    """Extract Questions for the Record for a witness."""
    qfrs = []
    last_name = normalized_name.split()[-1].upper() if normalized_name else ""

    if not last_name:
        return qfrs

    # Look for QFR sections
    patterns = [
        rf'RESPONSES?\s+TO\s+(?:WRITTEN\s+)?QUESTIONS?\s+(?:FOR\s+THE\s+RECORD\s+)?OF\s+(\w+(?:\s+\w+)?)\s+FROM\s+.*?{re.escape(last_name)}(.*?)(?=RESPONSES?\s+TO|ADDITIONAL\s+MATERIAL|$)',
        rf'QUESTIONS?\s+FOR\s+THE\s+RECORD.*?{re.escape(last_name)}(.*?)(?=QUESTIONS?\s+FOR|ADDITIONAL\s+MATERIAL|$)',
    ]

    for pattern in patterns:
        for match in re.finditer(pattern, text, re.DOTALL | re.IGNORECASE):
            groups = match.groups()
            questioner = groups[0] if len(groups) > 1 else "Unknown"
            content = groups[-1].strip()

            if len(content) < 50:
                continue

            # Try to split into Q&A pairs
            qa_pattern = re.compile(r'Q\.?\d*[.:]\s*(.*?)\s*A\.?\d*[.:]\s*(.*?)(?=Q\.?\d*[.:]|$)', re.DOTALL)
            qa_matches = qa_pattern.findall(content)

            if qa_matches:
                for q, a in qa_matches:
                    if q.strip() and a.strip():
                        qfrs.append({
                            "questioner": questioner,
                            "question": q.strip()[:10000],
                            "answer": a.strip()[:10000]
                        })
            elif content:
                # Store as single block
                qfrs.append({
                    "questioner": questioner,
                    "question": None,
                    "answer": content[:20000]
                })

    return qfrs


def main():
    parser = argparse.ArgumentParser(description="Collect congressional witness data")
    parser.add_argument("--congress", type=str, default="116,117,118,119",
                        help="Comma-separated congress numbers (default: 116,117,118,119)")
    parser.add_argument("--chamber", type=str, default=None,
                        help="Filter by chamber (house/senate)")
    parser.add_argument("--limit", type=int, default=None,
                        help="Limit number of records per congress")
    parser.add_argument("--skip-meetings", action="store_true",
                        help="Skip committee meeting collection")
    parser.add_argument("--skip-transcripts", action="store_true",
                        help="Skip transcript collection")
    parser.add_argument("--skip-parse", action="store_true",
                        help="Skip transcript parsing")
    args = parser.parse_args()

    congress_list = [int(c.strip()) for c in args.congress.split(",")]

    print("=== Congressional Witness Database Collection ===")
    print(f"Congresses: {congress_list}")
    print(f"Chamber: {args.chamber or 'all'}")
    print(f"Limit: {args.limit or 'none'}")
    print()

    # Initialize database
    init_db()

    # Phase 1: Collect committee meetings with witness data
    if not args.skip_meetings:
        print("\n--- Phase 1: Collecting committee meetings ---")
        collect_committee_meetings(congress_list, args.chamber, args.limit)

    # Phase 2: Link hearings to GovInfo transcripts
    if not args.skip_transcripts:
        print("\n--- Phase 2: Linking hearings to transcripts ---")
        collect_hearing_transcripts(congress_list, args.limit)

    # Phase 3: Parse transcripts for witness details, testimony, QFR
    if not args.skip_parse:
        print("\n--- Phase 3: Parsing transcripts ---")
        parse_transcript_witnesses(args.limit)

    # Print summary
    db = get_db()
    stats = {
        "witnesses": db.execute("SELECT COUNT(*) FROM witnesses").fetchone()[0],
        "hearings": db.execute("SELECT COUNT(*) FROM hearings").fetchone()[0],
        "appearances": db.execute("SELECT COUNT(*) FROM witness_appearances").fetchone()[0],
        "testimony": db.execute("SELECT COUNT(*) FROM testimony").fetchone()[0],
        "qfr": db.execute("SELECT COUNT(*) FROM questions_for_record").fetchone()[0],
    }
    db.close()

    print("\n=== COLLECTION COMPLETE ===")
    for k, v in stats.items():
        print(f"  {k}: {v:,}")


if __name__ == "__main__":
    main()
