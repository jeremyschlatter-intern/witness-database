#!/usr/bin/env python3
"""
Normalize organization names across the database to eliminate duplicates.
Handles witness_appearances.organization and witness_titles.organization.
"""

import os
import re
import sqlite3
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from app.database import get_db


def build_canonical_map(orgs):
    """Build a mapping of org names to their canonical form."""
    canonical = {}

    for org in orgs:
        normalized = org.strip()

        # Strip leading "The " (e.g., "The Foundation for Defense of Democracies")
        normalized = re.sub(r'^The\s+', '', normalized)

        # Strip "U.S. " / "U.S." prefix
        normalized = re.sub(r'^U\.?S\.?\s+', '', normalized)

        # Strip "United States " prefix
        normalized = re.sub(r'^United States\s+', '', normalized)

        # Strip trailing " of the United States" or " of the U.S."
        normalized = re.sub(r'\s+of the United States$', '', normalized)
        normalized = re.sub(r'\s+of the U\.?S\.?$', '', normalized)

        # Strip trailing parenthetical abbreviations like " (GAO)", " (EPA)"
        normalized = re.sub(r'\s*\([A-Z]{2,10}\)$', '', normalized)

        # Normalize internal "United States" references
        normalized = re.sub(r',\s*United States\s+', ', ', normalized)
        normalized = re.sub(r',\s*U\.?S\.?\s+', ', ', normalized)

        # Collapse whitespace
        normalized = re.sub(r'\s+', ' ', normalized).strip()

        if normalized != org and normalized:
            canonical[org] = normalized

    return canonical


def normalize_orgs(db):
    """Normalize organization names in both tables."""

    # Get all unique org names from both tables
    orgs_appearances = set()
    for row in db.execute("SELECT DISTINCT organization FROM witness_appearances WHERE organization IS NOT NULL AND organization <> ''"):
        orgs_appearances.add(row[0])

    orgs_titles = set()
    for row in db.execute("SELECT DISTINCT organization FROM witness_titles WHERE organization IS NOT NULL AND organization <> ''"):
        orgs_titles.add(row[0])

    all_orgs = orgs_appearances | orgs_titles
    print(f"Found {len(all_orgs)} unique organization names")

    canonical = build_canonical_map(all_orgs)

    if not canonical:
        print("No normalizations needed")
        return

    print(f"Normalizing {len(canonical)} organization names:")
    for old, new in sorted(canonical.items())[:30]:
        print(f"  '{old}' -> '{new}'")
    if len(canonical) > 30:
        print(f"  ... and {len(canonical) - 30} more")

    # Apply to witness_appearances
    count_appearances = 0
    for old, new in canonical.items():
        r = db.execute("UPDATE witness_appearances SET organization = ? WHERE organization = ?", (new, old))
        count_appearances += r.rowcount

    # Apply to witness_titles
    count_titles = 0
    for old, new in canonical.items():
        r = db.execute("UPDATE OR IGNORE witness_titles SET organization = ? WHERE organization = ?", (new, old))
        count_titles += r.rowcount

    # Remove duplicate witness_titles entries that now match
    db.execute("""
        DELETE FROM witness_titles
        WHERE rowid NOT IN (
            SELECT MIN(rowid) FROM witness_titles
            GROUP BY witness_id, title, organization
        )
    """)

    db.commit()
    print(f"\nUpdated {count_appearances} appearance records, {count_titles} title records")

    # Show new top orgs
    print("\nTop 20 organizations after normalization:")
    for row in db.execute("""
        SELECT organization, COUNT(DISTINCT witness_id) as wc
        FROM witness_appearances
        WHERE organization IS NOT NULL AND organization <> ''
        GROUP BY organization ORDER BY wc DESC LIMIT 20
    """):
        print(f"  {row[0]}: {row[1]} witnesses")


def clean_garbled_data(db):
    """Clean garbled location data and other artifacts."""
    # Fix rooms that are just dashes
    db.execute("UPDATE hearings SET location_room = NULL WHERE location_room LIKE '%--%'")

    # Fix hearing titles with CR/LF
    bad_titles = db.execute("SELECT id, title FROM hearings WHERE title LIKE '%\r%' OR title LIKE '%\n%'").fetchall()
    for row in bad_titles:
        clean = re.sub(r'\r\n?|\n', ' ', row['title'])
        clean = re.sub(r'\s+', ' ', clean).strip()
        db.execute("UPDATE hearings SET title = ? WHERE id = ?", (clean, row['id']))

    if bad_titles:
        # Rebuild FTS index
        db.execute("INSERT INTO hearings_fts(hearings_fts) VALUES('rebuild')")

    db.commit()
    print(f"Cleaned {len(bad_titles)} hearing titles, fixed garbled room numbers")


if __name__ == "__main__":
    print("=== Organization Name Normalization ===\n")
    db = get_db()
    normalize_orgs(db)
    print()
    clean_garbled_data(db)
    db.close()
