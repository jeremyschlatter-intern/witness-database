#!/usr/bin/env python3
"""
Conservative witness deduplication.
Only merges records when there's strong evidence they're the same person.
"""

import os
import re
import sqlite3
import sys
import unicodedata
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from app.database import get_db


def clean_name(name):
    """Remove honorifics, titles, and suffixes for comparison."""
    # Remove leading titles/honorifics
    name = re.sub(
        r'^(The Honorable|Hon\.|Dr\.|Mr\.|Mrs\.|Ms\.|Miss|Prof\.|Rear Admiral|'
        r'Vice Admiral|Admiral|General|Colonel|Major|Captain|Lieutenant|Sergeant|'
        r'Lieutenant General|Major General|Brigadier General|'
        r'Ambassador|Commissioner|Master Chief|Master Chief Petty Officer|'
        r'Chief Master Sergeant|Command Sergeant Major)\s+',
        '', name, flags=re.IGNORECASE
    ).strip()
    # Remove suffixes
    name = re.sub(r',?\s*(Jr\.?|Sr\.?|III|IV|II|Esq\.?|Ph\.?D\.?|M\.?D\.?|J\.?D\.?|CPA|LCSW|RN|MSN|DVM|FACHE)$', '', name, flags=re.IGNORECASE).strip()
    name = re.sub(r'\s+', ' ', name).strip()
    return name


def normalize_for_compare(name):
    """Normalize name for comparison (lowercase, no middle initials)."""
    name = clean_name(name)
    # Remove middle initials like "John A. Smith" -> "John Smith"
    name = re.sub(r'\s+[A-Z]\.\s*', ' ', name)
    name = re.sub(r'\s+[A-Z]\s+', ' ', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name.lower()


# Common nickname mappings
NICKNAMES = {
    'william': ['bill', 'billy', 'will', 'willy'],
    'robert': ['bob', 'bobby', 'rob'],
    'richard': ['dick', 'rick', 'rich'],
    'james': ['jim', 'jimmy', 'jamie'],
    'john': ['jack', 'johnny', 'jon'],
    'thomas': ['tom', 'tommy'],
    'michael': ['mike', 'mikey'],
    'daniel': ['dan', 'danny'],
    'david': ['dave', 'davey'],
    'christopher': ['chris'],
    'matthew': ['matt'],
    'timothy': ['tim', 'timmy'],
    'nicholas': ['nick'],
    'stephen': ['steve', 'steven'],
    'steven': ['steve', 'stephen'],
    'joseph': ['joe', 'joey'],
    'edward': ['ed', 'eddie', 'ted'],
    'anthony': ['tony'],
    'charles': ['charlie', 'chuck'],
    'douglas': ['doug'],
    'gregory': ['greg'],
    'andrew': ['andy', 'drew'],
    'benjamin': ['ben'],
    'raymond': ['ray'],
    'gerald': ['jerry', 'gerry'],
    'kenneth': ['ken', 'kenny'],
    'lawrence': ['larry'],
    'patrick': ['pat'],
    'katherine': ['kathy', 'kate', 'katie', 'cathy'],
    'catherine': ['kathy', 'kate', 'katie', 'cathy'],
    'elizabeth': ['liz', 'beth', 'betty', 'eliz'],
    'margaret': ['maggie', 'peggy', 'meg'],
    'deborah': ['debra', 'deb', 'debbie'],
    'debra': ['deborah', 'deb', 'debbie'],
    'patricia': ['pat', 'patty', 'trish'],
    'jennifer': ['jen', 'jenny'],
    'jessica': ['jess', 'jessie'],
    'susan': ['sue', 'susie'],
    'virginia': ['ginny', 'ginger'],
    'jonathan': ['jon', 'john'],
    'raúl': ['raul'],
    'raul': ['raúl'],
    'alexander': ['alex'],
    'gt': ['glenn'],
}


def are_names_same_person(name1, name2):
    """Conservative check if two names refer to the same person."""
    n1 = normalize_for_compare(name1)
    n2 = normalize_for_compare(name2)

    if n1 == n2:
        return True

    parts1 = n1.split()
    parts2 = n2.split()

    if len(parts1) < 2 or len(parts2) < 2:
        return False

    last1, last2 = parts1[-1], parts2[-1]
    first1, first2 = parts1[0], parts2[0]

    # Last names must match exactly
    if last1 != last2:
        return False

    # First names must match or be known nicknames
    if first1 == first2:
        return True

    # Check nickname mappings
    for canonical, nicks in NICKNAMES.items():
        all_variants = [canonical] + nicks
        if first1 in all_variants and first2 in all_variants:
            return True

    # One first name starts with the other (e.g., "Dan" and "Daniel")
    if len(first1) >= 3 and len(first2) >= 3:
        shorter = first1 if len(first1) <= len(first2) else first2
        longer = first2 if shorter == first1 else first1
        if longer.startswith(shorter):
            return True

    return False


def find_conservative_duplicates(db):
    """Find duplicate groups using conservative matching."""
    witnesses = db.execute("""
        SELECT id, name, normalized_name, first_name, last_name, appearance_count
        FROM witnesses
        WHERE appearance_count > 0
    """).fetchall()

    # Group by last name
    by_last = defaultdict(list)
    for w in witnesses:
        if w["last_name"]:
            by_last[w["last_name"].lower()].append(dict(w))

    duplicates = []
    merged_ids = set()

    for last_name, group in by_last.items():
        if len(group) < 2:
            continue

        # For each pair, check if they're the same person
        # Build connected components
        same_person = defaultdict(set)
        for i in range(len(group)):
            for j in range(i + 1, len(group)):
                w1, w2 = group[i], group[j]
                if are_names_same_person(w1["name"], w2["name"]):
                    same_person[w1["id"]].add(w2["id"])
                    same_person[w2["id"]].add(w1["id"])

        # Build groups from connected components
        visited = set()
        for wid in same_person:
            if wid in visited:
                continue
            component = set()
            stack = [wid]
            while stack:
                curr = stack.pop()
                if curr in visited:
                    continue
                visited.add(curr)
                component.add(curr)
                stack.extend(same_person.get(curr, []))

            if len(component) > 1:
                # Find all witnesses in this component
                comp_witnesses = [w for w in group if w["id"] in component]
                # Pick canonical: most appearances, then longest name
                canonical = max(comp_witnesses,
                                key=lambda w: (w["appearance_count"], len(w["name"])))
                others = [w for w in comp_witnesses if w["id"] != canonical["id"]]
                if others and canonical["id"] not in merged_ids:
                    duplicates.append({
                        "canonical": canonical,
                        "duplicates": others
                    })
                    merged_ids.add(canonical["id"])
                    for o in others:
                        merged_ids.add(o["id"])

    return duplicates


def merge_witnesses(db, canonical_id, duplicate_ids):
    """Merge duplicate witness records into the canonical one."""
    for dup_id in duplicate_ids:
        # Check for conflicting appearances (same hearing)
        conflicts = db.execute("""
            SELECT hearing_id FROM witness_appearances
            WHERE witness_id = ?
            AND hearing_id IN (SELECT hearing_id FROM witness_appearances WHERE witness_id = ?)
        """, (dup_id, canonical_id)).fetchall()

        # Delete conflicting appearances from the duplicate
        for c in conflicts:
            db.execute("""
                DELETE FROM witness_appearances
                WHERE witness_id = ? AND hearing_id = ?
            """, (dup_id, c["hearing_id"]))

        # Move remaining appearances
        db.execute("""
            UPDATE witness_appearances SET witness_id = ? WHERE witness_id = ?
        """, (canonical_id, dup_id))

        # Move titles (avoid duplicates)
        db.execute("""
            INSERT OR IGNORE INTO witness_titles (witness_id, title, organization, start_date)
            SELECT ?, title, organization, start_date
            FROM witness_titles WHERE witness_id = ?
        """, (canonical_id, dup_id))
        db.execute("DELETE FROM witness_titles WHERE witness_id = ?", (dup_id,))

        # Delete the duplicate witness record
        db.execute("DELETE FROM witnesses WHERE id = ?", (dup_id,))

    db.commit()


def normalize_titles(db):
    """Normalize organization names to reduce fragmentation."""
    replacements = [
        (r'^U\.?S\.?\s+', ''),
        (r'^United States\s+', ''),
        (r',\s*United States Department of ', ', Department of '),
        (r',\s*U\.?S\.?\s*Department of ', ', Department of '),
        (r' of the United States$', ''),
        (r' of the United States,', ','),
    ]

    titles = db.execute("SELECT DISTINCT organization FROM witness_titles WHERE organization IS NOT NULL AND organization != ''").fetchall()
    updates = {}

    for row in titles:
        org = row[0]
        normalized = org.strip()
        for pattern, replacement in replacements:
            normalized = re.sub(pattern, replacement, normalized, flags=re.IGNORECASE)
        normalized = re.sub(r'\s+', ' ', normalized).strip()
        if normalized != org and normalized:
            updates[org] = normalized

    for old_org, new_org in updates.items():
        db.execute(
            "UPDATE OR IGNORE witness_titles SET organization = ? WHERE organization = ?",
            (new_org, old_org)
        )

    # Remove duplicate title entries that now match after normalization
    db.execute("""
        DELETE FROM witness_titles
        WHERE rowid NOT IN (
            SELECT MIN(rowid) FROM witness_titles
            GROUP BY witness_id, title, organization
        )
    """)

    db.commit()
    print(f"Normalized {len(updates)} organization name variants")


def update_counts(db):
    """Update witness appearance counts after merging."""
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
    db.execute("INSERT INTO witnesses_fts(witnesses_fts) VALUES('rebuild')")
    db.commit()


if __name__ == "__main__":
    print("=== Conservative Witness Deduplication ===\n")
    db = get_db()

    before = db.execute("SELECT COUNT(*) FROM witnesses WHERE appearance_count > 0").fetchone()[0]
    print(f"Active witnesses before: {before}")

    duplicates = find_conservative_duplicates(db)
    print(f"Found {len(duplicates)} duplicate groups\n")

    # Show examples
    total_merged = 0
    for dup in duplicates[:30]:
        names = [dup["canonical"]["name"]] + [d["name"] for d in dup["duplicates"]]
        total_appearances = dup["canonical"]["appearance_count"] + sum(d["appearance_count"] for d in dup["duplicates"])
        total_merged += len(dup["duplicates"])
        print(f"  {' | '.join(names)} -> keep '{dup['canonical']['name']}' ({total_appearances} total)")

    print(f"\n... and {len(duplicates) - 30} more groups") if len(duplicates) > 30 else None
    print(f"\nMerging {len(duplicates)} groups ({total_merged} duplicate records)...")
    for dup in duplicates:
        merge_witnesses(db, dup["canonical"]["id"], [d["id"] for d in dup["duplicates"]])

    print("\nNormalizing organization names...")
    normalize_titles(db)

    print("Updating counts...")
    update_counts(db)

    after = db.execute("SELECT COUNT(*) FROM witnesses WHERE appearance_count > 0").fetchone()[0]
    print(f"\nActive witnesses after: {after} (removed {before - after} duplicates)")

    print("\nTop 15 witnesses after dedup:")
    for r in db.execute("""
        SELECT name, appearance_count FROM witnesses
        WHERE appearance_count > 0
        ORDER BY appearance_count DESC LIMIT 15
    """):
        print(f"  {r[0]}: {r[1]}")

    db.close()
