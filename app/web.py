"""Flask web application for the Congressional Witness Database."""

import csv
import io
import math
import os
import re
import sys
from urllib.parse import urlencode

from flask import Flask, render_template, request, jsonify, g, Response

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from app.database import get_db, init_db

app = Flask(
    __name__,
    template_folder=os.path.join(os.path.dirname(os.path.dirname(__file__)), "templates"),
    static_folder=os.path.join(os.path.dirname(os.path.dirname(__file__)), "static"),
)

PER_PAGE = 50

# Common abbreviations used in congressional testimony
ABBREVIATIONS = {
    'FAA': 'Federal Aviation Administration',
    'FDA': 'Food and Drug Administration',
    'FBI': 'Federal Bureau of Investigation',
    'CIA': 'Central Intelligence Agency',
    'NSA': 'National Security Agency',
    'EPA': 'Environmental Protection Agency',
    'DOD': 'Department of Defense',
    'DOJ': 'Department of Justice',
    'DOE': 'Department of Energy',
    'DOT': 'Department of Transportation',
    'DHS': 'Department of Homeland Security',
    'HHS': 'Department of Health and Human Services',
    'HUD': 'Department of Housing and Urban Development',
    'VA': 'Department of Veterans Affairs',
    'SEC': 'Securities and Exchange Commission',
    'FCC': 'Federal Communications Commission',
    'FTC': 'Federal Trade Commission',
    'FEMA': 'Federal Emergency Management Agency',
    'IRS': 'Internal Revenue Service',
    'OMB': 'Office of Management and Budget',
    'GAO': 'Government Accountability Office',
    'CBO': 'Congressional Budget Office',
    'NOAA': 'National Oceanic and Atmospheric Administration',
    'NASA': 'National Aeronautics and Space Administration',
    'NIH': 'National Institutes of Health',
    'CDC': 'Centers for Disease Control and Prevention',
    'USDA': 'Department of Agriculture',
    'SBA': 'Small Business Administration',
    'NTSB': 'National Transportation Safety Board',
    'TSA': 'Transportation Security Administration',
    'CBP': 'Customs and Border Protection',
    'ICE': 'Immigration and Customs Enforcement',
    'ATF': 'Bureau of Alcohol Tobacco Firearms and Explosives',
    'DEA': 'Drug Enforcement Administration',
    'CISA': 'Cybersecurity and Infrastructure Security Agency',
    'CFPB': 'Consumer Financial Protection Bureau',
    'FERC': 'Federal Energy Regulatory Commission',
    'NRC': 'Nuclear Regulatory Commission',
    'OSHA': 'Occupational Safety and Health Administration',
    'OPM': 'Office of Personnel Management',
    'GSA': 'General Services Administration',
    'USPS': 'Postal Service',
    'SSA': 'Social Security Administration',
    'NIST': 'National Institute of Standards and Technology',
    'NSF': 'National Science Foundation',
    'DOL': 'Department of Labor',
    'DOS': 'Department of State',
    'USTR': 'United States Trade Representative',
    'FinCEN': 'Financial Crimes Enforcement Network',
    'DARPA': 'Defense Advanced Research Projects Agency',
    'FDIC': 'Federal Deposit Insurance Corporation',
    'NCUA': 'National Credit Union Administration',
    'CFTC': 'Commodity Futures Trading Commission',
    'CPSC': 'Consumer Product Safety Commission',
    'EEOC': 'Equal Employment Opportunity Commission',
    'FMC': 'Federal Maritime Commission',
    'NLRB': 'National Labor Relations Board',
    'PBGC': 'Pension Benefit Guaranty Corporation',
    'USAID': 'United States Agency for International Development',
    'ONDCP': 'Office of National Drug Control Policy',
    'DNI': 'Director of National Intelligence',
}


def get_connection():
    if "db" not in g:
        g.db = get_db()
    return g.db


@app.teardown_appcontext
def close_db(exception):
    db = g.pop("db", None)
    if db:
        db.close()


def display_name(name):
    """Clean up a witness name for display - strip honorific prefixes."""
    if not name:
        return name
    name = re.sub(
        r'^(The Honorable|Hon\.|Dr\.|Mr\.|Mrs\.|Ms\.|Miss|Prof\.)\s+',
        '', name, flags=re.IGNORECASE
    )
    return name


def is_member_of_congress(titles_str):
    """Check if a witness is a Member of Congress based on their titles."""
    if not titles_str:
        return False
    moc_indicators = ['member of congress', 'congressman', 'congresswoman',
                      'representative', 'senator', 'ranking member', 'chairman, committee',
                      'chairwoman, committee', 'chair, committee']
    lower = titles_str.lower()
    return any(ind in lower for ind in moc_indicators)


def expand_abbreviations(q):
    """Expand known abbreviations in a search query."""
    words = q.split()
    expanded_words = []
    has_expansion = False
    for word in words:
        upper = word.upper()
        if upper in ABBREVIATIONS:
            expanded_words.append(ABBREVIATIONS[upper])
            has_expansion = True
        else:
            expanded_words.append(word)
    if has_expansion:
        return ' '.join(expanded_words)
    return None


def fts_query(q):
    """Convert a user search query into an FTS5 query."""
    q = q.strip()
    if not q:
        return q
    q = q.replace('"', '')
    words = q.split()
    if len(words) > 1:
        return f'"{q}" OR ({" ".join(words)})'
    return q


@app.template_filter('clean_name')
def clean_name_filter(name):
    return display_name(name)


@app.template_filter('clean_text')
def clean_text_filter(text):
    """Remove carriage returns and collapse whitespace."""
    if not text:
        return text
    text = re.sub(r'\r\n?|\n', ' ', text)
    return re.sub(r'\s+', ' ', text).strip()


@app.template_filter('congress_ordinal')
def congress_ordinal_filter(n):
    """Convert congress number to ordinal (e.g., 119 -> 119th)."""
    n = int(n)
    if 11 <= (n % 100) <= 13:
        suffix = 'th'
    else:
        suffix = {1: 'st', 2: 'nd', 3: 'rd'}.get(n % 10, 'th')
    return f"{n}{suffix}"


@app.context_processor
def inject_stats():
    db = get_connection()
    return {
        "total_witnesses": db.execute("SELECT COUNT(*) FROM witnesses WHERE appearance_count > 0").fetchone()[0],
        "total_hearings": db.execute("SELECT COUNT(*) FROM hearings").fetchone()[0],
        "total_appearances": db.execute("SELECT COUNT(*) FROM witness_appearances").fetchone()[0],
    }


@app.route("/")
def index():
    db = get_connection()

    recent_hearings = db.execute("""
        SELECT h.*, GROUP_CONCAT(c.name, '; ') as committee_names,
            (SELECT COUNT(*) FROM witness_appearances wa WHERE wa.hearing_id = h.id) as witness_count
        FROM hearings h
        LEFT JOIN hearing_committees hc ON h.id = hc.hearing_id
        LEFT JOIN committees c ON hc.committee_id = c.id
        WHERE h.date IS NOT NULL
        GROUP BY h.id
        ORDER BY h.date DESC
        LIMIT 10
    """).fetchall()

    # Top NON-member witnesses (exclude Members of Congress)
    top_witnesses = db.execute("""
        SELECT w.*,
            (SELECT GROUP_CONCAT(wt.title, '; ')
             FROM witness_titles wt WHERE wt.witness_id = w.id) as titles
        FROM witnesses w
        WHERE w.appearance_count > 0
        AND NOT EXISTS (
            SELECT 1 FROM witness_titles wt
            WHERE wt.witness_id = w.id
            AND (wt.title LIKE '%Member of Congress%'
                 OR wt.title LIKE '%Rep.%'
                 OR wt.title LIKE '%Senator%')
        )
        ORDER BY w.appearance_count DESC
        LIMIT 15
    """).fetchall()

    # Top Members of Congress who testify
    top_moc_witnesses = db.execute("""
        SELECT w.*,
            (SELECT GROUP_CONCAT(wt.title, '; ')
             FROM witness_titles wt WHERE wt.witness_id = w.id) as titles
        FROM witnesses w
        WHERE w.appearance_count > 0
        AND EXISTS (
            SELECT 1 FROM witness_titles wt
            WHERE wt.witness_id = w.id
            AND (wt.title LIKE '%Member of Congress%'
                 OR wt.title LIKE '%Rep.%')
        )
        ORDER BY w.appearance_count DESC
        LIMIT 10
    """).fetchall()

    congress_stats = db.execute("""
        SELECT congress, chamber, COUNT(*) as hearing_count,
            (SELECT COUNT(DISTINCT wa2.witness_id)
             FROM witness_appearances wa2
             JOIN hearings h2 ON wa2.hearing_id = h2.id
             WHERE h2.congress = h.congress AND h2.chamber = h.chamber
            ) as witness_count
        FROM hearings h
        GROUP BY congress, chamber
        ORDER BY congress DESC, chamber
    """).fetchall()

    return render_template("index.html",
                           recent_hearings=recent_hearings,
                           top_witnesses=top_witnesses,
                           top_moc_witnesses=top_moc_witnesses,
                           congress_stats=congress_stats)


@app.route("/witnesses")
def witnesses_list():
    db = get_connection()
    page = request.args.get("page", 1, type=int)
    sort = request.args.get("sort", "appearances")
    q = request.args.get("q", "").strip()
    chamber = request.args.get("chamber", "")
    congress = request.args.get("congress", "", type=str)
    fmt = request.args.get("format", "")

    offset = (page - 1) * PER_PAGE

    if q:
        fts_q = fts_query(q)
        try:
            count = db.execute("""
                SELECT COUNT(DISTINCT w.id)
                FROM witnesses w
                JOIN witnesses_fts ON witnesses_fts.rowid = w.id
                WHERE witnesses_fts MATCH ?
            """, (fts_q,)).fetchone()[0]

            witnesses = db.execute("""
                SELECT w.*,
                    (SELECT GROUP_CONCAT(wt.title, '; ')
                     FROM witness_titles wt WHERE wt.witness_id = w.id) as titles
                FROM witnesses w
                JOIN witnesses_fts ON witnesses_fts.rowid = w.id
                WHERE witnesses_fts MATCH ?
                ORDER BY w.appearance_count DESC
                LIMIT ? OFFSET ?
            """, (fts_q, PER_PAGE, offset)).fetchall()
        except Exception:
            # Fallback to LIKE search if FTS fails
            like_q = f"%{q}%"
            count = db.execute(
                "SELECT COUNT(*) FROM witnesses WHERE name LIKE ?", (like_q,)
            ).fetchone()[0]
            witnesses = db.execute("""
                SELECT w.*,
                    (SELECT GROUP_CONCAT(wt.title, '; ')
                     FROM witness_titles wt WHERE wt.witness_id = w.id) as titles
                FROM witnesses w WHERE w.name LIKE ?
                ORDER BY w.appearance_count DESC
                LIMIT ? OFFSET ?
            """, (like_q, PER_PAGE, offset)).fetchall()
    else:
        where_clauses = ["w.appearance_count > 0"]
        params = []

        if chamber:
            where_clauses.append("""
                EXISTS (SELECT 1 FROM witness_appearances wa
                JOIN hearings h ON wa.hearing_id = h.id
                WHERE wa.witness_id = w.id AND h.chamber = ?)
            """)
            params.append(chamber)

        if congress:
            where_clauses.append("""
                EXISTS (SELECT 1 FROM witness_appearances wa
                JOIN hearings h ON wa.hearing_id = h.id
                WHERE wa.witness_id = w.id AND h.congress = ?)
            """)
            params.append(int(congress))

        where = " AND ".join(where_clauses)
        count = db.execute(f"SELECT COUNT(*) FROM witnesses w WHERE {where}", params).fetchone()[0]

        order = {
            "appearances": "w.appearance_count DESC",
            "name": "w.last_name ASC, w.first_name ASC",
            "recent": "w.last_appearance_date DESC",
        }.get(sort, "w.appearance_count DESC")

        if fmt == "csv":
            # Export all matching records
            witnesses = db.execute(f"""
                SELECT w.*,
                    (SELECT GROUP_CONCAT(wt.title, '; ')
                     FROM witness_titles wt WHERE wt.witness_id = w.id) as titles
                FROM witnesses w
                WHERE {where}
                ORDER BY {order}
            """, params).fetchall()
            return export_witnesses_csv(witnesses)

        witnesses = db.execute(f"""
            SELECT w.*,
                (SELECT GROUP_CONCAT(wt.title, '; ')
                 FROM witness_titles wt WHERE wt.witness_id = w.id) as titles
            FROM witnesses w
            WHERE {where}
            ORDER BY {order}
            LIMIT ? OFFSET ?
        """, params + [PER_PAGE, offset]).fetchall()

    total_pages = math.ceil(count / PER_PAGE) if count else 0

    congresses = db.execute(
        "SELECT DISTINCT congress FROM hearings ORDER BY congress DESC"
    ).fetchall()

    return render_template("witnesses.html",
                           witnesses=witnesses,
                           page=page,
                           total_pages=total_pages,
                           total=count,
                           sort=sort,
                           q=q,
                           chamber=chamber,
                           congress=congress,
                           congresses=congresses)


def export_witnesses_csv(witnesses):
    """Export witnesses to CSV."""
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["ID", "Name", "Appearances", "First Appearance", "Last Appearance", "Titles"])
    for w in witnesses:
        writer.writerow([
            f"W-{w['id']:05d}",
            display_name(w['name']),
            w['appearance_count'],
            w['first_appearance_date'] or '',
            w['last_appearance_date'] or '',
            w['titles'] or '',
        ])
    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=witnesses.csv"}
    )


@app.route("/witness/<int:witness_id>")
def witness_detail(witness_id):
    db = get_connection()

    witness = db.execute("SELECT * FROM witnesses WHERE id = ?", (witness_id,)).fetchone()
    if not witness:
        return "Witness not found", 404

    titles = db.execute("""
        SELECT * FROM witness_titles WHERE witness_id = ?
        ORDER BY start_date DESC
    """, (witness_id,)).fetchall()

    appearances = db.execute("""
        SELECT wa.*, h.title as hearing_title, h.date as hearing_date,
            h.congress, h.chamber, h.id as hearing_id,
            GROUP_CONCAT(c.name, '; ') as committee_names,
            (SELECT COUNT(*) FROM testimony t WHERE t.appearance_id = wa.id) as testimony_count,
            (SELECT COUNT(*) FROM questions_for_record q WHERE q.appearance_id = wa.id) as qfr_count
        FROM witness_appearances wa
        JOIN hearings h ON wa.hearing_id = h.id
        LEFT JOIN hearing_committees hc ON h.id = hc.hearing_id
        LEFT JOIN committees c ON hc.committee_id = c.id
        WHERE wa.witness_id = ?
        GROUP BY wa.id
        ORDER BY h.date DESC
    """, (witness_id,)).fetchall()

    return render_template("witness_detail.html",
                           witness=witness,
                           titles=titles,
                           appearances=appearances)


@app.route("/hearings")
def hearings_list():
    db = get_connection()
    page = request.args.get("page", 1, type=int)
    q = request.args.get("q", "").strip()
    chamber = request.args.get("chamber", "")
    congress = request.args.get("congress", "", type=str)
    committee = request.args.get("committee", "")
    date_from = request.args.get("date_from", "")
    date_to = request.args.get("date_to", "")

    offset = (page - 1) * PER_PAGE

    where_clauses = ["1=1"]
    params = []

    if q:
        fts_q = fts_query(q)
        try:
            where_clauses.append("h.id IN (SELECT rowid FROM hearings_fts WHERE hearings_fts MATCH ?)")
            params.append(fts_q)
        except Exception:
            where_clauses.append("h.title LIKE ?")
            params.append(f"%{q}%")

    if chamber:
        where_clauses.append("h.chamber = ?")
        params.append(chamber)

    if congress:
        where_clauses.append("h.congress = ?")
        params.append(int(congress))

    if committee:
        where_clauses.append("""
            EXISTS (SELECT 1 FROM hearing_committees hc
            WHERE hc.hearing_id = h.id AND hc.committee_id = ?)
        """)
        params.append(int(committee))

    if date_from:
        where_clauses.append("h.date >= ?")
        params.append(date_from)

    if date_to:
        where_clauses.append("h.date <= ?")
        params.append(date_to)

    where = " AND ".join(where_clauses)
    count = db.execute(f"SELECT COUNT(*) FROM hearings h WHERE {where}", params).fetchone()[0]

    hearings = db.execute(f"""
        SELECT h.*,
            GROUP_CONCAT(c.name) as committee_names,
            (SELECT COUNT(*) FROM witness_appearances wa WHERE wa.hearing_id = h.id) as witness_count
        FROM hearings h
        LEFT JOIN hearing_committees hc ON h.id = hc.hearing_id
        LEFT JOIN committees c ON hc.committee_id = c.id
        WHERE {where}
        GROUP BY h.id
        ORDER BY h.date DESC NULLS LAST
        LIMIT ? OFFSET ?
    """, params + [PER_PAGE, offset]).fetchall()

    total_pages = math.ceil(count / PER_PAGE) if count else 0

    congresses = db.execute(
        "SELECT DISTINCT congress FROM hearings ORDER BY congress DESC"
    ).fetchall()

    # Only show committees with names
    committees = db.execute(
        "SELECT id, name FROM committees WHERE name != '' AND name IS NOT NULL ORDER BY name"
    ).fetchall()

    return render_template("hearings.html",
                           hearings=hearings,
                           page=page,
                           total_pages=total_pages,
                           total=count,
                           q=q,
                           chamber=chamber,
                           congress=congress,
                           committee=committee,
                           congresses=congresses,
                           committees=committees,
                           date_from=date_from,
                           date_to=date_to)


@app.route("/hearing/<int:hearing_id>")
def hearing_detail(hearing_id):
    db = get_connection()

    hearing = db.execute("""
        SELECT h.*,
            GROUP_CONCAT(c.name) as committee_names
        FROM hearings h
        LEFT JOIN hearing_committees hc ON h.id = hc.hearing_id
        LEFT JOIN committees c ON hc.committee_id = c.id
        WHERE h.id = ?
        GROUP BY h.id
    """, (hearing_id,)).fetchone()
    if not hearing:
        return "Hearing not found", 404

    witnesses = db.execute("""
        SELECT wa.*, w.id as witness_id, w.name as witness_name,
            w.normalized_name,
            (SELECT COUNT(*) FROM testimony t WHERE t.appearance_id = wa.id) as testimony_count,
            (SELECT COUNT(*) FROM questions_for_record q WHERE q.appearance_id = wa.id) as qfr_count
        FROM witness_appearances wa
        JOIN witnesses w ON wa.witness_id = w.id
        WHERE wa.hearing_id = ?
        ORDER BY wa.panel_number, w.last_name
    """, (hearing_id,)).fetchall()

    return render_template("hearing_detail.html",
                           hearing=hearing,
                           witnesses=witnesses)


@app.route("/hearing/<int:hearing_id>/testimony/<int:appearance_id>")
def testimony_detail(hearing_id, appearance_id):
    db = get_connection()

    appearance = db.execute("""
        SELECT wa.*, w.name as witness_name, w.id as witness_id,
            h.title as hearing_title, h.date as hearing_date,
            h.congress, h.chamber
        FROM witness_appearances wa
        JOIN witnesses w ON wa.witness_id = w.id
        JOIN hearings h ON wa.hearing_id = h.id
        WHERE wa.id = ? AND wa.hearing_id = ?
    """, (appearance_id, hearing_id)).fetchone()
    if not appearance:
        return "Testimony not found", 404

    testimony = db.execute(
        "SELECT * FROM testimony WHERE appearance_id = ?", (appearance_id,)
    ).fetchall()

    qfrs = db.execute(
        "SELECT * FROM questions_for_record WHERE appearance_id = ?", (appearance_id,)
    ).fetchall()

    return render_template("testimony_detail.html",
                           appearance=appearance,
                           testimony=testimony,
                           qfrs=qfrs)


@app.route("/titles")
def titles_list():
    db = get_connection()
    q = request.args.get("q", "").strip()
    page = request.args.get("page", 1, type=int)
    offset = (page - 1) * PER_PAGE

    if q:
        like_q = f"%{q}%"
        titles = db.execute("""
            SELECT wt.title, wt.organization,
                COUNT(DISTINCT wt.witness_id) as holder_count,
                GROUP_CONCAT(w.name) as holders
            FROM witness_titles wt
            JOIN witnesses w ON wt.witness_id = w.id
            WHERE wt.title LIKE ? OR wt.organization LIKE ?
            GROUP BY wt.title, wt.organization
            ORDER BY holder_count DESC
            LIMIT ? OFFSET ?
        """, (like_q, like_q, PER_PAGE, offset)).fetchall()

        count = db.execute("""
            SELECT COUNT(*) FROM (
                SELECT wt.title, wt.organization
                FROM witness_titles wt
                WHERE wt.title LIKE ? OR wt.organization LIKE ?
                GROUP BY wt.title, wt.organization
            )
        """, (like_q, like_q)).fetchone()[0]
    else:
        titles = db.execute("""
            SELECT wt.title, wt.organization,
                COUNT(DISTINCT wt.witness_id) as holder_count,
                GROUP_CONCAT(w.name) as holders
            FROM witness_titles wt
            JOIN witnesses w ON wt.witness_id = w.id
            GROUP BY wt.title, wt.organization
            ORDER BY holder_count DESC
            LIMIT ? OFFSET ?
        """, (PER_PAGE, offset)).fetchall()

        count = db.execute("""
            SELECT COUNT(*) FROM (
                SELECT wt.title, wt.organization
                FROM witness_titles wt
                GROUP BY wt.title, wt.organization
            )
        """).fetchone()[0]

    total_pages = math.ceil(count / PER_PAGE) if count else 0

    return render_template("titles.html",
                           titles=titles,
                           page=page,
                           total_pages=total_pages,
                           total=count,
                           q=q)


@app.route("/title/<path:title>")
def title_detail(title):
    db = get_connection()
    org = request.args.get("org", "")

    if org:
        holders = db.execute("""
            SELECT w.*, wt.start_date, wt.end_date, wt.organization
            FROM witness_titles wt
            JOIN witnesses w ON wt.witness_id = w.id
            WHERE wt.title = ? AND wt.organization = ?
            ORDER BY wt.start_date DESC
        """, (title, org)).fetchall()
    else:
        holders = db.execute("""
            SELECT w.*, wt.start_date, wt.end_date, wt.organization
            FROM witness_titles wt
            JOIN witnesses w ON wt.witness_id = w.id
            WHERE wt.title = ?
            ORDER BY wt.start_date DESC
        """, (title,)).fetchall()

    return render_template("title_detail.html",
                           title=title,
                           org=org,
                           holders=holders)


@app.route("/committees")
def committees_list():
    db = get_connection()
    committees = db.execute("""
        SELECT c.*,
            COUNT(DISTINCT hc.hearing_id) as hearing_count,
            COUNT(DISTINCT wa.witness_id) as witness_count
        FROM committees c
        LEFT JOIN hearing_committees hc ON c.id = hc.committee_id
        LEFT JOIN witness_appearances wa ON wa.hearing_id = hc.hearing_id
        WHERE c.name != '' AND c.name IS NOT NULL
        GROUP BY c.id
        HAVING hearing_count > 0
        ORDER BY hearing_count DESC
    """).fetchall()

    return render_template("committees.html", committees=committees)


@app.route("/committee/<int:committee_id>")
def committee_detail(committee_id):
    db = get_connection()
    committee = db.execute("SELECT * FROM committees WHERE id = ?", (committee_id,)).fetchone()
    if not committee:
        return "Committee not found", 404

    hearings = db.execute("""
        SELECT h.*,
            (SELECT COUNT(*) FROM witness_appearances wa WHERE wa.hearing_id = h.id) as witness_count
        FROM hearings h
        JOIN hearing_committees hc ON h.id = hc.hearing_id
        WHERE hc.committee_id = ?
        ORDER BY h.date DESC
        LIMIT 100
    """, (committee_id,)).fetchall()

    top_witnesses = db.execute("""
        SELECT w.id, w.name, w.appearance_count,
            COUNT(*) as committee_appearances,
            (SELECT GROUP_CONCAT(wt.title, '; ') FROM witness_titles wt WHERE wt.witness_id = w.id LIMIT 1) as titles
        FROM witnesses w
        JOIN witness_appearances wa ON wa.witness_id = w.id
        JOIN hearing_committees hc ON hc.hearing_id = wa.hearing_id
        WHERE hc.committee_id = ?
        GROUP BY w.id
        ORDER BY committee_appearances DESC
        LIMIT 20
    """, (committee_id,)).fetchall()

    return render_template("committee_detail.html",
                           committee=committee,
                           hearings=hearings,
                           top_witnesses=top_witnesses)


@app.route("/search")
def search():
    q = request.args.get("q", "").strip()
    if not q:
        return render_template("search.html", q="",
                               witness_results=[], hearing_results=[], title_results=[],
                               expanded_query=None)

    db = get_connection()

    # Expand abbreviations (e.g., "FAA" -> "Federal Aviation Administration")
    expanded = expand_abbreviations(q)
    search_queries = [q]
    if expanded:
        search_queries.append(expanded)

    fts_q = fts_query(q)
    like_q = f"%{q}%"

    # Search witnesses by name (FTS + LIKE fallback)
    try:
        witness_results = db.execute("""
            SELECT w.*, 'witness' as result_type,
                (SELECT GROUP_CONCAT(wt.title || CASE WHEN wt.organization IS NOT NULL AND wt.organization != ''
                    THEN ', ' || wt.organization ELSE '' END, '; ')
                 FROM witness_titles wt WHERE wt.witness_id = w.id) as titles
            FROM witnesses w
            JOIN witnesses_fts ON witnesses_fts.rowid = w.id
            WHERE witnesses_fts MATCH ?
            ORDER BY w.appearance_count DESC
            LIMIT 25
        """, (fts_q,)).fetchall()
    except Exception:
        witness_results = db.execute("""
            SELECT w.*, 'witness' as result_type,
                (SELECT GROUP_CONCAT(wt.title || CASE WHEN wt.organization IS NOT NULL AND wt.organization != ''
                    THEN ', ' || wt.organization ELSE '' END, '; ')
                 FROM witness_titles wt WHERE wt.witness_id = w.id) as titles
            FROM witnesses w
            WHERE w.name LIKE ?
            ORDER BY w.appearance_count DESC
            LIMIT 25
        """, (like_q,)).fetchall()

    # Search hearings - try original query and expanded version
    hearing_results = []
    for sq in search_queries:
        sq_fts = fts_query(sq)
        try:
            hearing_results = db.execute("""
                SELECT h.*, 'hearing' as result_type,
                    GROUP_CONCAT(c.name) as committee_names,
                    (SELECT COUNT(*) FROM witness_appearances wa WHERE wa.hearing_id = h.id) as witness_count
                FROM hearings h
                JOIN hearings_fts ON hearings_fts.rowid = h.id
                LEFT JOIN hearing_committees hc ON h.id = hc.hearing_id
                LEFT JOIN committees c ON hc.committee_id = c.id
                WHERE hearings_fts MATCH ?
                GROUP BY h.id
                ORDER BY h.date DESC
                LIMIT 25
            """, (sq_fts,)).fetchall()
        except Exception:
            hearing_results = db.execute("""
                SELECT h.*, 'hearing' as result_type,
                    GROUP_CONCAT(c.name) as committee_names,
                    (SELECT COUNT(*) FROM witness_appearances wa WHERE wa.hearing_id = h.id) as witness_count
                FROM hearings h
                LEFT JOIN hearing_committees hc ON h.id = hc.hearing_id
                LEFT JOIN committees c ON hc.committee_id = c.id
                WHERE h.title LIKE ?
                GROUP BY h.id
                ORDER BY h.date DESC
                LIMIT 25
            """, (f"%{sq}%",)).fetchall()
        if hearing_results:
            break

    # Search titles and organizations
    # Try original query AND expanded abbreviations
    title_results = []
    for sq in search_queries:
        words = [w for w in sq.split() if len(w) > 1]
        combined_field = "(wt.title || ' ' || COALESCE(wt.organization, ''))"
        if words:
            word_clauses = []
            word_params = []
            for word in words:
                word_clauses.append(f"{combined_field} LIKE ?")
                word_params.append(f"%{word}%")

            title_results = db.execute(f"""
                SELECT wt.title, wt.organization, w.name, w.id as witness_id,
                    w.appearance_count, 'title' as result_type
                FROM witness_titles wt
                JOIN witnesses w ON wt.witness_id = w.id
                WHERE {' AND '.join(word_clauses)}
                ORDER BY w.appearance_count DESC
                LIMIT 25
            """, word_params).fetchall()

            if not title_results:
                any_clauses = []
                any_params = []
                for word in words:
                    any_clauses.append(f"{combined_field} LIKE ?")
                    any_params.append(f"%{word}%")
                title_results = db.execute(f"""
                    SELECT wt.title, wt.organization, w.name, w.id as witness_id,
                        w.appearance_count, 'title' as result_type
                    FROM witness_titles wt
                    JOIN witnesses w ON wt.witness_id = w.id
                    WHERE {' OR '.join(any_clauses)}
                    ORDER BY w.appearance_count DESC
                    LIMIT 25
                """, any_params).fetchall()
        else:
            sq_like = f"%{sq}%"
            title_results = db.execute("""
                SELECT wt.title, wt.organization, w.name, w.id as witness_id,
                    w.appearance_count, 'title' as result_type
                FROM witness_titles wt
                JOIN witnesses w ON wt.witness_id = w.id
                WHERE wt.title LIKE ? OR wt.organization LIKE ?
                ORDER BY w.appearance_count DESC
                LIMIT 25
            """, (sq_like, sq_like)).fetchall()

        if title_results:
            break

    return render_template("search.html",
                           q=q,
                           witness_results=witness_results,
                           hearing_results=hearing_results,
                           title_results=title_results,
                           expanded_query=expanded)


@app.route("/statistics")
def statistics():
    db = get_connection()

    # Overall stats
    stats = {
        "witnesses": db.execute("SELECT COUNT(*) FROM witnesses WHERE appearance_count > 0").fetchone()[0],
        "hearings": db.execute("SELECT COUNT(*) FROM hearings").fetchone()[0],
        "appearances": db.execute("SELECT COUNT(*) FROM witness_appearances").fetchone()[0],
        "committees": db.execute("SELECT COUNT(*) FROM committees WHERE name != '' AND name IS NOT NULL").fetchone()[0],
        "titles": db.execute("SELECT COUNT(*) FROM (SELECT title, organization FROM witness_titles GROUP BY title, organization)").fetchone()[0],
        "has_statement": db.execute("SELECT COUNT(*) FROM witness_appearances WHERE statement_url IS NOT NULL AND statement_url != ''").fetchone()[0],
        "has_bio": db.execute("SELECT COUNT(*) FROM witness_appearances WHERE biography_url IS NOT NULL AND biography_url != ''").fetchone()[0],
    }

    # Top organizations by number of witnesses
    top_orgs = db.execute("""
        SELECT organization, COUNT(DISTINCT witness_id) as witness_count,
            COUNT(DISTINCT hearing_id) as hearing_count
        FROM witness_appearances
        WHERE organization IS NOT NULL AND organization != ''
        GROUP BY organization
        ORDER BY witness_count DESC
        LIMIT 25
    """).fetchall()

    # Most active committees - prioritize those with witness data
    top_committees = db.execute("""
        SELECT c.id, c.name, c.chamber,
            COUNT(DISTINCT hc.hearing_id) as hearing_count,
            COUNT(DISTINCT wa.witness_id) as witness_count
        FROM committees c
        JOIN hearing_committees hc ON c.id = hc.committee_id
        LEFT JOIN witness_appearances wa ON wa.hearing_id = hc.hearing_id
        WHERE c.name != '' AND c.name IS NOT NULL
        GROUP BY c.id
        ORDER BY witness_count DESC, hearing_count DESC
        LIMIT 20
    """).fetchall()

    # Repeat witnesses (appeared 3+ times)
    repeat_witnesses = db.execute("""
        SELECT w.id, w.name, w.appearance_count, w.first_appearance_date, w.last_appearance_date,
            (SELECT GROUP_CONCAT(wt.title, '; ')
             FROM (SELECT DISTINCT title FROM witness_titles WHERE witness_id = w.id) wt) as titles
        FROM witnesses w
        WHERE w.appearance_count >= 3
        AND NOT EXISTS (
            SELECT 1 FROM witness_titles wt
            WHERE wt.witness_id = w.id
            AND (wt.title LIKE '%Member of Congress%' OR wt.title LIKE '%Rep.%' OR wt.title LIKE '%Senator%')
        )
        ORDER BY w.appearance_count DESC
        LIMIT 30
    """).fetchall()

    # Hearings by month (for the current congress data)
    hearings_by_month = db.execute("""
        SELECT substr(date, 1, 7) as month, chamber, COUNT(*) as cnt
        FROM hearings
        WHERE date IS NOT NULL AND date != ''
        GROUP BY month, chamber
        ORDER BY month DESC
        LIMIT 60
    """).fetchall()

    # Coverage stats
    coverage = db.execute("""
        SELECT congress, chamber,
            COUNT(*) as hearing_count,
            SUM(CASE WHEN event_id IS NOT NULL THEN 1 ELSE 0 END) as with_event_id,
            (SELECT COUNT(DISTINCT wa.witness_id) FROM witness_appearances wa
             JOIN hearings h2 ON wa.hearing_id = h2.id
             WHERE h2.congress = h.congress AND h2.chamber = h.chamber) as witness_count,
            SUM(CASE WHEN govinfo_package_id IS NOT NULL THEN 1 ELSE 0 END) as with_transcript
        FROM hearings h
        GROUP BY congress, chamber
        ORDER BY congress DESC, chamber
    """).fetchall()

    return render_template("statistics.html",
                           stats=stats,
                           top_orgs=top_orgs,
                           top_committees=top_committees,
                           repeat_witnesses=repeat_witnesses,
                           hearings_by_month=hearings_by_month,
                           coverage=coverage)


@app.route("/about")
def about():
    return render_template("about.html")


@app.route("/api/witnesses")
def api_witnesses():
    db = get_connection()
    q = request.args.get("q", "")
    limit = min(request.args.get("limit", 100, type=int), 500)

    if q:
        fts_q = fts_query(q)
        try:
            witnesses = db.execute("""
                SELECT w.id, w.name, w.normalized_name, w.appearance_count,
                    w.first_appearance_date, w.last_appearance_date
                FROM witnesses w
                JOIN witnesses_fts ON witnesses_fts.rowid = w.id
                WHERE witnesses_fts MATCH ?
                ORDER BY w.appearance_count DESC
                LIMIT ?
            """, (fts_q, limit)).fetchall()
        except Exception:
            witnesses = db.execute("""
                SELECT id, name, normalized_name, appearance_count,
                    first_appearance_date, last_appearance_date
                FROM witnesses WHERE name LIKE ?
                ORDER BY appearance_count DESC
                LIMIT ?
            """, (f"%{q}%", limit)).fetchall()
    else:
        witnesses = db.execute("""
            SELECT id, name, normalized_name, appearance_count,
                first_appearance_date, last_appearance_date
            FROM witnesses
            WHERE appearance_count > 0
            ORDER BY appearance_count DESC
            LIMIT ?
        """, (limit,)).fetchall()

    return jsonify([dict(w) for w in witnesses])


@app.route("/api/stats")
def api_stats():
    db = get_connection()
    stats = {
        "witnesses": db.execute("SELECT COUNT(*) FROM witnesses WHERE appearance_count > 0").fetchone()[0],
        "hearings": db.execute("SELECT COUNT(*) FROM hearings").fetchone()[0],
        "appearances": db.execute("SELECT COUNT(*) FROM witness_appearances").fetchone()[0],
        "testimony": db.execute("SELECT COUNT(*) FROM testimony").fetchone()[0],
        "qfr": db.execute("SELECT COUNT(*) FROM questions_for_record").fetchone()[0],
        "committees": db.execute("SELECT COUNT(*) FROM committees WHERE name != ''").fetchone()[0],
        "written_statements": db.execute("SELECT COUNT(*) FROM witness_appearances WHERE statement_url IS NOT NULL AND statement_url != ''").fetchone()[0],
        "congresses_covered": [r[0] for r in db.execute(
            "SELECT DISTINCT congress FROM hearings ORDER BY congress DESC"
        ).fetchall()],
    }
    return jsonify(stats)


@app.route("/api/witness/<int:witness_id>")
def api_witness_detail(witness_id):
    db = get_connection()
    witness = db.execute("SELECT * FROM witnesses WHERE id = ?", (witness_id,)).fetchone()
    if not witness:
        return jsonify({"error": "Witness not found"}), 404

    result = dict(witness)
    result["witness_id"] = f"W-{witness['id']:05d}"
    result["display_name"] = display_name(witness["name"])

    titles = db.execute(
        "SELECT title, organization, start_date, end_date FROM witness_titles WHERE witness_id = ? ORDER BY start_date DESC",
        (witness_id,)
    ).fetchall()
    result["titles"] = [dict(t) for t in titles]

    appearances = db.execute("""
        SELECT wa.id, wa.position, wa.organization, wa.statement_url, wa.biography_url,
            h.id as hearing_id, h.title as hearing_title, h.date as hearing_date,
            h.congress, h.chamber
        FROM witness_appearances wa
        JOIN hearings h ON wa.hearing_id = h.id
        WHERE wa.witness_id = ?
        ORDER BY h.date DESC
    """, (witness_id,)).fetchall()
    result["appearances"] = [dict(a) for a in appearances]

    return jsonify(result)


@app.route("/api/hearing/<int:hearing_id>")
def api_hearing_detail(hearing_id):
    db = get_connection()
    hearing = db.execute("SELECT * FROM hearings WHERE id = ?", (hearing_id,)).fetchone()
    if not hearing:
        return jsonify({"error": "Hearing not found"}), 404

    result = dict(hearing)

    committees = db.execute("""
        SELECT c.id, c.name, c.system_code, c.chamber
        FROM committees c
        JOIN hearing_committees hc ON c.id = hc.committee_id
        WHERE hc.hearing_id = ?
    """, (hearing_id,)).fetchall()
    result["committees"] = [dict(c) for c in committees]

    witnesses = db.execute("""
        SELECT wa.id as appearance_id, wa.position, wa.organization,
            wa.statement_url, wa.biography_url, wa.truth_in_testimony_url,
            w.id as witness_id, w.name as witness_name
        FROM witness_appearances wa
        JOIN witnesses w ON wa.witness_id = w.id
        WHERE wa.hearing_id = ?
        ORDER BY w.last_name
    """, (hearing_id,)).fetchall()
    result["witnesses"] = [dict(w) for w in witnesses]

    return jsonify(result)


@app.route("/api/search")
def api_search():
    q = request.args.get("q", "").strip()
    if not q:
        return jsonify({"error": "Query parameter 'q' is required"}), 400

    db = get_connection()
    limit = min(request.args.get("limit", 25, type=int), 100)

    expanded = expand_abbreviations(q)
    fts_q = fts_query(q)

    # Search witnesses
    try:
        witnesses = db.execute("""
            SELECT w.id, w.name, w.appearance_count, w.first_appearance_date, w.last_appearance_date
            FROM witnesses w
            JOIN witnesses_fts ON witnesses_fts.rowid = w.id
            WHERE witnesses_fts MATCH ?
            ORDER BY w.appearance_count DESC
            LIMIT ?
        """, (fts_q, limit)).fetchall()
    except Exception:
        witnesses = db.execute("""
            SELECT id, name, appearance_count, first_appearance_date, last_appearance_date
            FROM witnesses WHERE name LIKE ?
            ORDER BY appearance_count DESC
            LIMIT ?
        """, (f"%{q}%", limit)).fetchall()

    # Search hearings
    search_q = expanded if expanded else q
    try:
        hearings = db.execute("""
            SELECT h.id, h.title, h.date, h.congress, h.chamber
            FROM hearings h
            JOIN hearings_fts ON hearings_fts.rowid = h.id
            WHERE hearings_fts MATCH ?
            ORDER BY h.date DESC
            LIMIT ?
        """, (fts_query(search_q), limit)).fetchall()
    except Exception:
        hearings = db.execute("""
            SELECT id, title, date, congress, chamber
            FROM hearings WHERE title LIKE ?
            ORDER BY date DESC
            LIMIT ?
        """, (f"%{search_q}%", limit)).fetchall()

    return jsonify({
        "query": q,
        "expanded_query": expanded,
        "witnesses": [dict(w) for w in witnesses],
        "hearings": [dict(h) for h in hearings],
    })


if __name__ == "__main__":
    init_db()
    port = int(os.environ.get("PORT", 8095))
    app.run(debug=True, port=port, host="0.0.0.0")
