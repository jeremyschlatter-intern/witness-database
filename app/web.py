"""Flask web application for the Congressional Witness Database."""

import math
import os
import sys

from flask import Flask, render_template, request, jsonify, g

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from app.database import get_db, init_db

app = Flask(
    __name__,
    template_folder=os.path.join(os.path.dirname(os.path.dirname(__file__)), "templates"),
    static_folder=os.path.join(os.path.dirname(os.path.dirname(__file__)), "static"),
)

PER_PAGE = 50


def get_connection():
    if "db" not in g:
        g.db = get_db()
    return g.db


@app.teardown_appcontext
def close_db(exception):
    db = g.pop("db", None)
    if db:
        db.close()


@app.context_processor
def inject_stats():
    """Inject global stats into all templates."""
    db = get_connection()
    return {
        "total_witnesses": db.execute("SELECT COUNT(*) FROM witnesses").fetchone()[0],
        "total_hearings": db.execute("SELECT COUNT(*) FROM hearings").fetchone()[0],
        "total_appearances": db.execute("SELECT COUNT(*) FROM witness_appearances").fetchone()[0],
    }


@app.route("/")
def index():
    db = get_connection()

    # Recent hearings
    recent_hearings = db.execute("""
        SELECT h.*, GROUP_CONCAT(c.name, '; ') as committee_names
        FROM hearings h
        LEFT JOIN hearing_committees hc ON h.id = hc.hearing_id
        LEFT JOIN committees c ON hc.committee_id = c.id
        WHERE h.date IS NOT NULL
        GROUP BY h.id
        ORDER BY h.date DESC
        LIMIT 10
    """).fetchall()

    # Top witnesses by appearances
    top_witnesses = db.execute("""
        SELECT w.*,
            (SELECT GROUP_CONCAT(DISTINCT wt.title, '; ')
             FROM witness_titles wt WHERE wt.witness_id = w.id LIMIT 3) as titles
        FROM witnesses w
        WHERE w.appearance_count > 0
        ORDER BY w.appearance_count DESC
        LIMIT 15
    """).fetchall()

    # Stats by congress
    congress_stats = db.execute("""
        SELECT congress, chamber, COUNT(*) as hearing_count,
            COUNT(DISTINCT wa.witness_id) as witness_count
        FROM hearings h
        LEFT JOIN witness_appearances wa ON h.id = wa.hearing_id
        GROUP BY congress, chamber
        ORDER BY congress DESC, chamber
    """).fetchall()

    return render_template("index.html",
                           recent_hearings=recent_hearings,
                           top_witnesses=top_witnesses,
                           congress_stats=congress_stats)


@app.route("/witnesses")
def witnesses_list():
    db = get_connection()
    page = request.args.get("page", 1, type=int)
    sort = request.args.get("sort", "appearances")
    q = request.args.get("q", "").strip()
    chamber = request.args.get("chamber", "")
    congress = request.args.get("congress", "", type=str)

    offset = (page - 1) * PER_PAGE

    if q:
        # Full-text search
        count = db.execute("""
            SELECT COUNT(DISTINCT w.id)
            FROM witnesses w
            JOIN witnesses_fts ON witnesses_fts.rowid = w.id
            WHERE witnesses_fts MATCH ?
        """, (q,)).fetchone()[0]

        witnesses = db.execute("""
            SELECT w.*,
                (SELECT GROUP_CONCAT(DISTINCT wt.title, '; ')
                 FROM witness_titles wt WHERE wt.witness_id = w.id) as titles
            FROM witnesses w
            JOIN witnesses_fts ON witnesses_fts.rowid = w.id
            WHERE witnesses_fts MATCH ?
            ORDER BY rank
            LIMIT ? OFFSET ?
        """, (q, PER_PAGE, offset)).fetchall()
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

        witnesses = db.execute(f"""
            SELECT w.*,
                (SELECT GROUP_CONCAT(DISTINCT wt.title, '; ')
                 FROM witness_titles wt WHERE wt.witness_id = w.id) as titles
            FROM witnesses w
            WHERE {where}
            ORDER BY {order}
            LIMIT ? OFFSET ?
        """, params + [PER_PAGE, offset]).fetchall()

    total_pages = math.ceil(count / PER_PAGE)

    # Get distinct congresses for filter
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

    offset = (page - 1) * PER_PAGE

    where_clauses = ["1=1"]
    params = []

    if q:
        where_clauses.append("""
            h.id IN (SELECT rowid FROM hearings_fts WHERE hearings_fts MATCH ?)
        """)
        params.append(q)

    if chamber:
        where_clauses.append("h.chamber = ?")
        params.append(chamber)

    if congress:
        where_clauses.append("h.congress = ?")
        params.append(int(congress))

    if committee:
        where_clauses.append("""
            EXISTS (SELECT 1 FROM hearing_committees hc
            JOIN committees c ON hc.committee_id = c.id
            WHERE hc.hearing_id = h.id AND c.id = ?)
        """)
        params.append(int(committee))

    where = " AND ".join(where_clauses)

    count = db.execute(f"SELECT COUNT(*) FROM hearings h WHERE {where}", params).fetchone()[0]

    hearings = db.execute(f"""
        SELECT h.*,
            GROUP_CONCAT(DISTINCT c.name) as committee_names,
            (SELECT COUNT(*) FROM witness_appearances wa WHERE wa.hearing_id = h.id) as witness_count
        FROM hearings h
        LEFT JOIN hearing_committees hc ON h.id = hc.hearing_id
        LEFT JOIN committees c ON hc.committee_id = c.id
        WHERE {where}
        GROUP BY h.id
        ORDER BY h.date DESC NULLS LAST
        LIMIT ? OFFSET ?
    """, params + [PER_PAGE, offset]).fetchall()

    total_pages = math.ceil(count / PER_PAGE)

    congresses = db.execute(
        "SELECT DISTINCT congress FROM hearings ORDER BY congress DESC"
    ).fetchall()

    committees = db.execute(
        "SELECT id, name FROM committees ORDER BY name"
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
                           committees=committees)


@app.route("/hearing/<int:hearing_id>")
def hearing_detail(hearing_id):
    db = get_connection()

    hearing = db.execute("""
        SELECT h.*,
            GROUP_CONCAT(DISTINCT c.name) as committee_names
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

    testimony = db.execute("""
        SELECT * FROM testimony WHERE appearance_id = ?
    """, (appearance_id,)).fetchall()

    qfrs = db.execute("""
        SELECT * FROM questions_for_record WHERE appearance_id = ?
    """, (appearance_id,)).fetchall()

    return render_template("testimony_detail.html",
                           appearance=appearance,
                           testimony=testimony,
                           qfrs=qfrs)


@app.route("/titles")
def titles_list():
    """Browse witnesses by their official titles."""
    db = get_connection()
    q = request.args.get("q", "").strip()
    page = request.args.get("page", 1, type=int)
    offset = (page - 1) * PER_PAGE

    if q:
        titles = db.execute("""
            SELECT wt.title, wt.organization,
                COUNT(DISTINCT wt.witness_id) as holder_count,
                GROUP_CONCAT(DISTINCT w.name) as holders
            FROM witness_titles wt
            JOIN witnesses w ON wt.witness_id = w.id
            WHERE wt.title LIKE ? OR wt.organization LIKE ?
            GROUP BY wt.title, wt.organization
            ORDER BY holder_count DESC
            LIMIT ? OFFSET ?
        """, (f"%{q}%", f"%{q}%", PER_PAGE, offset)).fetchall()

        count = db.execute("""
            SELECT COUNT(DISTINCT wt.title || wt.organization)
            FROM witness_titles wt
            WHERE wt.title LIKE ? OR wt.organization LIKE ?
        """, (f"%{q}%", f"%{q}%")).fetchone()[0]
    else:
        titles = db.execute("""
            SELECT wt.title, wt.organization,
                COUNT(DISTINCT wt.witness_id) as holder_count,
                GROUP_CONCAT(DISTINCT w.name) as holders
            FROM witness_titles wt
            JOIN witnesses w ON wt.witness_id = w.id
            GROUP BY wt.title, wt.organization
            ORDER BY holder_count DESC
            LIMIT ? OFFSET ?
        """, (PER_PAGE, offset)).fetchall()

        count = db.execute("""
            SELECT COUNT(DISTINCT wt.title || COALESCE(wt.organization, ''))
            FROM witness_titles wt
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
    """Show all people who have held a specific title."""
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
        GROUP BY c.id
        ORDER BY hearing_count DESC
    """).fetchall()

    return render_template("committees.html", committees=committees)


@app.route("/search")
def search():
    q = request.args.get("q", "").strip()
    if not q:
        return render_template("search.html", q="", results=None)

    db = get_connection()

    # Search witnesses
    witness_results = db.execute("""
        SELECT w.*, 'witness' as result_type
        FROM witnesses w
        JOIN witnesses_fts ON witnesses_fts.rowid = w.id
        WHERE witnesses_fts MATCH ?
        ORDER BY rank
        LIMIT 20
    """, (q,)).fetchall()

    # Search hearings
    hearing_results = db.execute("""
        SELECT h.*, 'hearing' as result_type,
            GROUP_CONCAT(DISTINCT c.name) as committee_names
        FROM hearings h
        JOIN hearings_fts ON hearings_fts.rowid = h.id
        LEFT JOIN hearing_committees hc ON h.id = hc.hearing_id
        LEFT JOIN committees c ON hc.committee_id = c.id
        WHERE hearings_fts MATCH ?
        GROUP BY h.id
        ORDER BY rank
        LIMIT 20
    """, (q,)).fetchall()

    # Search titles
    title_results = db.execute("""
        SELECT wt.title, wt.organization, w.name, w.id as witness_id,
            'title' as result_type
        FROM witness_titles wt
        JOIN witnesses w ON wt.witness_id = w.id
        WHERE wt.title LIKE ? OR wt.organization LIKE ?
        LIMIT 20
    """, (f"%{q}%", f"%{q}%")).fetchall()

    return render_template("search.html",
                           q=q,
                           witness_results=witness_results,
                           hearing_results=hearing_results,
                           title_results=title_results)


@app.route("/api/witnesses")
def api_witnesses():
    """JSON API for witness data."""
    db = get_connection()
    q = request.args.get("q", "")
    limit = min(request.args.get("limit", 100, type=int), 500)

    if q:
        witnesses = db.execute("""
            SELECT w.id, w.name, w.normalized_name, w.appearance_count,
                w.first_appearance_date, w.last_appearance_date
            FROM witnesses w
            JOIN witnesses_fts ON witnesses_fts.rowid = w.id
            WHERE witnesses_fts MATCH ?
            ORDER BY rank
            LIMIT ?
        """, (q, limit)).fetchall()
    else:
        witnesses = db.execute("""
            SELECT id, name, normalized_name, appearance_count,
                first_appearance_date, last_appearance_date
            FROM witnesses
            ORDER BY appearance_count DESC
            LIMIT ?
        """, (limit,)).fetchall()

    return jsonify([dict(w) for w in witnesses])


@app.route("/api/stats")
def api_stats():
    """JSON API for database statistics."""
    db = get_connection()
    stats = {
        "witnesses": db.execute("SELECT COUNT(*) FROM witnesses").fetchone()[0],
        "hearings": db.execute("SELECT COUNT(*) FROM hearings").fetchone()[0],
        "appearances": db.execute("SELECT COUNT(*) FROM witness_appearances").fetchone()[0],
        "testimony": db.execute("SELECT COUNT(*) FROM testimony").fetchone()[0],
        "qfr": db.execute("SELECT COUNT(*) FROM questions_for_record").fetchone()[0],
        "committees": db.execute("SELECT COUNT(*) FROM committees").fetchone()[0],
    }
    return jsonify(stats)


if __name__ == "__main__":
    init_db()
    port = int(os.environ.get("PORT", 8095))
    app.run(debug=True, port=port, host="0.0.0.0")
