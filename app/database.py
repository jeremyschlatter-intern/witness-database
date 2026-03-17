"""Database schema and connection management for the Congressional Witness Database."""

import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "witnesses.db")


def get_db():
    """Get a database connection with row factory."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    """Initialize the database schema."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS committees (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            system_code TEXT UNIQUE NOT NULL,
            name TEXT NOT NULL,
            chamber TEXT,  -- 'House', 'Senate', 'Joint'
            parent_code TEXT,
            url TEXT
        );

        CREATE TABLE IF NOT EXISTS hearings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            congress INTEGER NOT NULL,
            chamber TEXT NOT NULL,
            title TEXT NOT NULL,
            date TEXT,
            hearing_type TEXT,  -- 'Hearing', 'Markup', 'Meeting'
            location_building TEXT,
            location_room TEXT,
            -- Congress.gov identifiers
            event_id TEXT UNIQUE,
            jacket_number TEXT,
            -- GovInfo identifiers
            govinfo_package_id TEXT,
            -- URLs
            transcript_url TEXT,
            transcript_pdf_url TEXT,
            congress_gov_url TEXT,
            -- Status
            has_transcript INTEGER DEFAULT 0,
            has_parsed_witnesses INTEGER DEFAULT 0,
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS witnesses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            normalized_name TEXT NOT NULL,
            first_name TEXT,
            last_name TEXT,
            -- Aggregated info
            appearance_count INTEGER DEFAULT 0,
            first_appearance_date TEXT,
            last_appearance_date TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS witness_titles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            witness_id INTEGER NOT NULL REFERENCES witnesses(id),
            title TEXT NOT NULL,  -- e.g., 'FAA Administrator'
            organization TEXT,
            start_date TEXT,  -- approximate from hearing dates
            end_date TEXT,
            UNIQUE(witness_id, title, organization)
        );

        CREATE TABLE IF NOT EXISTS witness_appearances (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            witness_id INTEGER NOT NULL REFERENCES witnesses(id),
            hearing_id INTEGER NOT NULL REFERENCES hearings(id),
            position TEXT,  -- title at time of appearance
            organization TEXT,  -- org at time of appearance
            panel_number INTEGER,
            -- Document URLs from congress.gov
            biography_url TEXT,
            statement_url TEXT,
            truth_in_testimony_url TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            UNIQUE(witness_id, hearing_id)
        );

        CREATE TABLE IF NOT EXISTS hearing_committees (
            hearing_id INTEGER NOT NULL REFERENCES hearings(id),
            committee_id INTEGER NOT NULL REFERENCES committees(id),
            PRIMARY KEY (hearing_id, committee_id)
        );

        CREATE TABLE IF NOT EXISTS testimony (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            appearance_id INTEGER NOT NULL REFERENCES witness_appearances(id),
            testimony_type TEXT NOT NULL,  -- 'written_statement', 'oral_statement', 'prepared_remarks'
            content TEXT,
            summary TEXT,
            source TEXT,  -- 'transcript_parse', 'pdf_download', 'api'
            created_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS questions_for_record (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            appearance_id INTEGER NOT NULL REFERENCES witness_appearances(id),
            questioner_name TEXT,
            questioner_title TEXT,  -- e.g., 'Senator', 'Representative'
            question_text TEXT,
            answer_text TEXT,
            source TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        );

        -- Indexes for common queries
        CREATE INDEX IF NOT EXISTS idx_witnesses_normalized_name ON witnesses(normalized_name);
        CREATE INDEX IF NOT EXISTS idx_witnesses_last_name ON witnesses(last_name);
        CREATE INDEX IF NOT EXISTS idx_hearings_congress ON hearings(congress);
        CREATE INDEX IF NOT EXISTS idx_hearings_chamber ON hearings(chamber);
        CREATE INDEX IF NOT EXISTS idx_hearings_date ON hearings(date);
        CREATE INDEX IF NOT EXISTS idx_hearings_event_id ON hearings(event_id);
        CREATE INDEX IF NOT EXISTS idx_appearances_witness ON witness_appearances(witness_id);
        CREATE INDEX IF NOT EXISTS idx_appearances_hearing ON witness_appearances(hearing_id);
        CREATE INDEX IF NOT EXISTS idx_titles_witness ON witness_titles(witness_id);
        CREATE INDEX IF NOT EXISTS idx_titles_title ON witness_titles(title);
        CREATE INDEX IF NOT EXISTS idx_testimony_appearance ON testimony(appearance_id);
        CREATE INDEX IF NOT EXISTS idx_qfr_appearance ON questions_for_record(appearance_id);

        -- Full-text search
        CREATE VIRTUAL TABLE IF NOT EXISTS witnesses_fts USING fts5(
            name, content='witnesses', content_rowid='id',
            tokenize='porter unicode61'
        );

        CREATE VIRTUAL TABLE IF NOT EXISTS hearings_fts USING fts5(
            title, content='hearings', content_rowid='id',
            tokenize='porter unicode61'
        );

        CREATE VIRTUAL TABLE IF NOT EXISTS testimony_fts USING fts5(
            content, summary, content='testimony', content_rowid='id',
            tokenize='porter unicode61'
        );

        -- Triggers to keep FTS in sync
        CREATE TRIGGER IF NOT EXISTS witnesses_ai AFTER INSERT ON witnesses BEGIN
            INSERT INTO witnesses_fts(rowid, name) VALUES (new.id, new.name);
        END;
        CREATE TRIGGER IF NOT EXISTS witnesses_au AFTER UPDATE ON witnesses BEGIN
            INSERT INTO witnesses_fts(witnesses_fts, rowid, name) VALUES('delete', old.id, old.name);
            INSERT INTO witnesses_fts(rowid, name) VALUES (new.id, new.name);
        END;

        CREATE TRIGGER IF NOT EXISTS hearings_ai AFTER INSERT ON hearings BEGIN
            INSERT INTO hearings_fts(rowid, title) VALUES (new.id, new.title);
        END;
        CREATE TRIGGER IF NOT EXISTS hearings_au AFTER UPDATE ON hearings BEGIN
            INSERT INTO hearings_fts(hearings_fts, rowid, title) VALUES('delete', old.id, old.title);
            INSERT INTO hearings_fts(rowid, title) VALUES (new.id, new.title);
        END;

        CREATE TRIGGER IF NOT EXISTS testimony_ai AFTER INSERT ON testimony BEGIN
            INSERT INTO testimony_fts(rowid, content, summary) VALUES (new.id, new.content, new.summary);
        END;
        CREATE TRIGGER IF NOT EXISTS testimony_au AFTER UPDATE ON testimony BEGIN
            INSERT INTO testimony_fts(testimony_fts, rowid, content, summary) VALUES('delete', old.id, old.content, old.summary);
            INSERT INTO testimony_fts(rowid, content, summary) VALUES (new.id, new.content, new.summary);
        END;
    """)
    conn.commit()
    conn.close()
    print(f"Database initialized at {DB_PATH}")


if __name__ == "__main__":
    init_db()
