# Congressional Witness Database - Implementation Plan

## Goal
Build a searchable database of all witnesses who have testified before House and Senate committees, with unique IDs, hearing associations, testimony transcripts, summaries, and Questions for the Record.

## Architecture

### Data Sources
1. **Congress.gov Committee Meeting API** (`/v3/committee-meeting`) - Structured witness data (name, org, position, PDF documents) from ~116th Congress onward
2. **GovInfo API** (`/packages/{id}/htm`) - Full transcript text for extracting testimony, Q&A, and QFR sections (46,235+ hearings)
3. **Congress.gov Hearing API** (`/v3/hearing`) - Hearing metadata and links to published transcripts

### Tech Stack
- **Backend**: Python with Flask
- **Database**: SQLite (portable, no server needed)
- **Frontend**: HTML/CSS/JS with server-rendered templates
- **Data Processing**: Python scripts for API ingestion and transcript parsing

### Database Schema
- `witnesses` - Unique witness records (ID, name, normalized_name, titles held)
- `hearings` - Individual hearing/proceeding records
- `witness_appearances` - Join table linking witnesses to hearings with role/position at time of testimony
- `testimony` - Written testimony text and summaries
- `qfr` - Questions for the Record with answers
- `committees` - Committee information

### Key Features
1. **Witness profiles** - Each witness gets a unique page with all appearances, titles, and testimony
2. **Search** - Full-text search by witness name, title, organization, hearing topic
3. **Title tracking** - Track witnesses by official title (e.g., "FAA Administrator") across different holders
4. **Hearing detail pages** - Complete witness panel, testimony, and QFR for each hearing
5. **Cross-referencing** - Link witnesses across multiple appearances

## Implementation Phases

### Phase 1: Data Collection Infrastructure
- Build API client for Congress.gov and GovInfo
- Create database schema
- Implement data ingestion pipeline

### Phase 2: Data Processing
- Collect committee meeting data with witness info from Congress.gov
- Fetch hearing transcripts from GovInfo
- Parse transcripts to extract witness statements, Q&A, QFR
- Entity resolution to link witness appearances across hearings

### Phase 3: Web Application
- Build Flask app with witness profiles, hearing pages, search
- Create responsive UI suitable for Hill staffers
- Add filtering by committee, congress, date range, chamber

### Phase 4: Polish & Iterate
- Get feedback from DC agent
- Improve UI/UX
- Add data quality improvements
