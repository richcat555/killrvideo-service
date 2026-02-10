# KillrVideo – Python Backend (FastAPI + Astra DB)

A lightweight REST API service for the KillrVideo dataset, built with **FastAPI** and **astrapy** (Data API Table API) connecting to **Astra DB**.

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Health check — lists table names to verify connectivity |
| GET | `/api/v1/videos?limit=N` | List videos (default 10, max 50) |
| GET | `/api/v1/videos/{videoid}` | Get a single video by UUID |
| GET | `/api/v1/videos/{videoid}/related?limit=N` | ANN vector search for related videos (default 5, max 20) |

## Workshop Exercises Completed

- **#4a** — Health check using `db.list_table_names()`
- **#4b** — Astra DB client initialization via `DataAPIClient` + `get_database()`
- **#5b** — List videos using `table.find()` with filter, limit, and projection
- **#6a** — Fetch source video's embedding vector via `table.find_one()`
- **#6b** — ANN vector search using `DataAPIVector` sort on `content_features`

## Setup

1. Clone the repo and create a virtual environment:
   ```bash
   git clone https://github.com/richcat555/killrvideo-service.git
   cd killrvideo-service
   python3 -m venv venv
   source venv/bin/activate
   ```

2. Install dependencies:
   ```bash
   pip install fastapi uvicorn astrapy python-dotenv
   ```

3. Create a `.env` file (see `.env.example`):
   ```
   ASTRA_DB_API_ENDPOINT=https://your-db-id.apps.astra.datastax.com
   ASTRA_DB_APPLICATION_TOKEN=AstraCS:your-token-here
   ASTRA_DB_KEYSPACE=killrvideo
   ```

4. Run the server:
   ```bash
   uvicorn main:app --reload --host 0.0.0.0 --port 8000
   ```

5. Test:
   ```bash
   curl http://localhost:8000/health
   curl "http://localhost:8000/api/v1/videos?limit=3"
   curl "http://localhost:8000/api/v1/videos/{videoid}/related?limit=5"
   ```

## Tech Stack

- **FastAPI** — Python web framework with automatic OpenAPI docs at `/docs`
- **astrapy** — DataStax Data API client (Table API)
- **Astra DB** — Serverless Cassandra with vector search support

## Project Structure

```
killrvideo-service/
├── db.py          # Astra DB connection (DataAPIClient singleton)
├── main.py        # FastAPI app with all endpoints
├── .env           # Environment variables (not committed)
├── .env.example   # Example environment configuration
└── .gitignore
```
