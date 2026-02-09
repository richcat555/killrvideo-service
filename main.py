import uuid as _uuid

from fastapi import FastAPI, HTTPException, Query
from db import get_session

app = FastAPI(title="KillrVideo Service")

# -------------------------
# Exercise #4 — Health check
# -------------------------
@app.get("/health")
def health():
    try:
        session = get_session()
        row = session.execute("SELECT now() FROM system.local").one()
        return {"status": "ok", "now": str(row[0])}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# -------------------------
# Exercise #5 — List videos
# -------------------------
@app.get("/videos")
def list_videos(limit: int = Query(10, ge=1, le=50)):
    try:
        session = get_session()
        rows = session.execute(
            "SELECT videoid, name, youtube_id, category FROM videos LIMIT %s",
            (limit,)
        )
        return [
            {
                "videoid": str(r.videoid),
                "name": r.name,
                "youtube_id": r.youtube_id,
                "category": r.category,
            }
            for r in rows
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# -------------------------
# Exercise #6 — Related videos (ANN vector search)
# -------------------------
@app.get("/videos/{videoid}/related")
def related_videos(videoid: str, limit: int = Query(5, ge=1, le=20)):
    """
    Exercise #6 — return videos similar to the given videoid using ANN on content_features
    """
    try:
        session = get_session()

        # 1) Fetch the base video's embedding vector
        base = session.execute(
            "SELECT content_features FROM videos WHERE videoid = %s",
            (_uuid.UUID(videoid),)
        ).one()

        if not base or not base.content_features:
            raise HTTPException(status_code=404, detail="Video not found or missing embedding")

        vec = list(base.content_features)

        # 2) ANN query: vector literal must be inlined in CQL
        vec_literal = "[" + ",".join(f"{float(x):.8f}" for x in vec) + "]"

        cql = f"""
        SELECT videoid, name, category
        FROM videos
        ORDER BY content_features ANN OF {vec_literal}
        LIMIT {limit}
        """

        rows = session.execute(cql)

        # Exclude the source video itself
        return [
            {"videoid": str(r.videoid), "name": r.name, "category": r.category}
            for r in rows
            if str(r.videoid) != videoid
        ]

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
