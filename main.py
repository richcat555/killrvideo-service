from astrapy.data_types import DataAPIVector
from fastapi import FastAPI, HTTPException, Query

from db import get_db

app = FastAPI(title="KillrVideo Service")

VIDEO_PROJECTION = {"videoid": True, "name": True, "youtube_id": True, "category": True}


# -------------------------
# Exercise #4 — Health check
# -------------------------
@app.get("/health")
def health():
    try:
        db = get_db()
        tables = db.list_table_names()
        return {"status": "ok", "tables": tables}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# -------------------------
# Exercise #5 — List videos
# -------------------------
@app.get("/videos")
def list_videos(limit: int = Query(10, ge=1, le=50)):
    try:
        db = get_db()
        table = db.get_table("videos")
        rows = table.find({}, limit=limit, projection=VIDEO_PROJECTION)
        return [
            {
                "videoid": str(r["videoid"]),
                "name": r["name"],
                "youtube_id": r.get("youtube_id"),
                "category": r.get("category"),
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
    """Return videos similar to the given videoid using ANN on content_features."""
    try:
        db = get_db()
        table = db.get_table("videos")

        # 1) Fetch the base video's embedding vector
        base = table.find_one({"videoid": videoid})

        if not base or not base.get("content_features"):
            raise HTTPException(status_code=404, detail="Video not found or missing embedding")

        vec = base["content_features"]

        # 2) ANN search using the vector
        rows = table.find(
            {},
            sort={"content_features": DataAPIVector(vec)},
            limit=limit + 1,
            projection={"videoid": True, "name": True, "category": True},
            include_similarity=True,
        )

        # Exclude the source video itself
        return [
            {
                "videoid": str(r["videoid"]),
                "name": r["name"],
                "category": r.get("category"),
                "similarity": r.get("$similarity"),
            }
            for r in rows
            if str(r["videoid"]) != videoid
        ][:limit]

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
