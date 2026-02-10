import re
from uuid import UUID

from astrapy.data_types import DataAPIVector
from fastapi import APIRouter, FastAPI, HTTPException, Query

from db import get_db


def _validate_uuid(value: str) -> str:
    try:
        UUID(value)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid UUID: {value}")
    return value


def _video_summary(r: dict) -> dict:
    """Map a DB row to the VideoSummary shape expected by the frontend."""
    return {
        "videoId": str(r["videoid"]),
        "title": r.get("name", ""),
        "thumbnailUrl": r.get("preview_image_location"),
        "userId": str(r["userid"]) if r.get("userid") else "",
        "submittedAt": str(r["added_date"]) if r.get("added_date") else "",
        "content_rating": r.get("content_rating"),
        "category": r.get("category"),
        "viewCount": r.get("views", 0) or 0,
        "averageRating": None,
    }


def _extract_youtube_id(url: str) -> str | None:
    """Extract YouTube video ID from a URL like https://www.youtube.com/embed/69sHSF0iUqg"""
    if not url:
        return None
    m = re.search(r"(?:embed/|watch\?v=|youtu\.be/)([a-zA-Z0-9_-]{11})", url)
    return m.group(1) if m else None


def _video_detail(r: dict) -> dict:
    """Map a DB row to the VideoDetailResponse shape expected by the frontend."""
    return {
        "videoId": str(r["videoid"]),
        "title": r.get("name", ""),
        "description": r.get("description"),
        "tags": list(r["tags"]) if r.get("tags") else [],
        "userId": str(r["userid"]) if r.get("userid") else "",
        "submittedAt": str(r["added_date"]) if r.get("added_date") else "",
        "thumbnailUrl": r.get("preview_image_location"),
        "location": r.get("location", ""),
        "location_type": r.get("location_type", 0) or 0,
        "content_rating": r.get("content_rating"),
        "category": r.get("category"),
        "language": r.get("language"),
        "youtubeVideoId": r.get("youtube_id") or _extract_youtube_id(r.get("location", "")),
        "status": "COMPLETE",
        "viewCount": r.get("views", 0) or 0,
        "averageRating": None,
    }


app = FastAPI(title="KillrVideo Service")
router = APIRouter(prefix="/api/v1")

SUMMARY_PROJECTION = {
    "videoid": True, "name": True, "youtube_id": True, "category": True,
    "preview_image_location": True, "userid": True, "added_date": True,
    "content_rating": True, "views": True,
}


# -------------------------
# Exercise #4 — Health check
# -------------------------
@app.get("/health")
def health():
    try:
        db = get_db()
        # WORKSHOP EXERCISE #4a
        # Use list_table_names() to verify Astra DB connectivity.
        tables = db.list_table_names()
        return {"status": "ok", "tables": tables}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# -------------------------
# Exercise #5 — List videos
# -------------------------
@router.get("/videos")
def list_videos(limit: int = Query(10, ge=1, le=50)):
    try:
        db = get_db()
        table = db.get_table("videos")
        # WORKSHOP EXERCISE #5b
        # Call find() on the table, passing named parameters for filter, skip, limit, and projection.
        rows = table.find({}, limit=limit, projection=SUMMARY_PROJECTION)
        return [_video_summary(r) for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# -------------------------
# Latest videos (for frontend homepage)
# -------------------------
@router.get("/videos/latest")
def latest_videos(page: int = Query(1, ge=1), page_size: int = Query(10, ge=1, le=50)):
    try:
        db = get_db()
        table = db.get_table("videos")
        rows = table.find({}, limit=page_size, projection=SUMMARY_PROJECTION)
        data = [_video_summary(r) for r in rows]
        return {
            "data": data,
            "pagination": {
                "currentPage": page,
                "pageSize": page_size,
                "totalItems": len(data),
                "totalPages": 1,
            },
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# -------------------------
# Get single video by ID (frontend uses /videos/id/{videoid})
# -------------------------
@router.get("/videos/id/{videoid}")
def get_video(videoid: str):
    _validate_uuid(videoid)
    try:
        db = get_db()
        table = db.get_table("videos")
        row = table.find_one({"videoid": videoid})
        if not row:
            raise HTTPException(status_code=404, detail="Video not found")
        return _video_detail(row)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# -------------------------
# Exercise #6 — Related videos (ANN vector search)
# -------------------------
@router.get("/videos/id/{videoid}/related")
def related_videos(videoid: str, limit: int = Query(5, ge=1, le=20)):
    """Return videos similar to the given videoid using ANN on content_features."""
    _validate_uuid(videoid)
    try:
        db = get_db()
        table = db.get_table("videos")

        # WORKSHOP EXERCISE #6a
        # Fetch the base video's embedding vector using find_one().
        base = table.find_one({"videoid": videoid})

        if not base or not base.get("content_features"):
            raise HTTPException(status_code=404, detail="Video not found or missing embedding")

        vec = base["content_features"]

        # WORKSHOP EXERCISE #6b
        # Run ANN vector search using DataAPIVector sort on content_features.
        rows = table.find(
            {},
            sort={"content_features": DataAPIVector(vec)},
            limit=limit + 1,
            projection=SUMMARY_PROJECTION,
            include_similarity=True,
        )

        # Exclude the source video and map to RecommendationItem shape
        results = []
        for r in rows:
            if str(r["videoid"]) == videoid:
                continue
            results.append({
                "videoId": str(r["videoid"]),
                "title": r.get("name", ""),
                "uploadDate": str(r["added_date"]) if r.get("added_date") else None,
                "thumbnailUrl": r.get("preview_image_location"),
                "score": r.get("$similarity"),
                "views": r.get("views", 0) or 0,
                "averageRating": 0,
            })
            if len(results) >= limit:
                break

        return results

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


app.include_router(router)
