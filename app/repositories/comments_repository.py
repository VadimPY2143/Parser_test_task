from datetime import datetime, timezone
from typing import Any

from motor.motor_asyncio import AsyncIOMotorDatabase


class CommentsRepository:
    def __init__(self, db: AsyncIOMotorDatabase) -> None:
        self._collection = db.get_collection("comments")

    async def save(self, url: str, comments: list[dict[str, Any]]) -> None:
        payload = {
            "url": url,
            "comments": comments,
            "updated_at": datetime.now(timezone.utc),
        }
        await self._collection.update_one({"url": url}, {"$set": payload}, upsert=True)
