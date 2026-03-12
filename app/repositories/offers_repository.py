from datetime import datetime, timezone
from typing import Any

from motor.motor_asyncio import AsyncIOMotorDatabase


class OffersRepository:
    def __init__(self, db: AsyncIOMotorDatabase) -> None:
        self._collection = db.get_collection("offers")

    async def save(self, url: str, offers: list[dict[str, Any]]) -> None:
        payload = {
            "url": url,
            "offers": offers,
            "updated_at": datetime.now(timezone.utc),
        }
        await self._collection.update_one({"url": url}, {"$set": payload}, upsert=True)
