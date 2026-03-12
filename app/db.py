from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase

_MONGO_URI = "mongodb://localhost:27017/scraper"
_MONGO_DB = "scraper"

_client: AsyncIOMotorClient | None = None
_db: AsyncIOMotorDatabase | None = None


def get_db() -> AsyncIOMotorDatabase:
    if _db is None:
        raise RuntimeError("MongoDB not initialized")
    return _db


async def init_mongo(mongo_uri: str | None) -> None:
    global _client, _db
    if _client is None:
        uri = mongo_uri or _MONGO_URI
        _client = AsyncIOMotorClient(uri)
        _db = _client.get_database(_MONGO_DB)


async def close_mongo() -> None:
    global _client, _db
    if _client is not None:
        _client.close()
    _client = None
    _db = None

