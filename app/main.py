import logging
import os
from collections.abc import AsyncIterator

from fastapi import FastAPI

from app.api import router as product_router
from app.db import close_mongo, init_mongo


def create_app() -> FastAPI:
    logging.basicConfig(level=logging.INFO)
    app = FastAPI(title="Scraper API", lifespan=lifespan)
    app.include_router(product_router)
    return app


async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    await init_mongo(os.getenv("MONGO_URI"))
    try:
        yield
    finally:
        await close_mongo()


app = create_app()
