import asyncio
import logging
from datetime import date, datetime, time

from fastapi import APIRouter, Depends, HTTPException, Query
from playwright.async_api import TimeoutError as PlaywrightTimeoutError

from app.db import get_db
from app.models import CommentItem, CommentsResponse, OfferItem, OffersResponse
from app.repositories import CommentsRepository, OffersRepository
from app.services import CommentService, HotlineService

router = APIRouter(prefix="/product", tags=["product"])
logger = logging.getLogger(__name__)


def get_hotline_service() -> HotlineService:
    return HotlineService(OffersRepository(get_db()))


def get_comment_service() -> CommentService:
    return CommentService(CommentsRepository(get_db()))


@router.get("/offers", response_model=OffersResponse)
async def offers_endpoint(
    url: str = Query(..., min_length=5),
    timeout_limit: int | None = Query(None, ge=1, le=120),
    price_sort: str | None = Query(None, pattern="^(asc|desc)$"),
    count_limit: int | None = Query(None, ge=1, le=500),
    service: HotlineService = Depends(get_hotline_service),
) -> OffersResponse:
    try:
        clean_url, offers = await service.fetch_offers(url, timeout_limit, price_sort, count_limit)
    except (asyncio.TimeoutError, PlaywrightTimeoutError):
        raise HTTPException(status_code=408, detail="Timeout while parsing offers")
    except Exception as exc:
        logger.exception("Failed to parse offers", exc_info=exc)
        raise HTTPException(status_code=500, detail=f"Unexpected error: {exc}") from exc

    api_offers = [OfferItem(**offer.model_dump()) for offer in offers]
    return OffersResponse(url=clean_url, offers=api_offers)


@router.get("/comments", response_model=CommentsResponse)
async def comments_endpoint(
    url: str = Query(..., min_length=5),
    date_to: date | None = Query(None),
    service: CommentService = Depends(get_comment_service),
) -> CommentsResponse:
    date_to_dt = datetime.combine(date_to, time.max) if date_to else None
    try:
        clean_url, comments = await service.fetch_comments(url, date_to_dt)
    except (asyncio.TimeoutError, PlaywrightTimeoutError):
        raise HTTPException(status_code=408, detail="Timeout while parsing comments")
    except Exception as exc:
        logger.exception("Failed to parse comments", exc_info=exc)
        raise HTTPException(status_code=500, detail=f"Unexpected error: {exc}") from exc

    api_comments = [CommentItem(**item.model_dump()) for item in comments]
    return CommentsResponse(url=clean_url, comments=api_comments)
