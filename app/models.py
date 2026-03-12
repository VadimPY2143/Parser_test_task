from datetime import datetime

from pydantic import BaseModel


class HotlineOffer(BaseModel):
    url: str | None
    original_url: str | None
    title: str | None
    shop: str | None
    price: int | None
    is_used: bool


class OfferItem(BaseModel):
    url: str | None
    original_url: str | None
    title: str | None
    shop: str | None
    price: int | None
    is_used: bool


class OffersResponse(BaseModel):
    url: str
    offers: list[OfferItem]


class ComfyComment(BaseModel):
    rating: float | None
    advantages: str
    shortcomings: str
    comment: str
    created_at: datetime | None


class BrainComment(BaseModel):
    rating: float | None
    advantages: str
    shortcomings: str
    comment: str
    created_at: datetime | None


class CommentItem(BaseModel):
    rating: float | None
    advantages: str
    shortcomings: str
    comment: str
    created_at: datetime | None


class CommentsResponse(BaseModel):
    url: str
    comments: list[CommentItem]
