from datetime import datetime

from app.comments import get_comments
from app.repositories import CommentsRepository


class CommentService:
    def __init__(self, repository: CommentsRepository) -> None:
        self._repository = repository

    async def fetch_comments(
        self,
        url: str,
        date_to: datetime | None,
    ):
        clean_url, comments = await get_comments(url, date_to)
        await self._repository.save(clean_url, [item.model_dump() for item in comments])
        return clean_url, comments
