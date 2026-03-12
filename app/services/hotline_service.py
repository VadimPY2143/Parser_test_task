from app.hotline import get_offers
from app.repositories import OffersRepository


class HotlineService:
    def __init__(self, repository: OffersRepository) -> None:
        self._repository = repository

    async def fetch_offers(
        self,
        url: str,
        timeout_limit: int | None,
        price_sort: str | None,
        count_limit: int | None,
    ):
        clean_url, offers = await get_offers(url, timeout_limit, price_sort, count_limit)
        await self._repository.save(clean_url, [offer.model_dump() for offer in offers])
        return clean_url, offers
