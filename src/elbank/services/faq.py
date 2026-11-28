"""FAQ articles, cached in Redis (``faq:articles``, ``SUPPORT_FAQ_CACHE_TTL_SECONDS``).

The whole published list is cached as one value (it is small); a category filter is
applied to the cached list. The cache is an optimization: Redis errors fall back to
the database.
"""

from pydantic import TypeAdapter, ValidationError
from redis.asyncio import Redis
from redis.exceptions import RedisError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from elbank.core.logging import get_logger
from elbank.db.models import FaqArticle, TicketCategory
from elbank.schemas.support import FaqArticleRead

logger = get_logger(__name__)

CACHE_KEY = "faq:articles"
_adapter = TypeAdapter(list[FaqArticleRead])


class FaqService:
    def __init__(self, session: AsyncSession, redis: Redis, ttl_seconds: int) -> None:
        self._session = session
        self._redis = redis
        self._ttl = ttl_seconds

    async def list_articles(self, category: TicketCategory | None = None) -> list[FaqArticleRead]:
        articles = await self._cached()
        if articles is None:
            articles = await self._load()
            await self._store(articles)
        if category is None:
            return articles
        return [a for a in articles if a.category is category]

    async def _load(self) -> list[FaqArticleRead]:
        rows = await self._session.scalars(
            select(FaqArticle)
            .where(FaqArticle.is_published)
            .order_by(FaqArticle.category, FaqArticle.position, FaqArticle.slug)
        )
        return [FaqArticleRead.model_validate(a) for a in rows]

    async def _cached(self) -> list[FaqArticleRead] | None:
        try:
            raw = await self._redis.get(CACHE_KEY)
        except RedisError as exc:
            logger.warning("faq cache read failed", error=str(exc))
            return None
        if raw is None:
            return None
        try:
            return _adapter.validate_json(raw)
        except ValidationError:
            logger.warning("faq cache entry is stale, ignored")
            return None

    async def _store(self, articles: list[FaqArticleRead]) -> None:
        try:
            await self._redis.set(CACHE_KEY, _adapter.dump_json(articles), ex=self._ttl)
        except RedisError as exc:
            logger.warning("faq cache write failed", error=str(exc))
