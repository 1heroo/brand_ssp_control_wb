from core.settings import settings
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import declarative_base, sessionmaker


DATABASE_URL = settings.DATABASE_URL


async_engine = create_async_engine(DATABASE_URL, pool_size=10)


async_session = sessionmaker(
    async_engine, class_=AsyncSession, expire_on_commit=False
)

Base = declarative_base()
