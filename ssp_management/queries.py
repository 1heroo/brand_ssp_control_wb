from db.queries import BaseQueries
from db.db import async_session
from ssp_management.models import Brand, MyProduct
import sqlalchemy as sa


class BrandQueries(BaseQueries):

    model = Brand

    async def fetch_all(self):
        async with async_session() as session:
            result = await session.execute(
                sa.select(self.model)
            )
            return result.scalars().all()

    async def get_brand_by_id(self, brand_id):
        async with async_session() as session:
            result = await session.execute(
                sa.select(self.model).where(self.model.id == brand_id)
            )
            return result.scalars().first()


class MyProductQueries(BaseQueries):

    model = MyProduct

    async def fetch_all(self):
        async with async_session() as session:
            result = await session.execute(
                sa.select(self.model)
            )
            return result.scalars().all()
