from db.db import async_session
import sqlalchemy as sa


class BaseQueries:

    @staticmethod
    async def save_in_db(instances, many=False):
        async with async_session() as session:

            if many:
                session.add_all(instances)
            else:
                session.add(instances)
            await session.commit()

    @staticmethod
    async def delete_instance(instance):
        async with async_session() as session:
            await session.delete(instance)
            await session.commit()

