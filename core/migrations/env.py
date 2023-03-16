import asyncio
from logging.config import fileConfig

from sqlalchemy import engine_from_config
from sqlalchemy import pool
from sqlalchemy.ext.asyncio import AsyncEngine

from alembic import context
from core.settings import settings

from db.base import Base

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
fileConfig(config.config_file_name)

# add your model's MetaData object here
# for 'autogenerate' support

target_metadata = Base.metadata

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.


def include_object(obj, name, type_, reflected, compare_to):
    if obj.info.get("skip_autogen", False):
        return False

    return True


def run_migrations_offline():
    url = settings.DATABASE_URL
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        include_object=include_object,

        compare_type=True,
    )

    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection):
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        dialect_opts={"paramstyle": "named"},
        # MATERIALIZED VIEW など無視する場合は下記をクラス属性に設定する
        # __table_args__ = {"info": {"skip_autogen": True}}
        include_object=include_object,
        # 型変更を検知する
        compare_type=True,
    )

    with context.begin_transaction():
        context.run_migrations()


async def run_migrations_online():
    """Run migrations in 'online' mode.
    In this scenario we need to create an Engine
    and associate a connection with the context.
    """
    configurations = config.get_section(config.config_ini_section)
    configurations['sqlalchemy.url'] = settings.DATABASE_URL

    connectable = AsyncEngine(
        engine_from_config(
            configurations,
            prefix="sqlalchemy.",
            poolclass=pool.NullPool,
            future=True,

            connect_args={"ssl": None}
        )
    )

    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)


if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_migrations_online())