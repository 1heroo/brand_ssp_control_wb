from decouple import config


class Settings:
    POSTGRES_USER = config('POSTGRES_USER')
    POSTGRES_PASSWORD = config('POSTGRES_PASSWORD')
    POSTGRES_HOST = config('POSTGRES_HOST')
    POSTGRES_DB_NAME = config('POSTGRES_DB_NAME')
    POSTGRES_PORT = config('POSTGRES_PORT')

    DATABASE_URL = \
        f'postgresql+asyncpg://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB_NAME}'

    WB_API_TOKEN = config('WB_API_TOKEN')


settings = Settings()
