from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file='.env', env_file_encoding='utf-8', extra='ignore')

    app_name: str = 'Evidentia'
    database_url: str = 'postgresql+asyncpg://postgres:postgres@localhost:5432/evidentia'
    chroma_path: str = './.chroma'
    openai_api_key: str | None = None
    embedding_model: str = 'text-embedding-3-small'
    extraction_model: str = 'gpt-4o-mini'
    cache_ttl_seconds: int = 300


settings = Settings()
