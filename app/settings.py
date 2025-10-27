from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import BaseModel
from typing import Optional


# Nested settings for Zammad
class ZammadSettings(BaseModel):
    url: str  # ZAMMAD_URL - base URL for API
    token: str  # ZAMMAD_TOKEN - Bearer token for auth


class Settings(BaseSettings):
    title: str = "Zammad Ticket Fetcher"
    description: str = "FastAPI app to fetch and export ticket data to CSV"
    debug: bool = False
    version: str = "1.0.0"
    log_level: str = "INFO"
    log_file: str = "zammad_app.log"
    csv_path: str = "./tickets_data.csv"  # CSV_PATH - path to append data

    # Zammad config
    zammad: ZammadSettings

    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="",
        env_nested_delimiter="_",  # e.g., ZAMMAD_URL -> zammad.url
        env_nested_max_split=1,
        case_sensitive=False,
    )


settings = Settings()
# Comment: This singleton ensures settings are loaded once; access via settings.zammad.url etc.
