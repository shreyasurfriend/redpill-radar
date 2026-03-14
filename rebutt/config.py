import logging
from pydantic_settings import BaseSettings

logger = logging.getLogger(__name__)


class Settings(BaseSettings):
    """Configuration settings for the rebuttal service."""
    
    # Database
    database_url: str = "sqlite+aiosqlite:///../data/redpill_radar.db"
    
    # Groq LLM
    groq_api_key: str = ""
    groq_model: str = "llama-3.3-70b-versatile"
    llm_rebuttal_timeout: int = 30
    rebuttal_max_retries: int = 3
    
    # Polling
    rebuttal_polling_interval: int = 10  # seconds
    
    # Browser
    browser_headless: bool = False
    
    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "extra": "ignore"  # Ignore extra environment variables
    }


settings = Settings()

# Validate critical settings
if not settings.groq_api_key:
    logger.warning("GROQ_API_KEY is not set. LLM calls will fail.")
