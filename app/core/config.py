"""
Configuration Settings
======================
Centralized configuration for the application using environment variables.
"""

import os
from typing import List
from pathlib import Path
from pydantic_settings import BaseSettings
from dotenv import load_dotenv

# Load .env file explicitly (important for production)
# Try multiple locations for .env file
env_paths = [
    Path(__file__).parent.parent.parent / ".env",  # Root of project
    Path(__file__).parent.parent / ".env",  # One level up
    Path(__file__).parent / ".env",  # Same directory as config
    Path.cwd() / ".env",  # Current working directory
]

env_loaded = False
for env_path in env_paths:
    if env_path.exists():
        load_dotenv(dotenv_path=env_path, override=True)  # override=True ensures it overwrites
        print(f"[OK] Loaded .env from: {env_path}")
        env_loaded = True
        break

if not env_loaded:
    print(f"[WARNING] No .env file found. Tried locations:")
    for path in env_paths:
        print(f"   - {path}")
    print("[WARNING] Using environment variables from system/PM2")


class Settings(BaseSettings):
    """Application settings loaded from environment variables"""

    # Application Info
    APP_NAME: str = "Universal Insurance Quote Comparison API"
    APP_DESCRIPTION: str = "AI-powered comparison and ranking of any insurance quotes from PDF documents"
    APP_VERSION: str = "2.0.0"

    # API Info (aliases for backward compatibility)
    API_TITLE: str = APP_NAME
    API_DESCRIPTION: str = APP_DESCRIPTION

    # OpenAI Configuration
    OPENAI_API_KEY: str = os.getenv("OPENAI_API_KEY", "")
    AI_MODEL: str = os.getenv("AI_MODEL", "gpt-4o-mini")
    OPENAI_MODEL: str = AI_MODEL  # Alias for backward compatibility
    # Alternative models: gpt-4-turbo, gpt-4o, gpt-4o-mini

    # Anthropic Configuration (for future Claude integration)
    ANTHROPIC_API_KEY: str = os.getenv("ANTHROPIC_API_KEY", "")
    # Claude models: claude-3-5-sonnet-20241022, claude-3-opus-20240229

    # File Upload Settings
    UPLOAD_DIR: str = "uploads"
    MAX_FILE_SIZE: int = 20 * 1024 * 1024  # 20MB
    MAX_FILE_SIZE_MB: int = 20  # 20MB for display
    ALLOWED_EXTENSIONS: List[str] = [".pdf"]

    # CORS Settings
    CORS_ORIGINS: List[str] = ["*"]  # In production, specify actual origins

    # AI Processing Settings
    MAX_TOKENS: int = 4096
    TEMPERATURE: float = 0.3  # Lower temperature for more consistent extraction

    # Logo Extraction Settings
    ENABLE_LOGO_EXTRACTION: bool = True
    MAX_LOGO_SIZE: int = 500 * 1024  # 500KB max for logos

    # Azure Computer Vision OCR Settings
    AZURE_VISION_ENDPOINT: str = os.getenv("AZURE_CV_ENDPOINT", "")
    AZURE_VISION_KEY: str = os.getenv("AZURE_CV_KEY_1", "")
    ENABLE_OCR_FALLBACK: bool = os.getenv("ENABLE_OCR_FALLBACK", "true").lower() == "true"
    OCR_MAX_DAILY_CALLS: int = int(os.getenv("OCR_MAX_DAILY_CALLS", "100"))
    OCR_COOLDOWN_SECONDS: int = int(os.getenv("OCR_COOLDOWN_SECONDS", "60"))

    # Storage Settings
    ENABLE_STORAGE: bool = os.getenv("ENABLE_STORAGE", "false").lower() == "true"
    # Try MONGO_URI first (used by Backend), then MONGODB_URL
    MONGODB_URL: str = os.getenv("MONGO_URI") or os.getenv("MONGODB_URL", "mongodb://localhost:27017")
    MONGODB_DATABASE: str = os.getenv("MONGODB_DATABASE") or os.getenv("MONGO_DB_NAME", "hakemAI")
    MONGODB_COLLECTION: str = os.getenv("MONGODB_COLLECTION", "comparisons")

    class Config:
        env_file = ".env"
        case_sensitive = True
        extra = "ignore"  # Ignore extra environment variables


# Create global settings instance
settings = Settings()

# Debug output (remove in production or set DEBUG=false)
if os.getenv("DEBUG", "false").lower() == "true":
    print("\n" + "="*60)
    print("CONFIGURATION DEBUG INFO:")
    print("="*60)
    print(f"MONGODB_URL: {settings.MONGODB_URL[:50]}..." if len(settings.MONGODB_URL) > 50 else f"MONGODB_URL: {settings.MONGODB_URL}")
    print(f"MONGODB_DATABASE: {settings.MONGODB_DATABASE}")
    print(f"MONGODB_COLLECTION: {settings.MONGODB_COLLECTION}")
    print(f"OPENAI_API_KEY: {'SET' if settings.OPENAI_API_KEY else 'NOT SET'}")
    print(f"AI_MODEL: {settings.AI_MODEL}")
    print("="*60 + "\n")