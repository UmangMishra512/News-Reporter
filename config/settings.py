"""
config/settings.py — Pydantic-settings configuration model.

Validates all environment variables at startup and fails fast
if required values are missing.
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ── LLM ───────────────────────────────────────────────────
    llm_provider: str = Field(default="gemini", description="gemini | openai | groq | ollama")
    gemini_api_key: str = Field(default="", description="Google Gemini API key")
    gemini_model: str = Field(default="gemini-1.5-flash")
    openai_api_key: str = Field(default="", description="OpenAI API key")
    openai_model: str = Field(default="gpt-4o-mini")
    groq_api_key: str = Field(default="", description="Groq API key")
    groq_model: str = Field(default="llama-3.1-70b-versatile")
    ollama_base_url: str = Field(default="http://localhost:11434")
    ollama_model: str = Field(default="llama3")

    # ── News APIs ─────────────────────────────────────────────
    news_api_key: str = Field(default="", description="NewsAPI.org key (optional)")

    # ── Schedule ──────────────────────────────────────────────
    publish_interval_hours: int = Field(default=2, ge=1, le=24)
    max_articles_per_run: int = Field(default=200, ge=10)
    top_stories_to_publish: int = Field(default=10, ge=1, le=20)

    # ── Storage ───────────────────────────────────────────────
    database_path: Path = Field(default=Path("storage/news.db"))
    log_dir: Path = Field(default=Path("storage/logs"))
    log_level: str = Field(default="INFO")

    # ── Platform behaviour ────────────────────────────────────
    dry_run: bool = Field(default=False)
    enable_browser_automation: bool = Field(default=False)
    request_timeout_seconds: int = Field(default=15, ge=5, le=60)
    rss_worker_concurrency: int = Field(default=8, ge=1, le=32)
    dedup_similarity_threshold: float = Field(default=0.82, ge=0.5, le=1.0)
    min_confidence_score: float = Field(default=0.5, ge=0.0, le=1.0)

    # ── Health Monitor ────────────────────────────────────────
    heartbeat_check_interval: int = Field(default=60, ge=10)
    agent_stale_threshold_seconds: int = Field(default=180, ge=30)

    # ── Dashboard ─────────────────────────────────────────────
    dashboard_host: str = Field(default="127.0.0.1", description="Host/IP for dashboard web server")
    dashboard_port: int = Field(default=8080, ge=1024, le=65535)
    enable_dashboard: bool = Field(default=True)

    @field_validator("llm_provider")
    @classmethod
    def validate_llm_provider(cls, v: str) -> str:
        valid = {"gemini", "openai", "groq", "ollama"}
        if v.lower() not in valid:
            raise ValueError(f"llm_provider must be one of {valid}")
        return v.lower()

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        valid = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        if v.upper() not in valid:
            raise ValueError(f"log_level must be one of {valid}")
        return v.upper()

    def validate_llm(self) -> None:
        """Call explicitly to assert LLM credentials are set."""
        checks = {
            "gemini": self.gemini_api_key,
            "openai": self.openai_api_key,
            "groq":   self.groq_api_key,
            "ollama": "ok",  # no key needed
        }
        if not checks.get(self.llm_provider):
            raise RuntimeError(f"Missing API key for LLM provider: {self.llm_provider}")


def get_settings() -> Settings:
    return Settings()
