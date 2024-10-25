import logging

from pydantic_settings import BaseSettings, SettingsConfigDict


class Config(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="METRICS_", arbitrary_types_allowed=True)

    log_level: int | str = logging.INFO
