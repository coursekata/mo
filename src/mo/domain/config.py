import logging

from pydantic_settings import BaseSettings, SettingsConfigDict


class Config(BaseSettings):
    model_config = SettingsConfigDict(env_prefix=f"{__package__.upper()}_" if __package__ else "")
    log_level: int | str = logging.INFO
