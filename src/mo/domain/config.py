import logging

from pydantic_settings import BaseSettings


class Config(BaseSettings):
    log_level: int | str = logging.INFO
