import logging
from enum import StrEnum


class LogLevel(StrEnum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class ColoredFormatter(logging.Formatter):
    # ANSI escape codes for colors
    RESET = "\x1b[0m"
    COLORS = {
        "D": "\x1b[90m",  # Grey for DEBUG
        "I": "\x1b[34m",  # Blue for INFO
        "W": "\x1b[33m",  # Yellow for WARNING
        "E": "\x1b[31m",  # Red for ERROR
        "C": "\x1b[31;1m",  # Bright Red for CRITICAL
    }

    level_to_letter = {
        logging.DEBUG: "D",
        logging.INFO: "I",
        logging.WARNING: "W",
        logging.ERROR: "E",
        logging.CRITICAL: "C",
    }

    def format(self, record: logging.LogRecord) -> str:
        level_letter = self.level_to_letter.get(record.levelno, "?")
        color = self.COLORS.get(level_letter, self.RESET)
        record.levelletter = level_letter
        message = super().format(record)
        return f"{color}{message}{self.RESET}"


def setup_logging(level: int | str = logging.DEBUG) -> logging.Logger:
    logger = logging.getLogger()
    logger.setLevel(level)

    # Remove existing handlers to prevent duplicate logs
    if logger.hasHandlers():
        logger.handlers.clear()

    # Create console handler
    handler = logging.StreamHandler()
    handler.setLevel(level)

    # Define formatter with custom format
    formatter = ColoredFormatter(
        "[%(levelletter)s] %(asctime)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )
    handler.setFormatter(formatter)

    # Add handler to the logger
    logger.addHandler(handler)

    return logger
