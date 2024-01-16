import logging

import typer


class TyperLoggerHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        fg = None
        bg = None
        if record.levelno == logging.DEBUG:
            fg = typer.colors.BLACK
        elif record.levelno == logging.INFO:
            fg = typer.colors.BRIGHT_BLUE
        elif record.levelno == logging.WARNING:
            fg = typer.colors.BRIGHT_YELLOW
        elif record.levelno == logging.ERROR:
            fg = typer.colors.BRIGHT_RED
        elif record.levelno == logging.CRITICAL:
            fg = typer.colors.RED
        typer.secho(self.format(record), bg=bg, fg=fg)


def configure_logging(level: int = logging.INFO):
    typer_handler = TyperLoggerHandler()
    logging.basicConfig(
        level=level, handlers=(typer_handler,), format="[%(levelname)-.1s] %(message)s"
    )
