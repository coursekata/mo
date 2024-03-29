from enum import StrEnum

from pydantic import BaseModel, FilePath


class LogLevel(StrEnum):
    "The levels of logging that can be used."

    debug = "DEBUG"
    info = "INFO"
    warning = "WARNING"
    error = "ERROR"
    critical = "CRITICAL"


class CommonConfig(BaseModel):
    remove: bool = False
    "Delete the input data after it has been processed."

    merge: bool = False
    "If there is existing output data, merge with it (otherwise fail with error)."

    strip_pii: bool = True
    "Remove columns that are specifically for personally identifiable information."

    dry_run: bool = False
    "Only print the actions that would be taken, do not actually perform them."

    log_level: LogLevel = LogLevel.info
    "The level of logging to use."

    log_file: FilePath | None = None
    "Path to a file where logs should be written. Default is not to write to a file."
