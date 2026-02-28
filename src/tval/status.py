"""Status enums for validation check and export results."""

from enum import Enum


class CheckStatus(str, Enum):
    """Status of a validation check execution."""

    OK = "OK"
    NG = "NG"
    SKIPPED = "SKIPPED"
    ERROR = "ERROR"


class ExportStatus(str, Enum):
    """Status of a table export operation."""

    OK = "OK"
    SKIPPED = "SKIPPED"
    ERROR = "ERROR"
