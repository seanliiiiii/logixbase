from enum import Enum

LOG_LEVELS = {"DEBUG": 10,
              "INFO": 20,
              "WARNING": 30,
              "ERROR": 40,
              "CRITICAL": 50}


class LogLevel(Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class LogFormat(Enum):
    JSON = 'json'
    TEXT = 'text'