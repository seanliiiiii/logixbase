from enum import Enum


class Status(Enum):
    """引擎运行状态定义"""
    INIT = "INIT"
    INITIALIZING = "INITIALIZING"
    RUNNING = "RUNNING"
    STOPPING = "STOPPING"
    STOPPED = "STOPPED"
    ERROR = "ERROR"
    EXITED = "EXITED"