from pydantic import BaseModel, Field
from pathlib import Path

from .constant import LogLevel, LogFormat


class LoggerConfig(BaseModel):
    log_path: Path = Field(default="./logs", description="日志保存路径")
    log_level: LogLevel = Field(default="DEBUG", description="日志级别")
    log_format: LogFormat = Field(default="json", description="日志文件类型")
    max_days: int = Field(default=7, description="日志保存天数")
    to_console: bool = Field(default=True, description="是否打印日志")
    rotation_size: float = Field(default=10, description="最大单一日志文件大小（MB)")
    multiprocess: bool = Field(default=True, description="是否开启多进程支持")
