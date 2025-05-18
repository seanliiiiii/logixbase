from pydantic import BaseModel, Field
import multiprocessing as mp

from ..utils import MailConfig, DatabaseConfig
from ..logger import LoggerConfig
from ..executor import ExecutorConfig


class EngineConfig(BaseModel):
    logger: LoggerConfig = Field(default_factory=LoggerConfig, description="日志管理器配置")
    executor: ExecutorConfig = Field(default_factory=ExecutorConfig, description="任务执行器配置")
    mail: MailConfig = Field(default_factory=MailConfig, description="邮件相关配置")
    database: DatabaseConfig = Field(default_factory=DatabaseConfig, description="数据库相关配置")