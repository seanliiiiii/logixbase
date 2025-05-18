from pydantic import BaseModel, Field
import multiprocessing as mp

from ..utils import MailConfig, DatabaseConfig
from ..logger import LoggerConfig


class ExecutorConfig(BaseModel):
    mode: str = Field(default="process", description="任务执行模式：process / pool / thread")
    max_workers: int = Field(default=mp.cpu_count() - 1, description="最大使用CPU数量")
    batch_size: int = Field(default=100, description="每个批次的执行任务数量")
    maxtasksperchild: int = Field(default=1, description="每个子进程最多执行的任务数")


class EngineConfig(BaseModel):
    logger: LoggerConfig = Field(default_factory=LoggerConfig, description="日志管理器配置")
    executor: ExecutorConfig = Field(default_factory=ExecutorConfig, description="任务执行器配置")
    mail: MailConfig = Field(default_factory=MailConfig, description="邮件相关配置")
    database: DatabaseConfig = Field(default_factory=DatabaseConfig, description="数据库相关配置")