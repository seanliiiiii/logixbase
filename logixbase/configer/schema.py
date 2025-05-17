from pydantic import BaseModel, Field

from ..utils import MailConfig, DatabaseConfig


class BaseConfig(BaseModel):
    mail: MailConfig = Field(default_factory=MailConfig, description="邮件相关配置")
    database: DatabaseConfig = Field(default_factory=DatabaseConfig, description="数据库相关配置")