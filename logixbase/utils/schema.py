from pydantic import BaseModel, Field


class MailConfig(BaseModel):
    is_private: bool = Field(default=False, description="是否为私人邮箱")
    host: str = Field(description="SMTP服务器地址")
    username: str = Field( description="邮箱用户名")
    password: str = Field(description="邮箱密码")
    mailname: str = Field(description="发送邮件显示名称")


class DatabaseConfig(BaseModel):
    host: str = Field(description="数据库地址")
    port: int = Field(description="数据库端口")
    username: str = Field(description="数据库用户名")
    password: str = Field(description="数据库密码")
    database: str = Field(default=None, description="指定数据库")