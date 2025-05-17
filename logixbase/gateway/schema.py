from pydantic import BaseModel, Field


class CtpConfig(BaseModel):
    tdfront: list = Field(description="交易服务器地址")
    mdfront: list = Field(description="行情服务器地址")
    username: str = Field(description="期货账户名")
    password: str = Field(description="期货账户密码")
    brokerid: str = Field( description="经纪商代码")
    authcode: str = Field(description="CTP授权登录码")
    appid: str = Field(description="应用ID")


class GatewayConfig(BaseModel):
    ctp: CtpConfig = Field(default_factory=CtpConfig, description="CTP连接配置")