from pydantic import BaseModel, Field


class TinysoftConfig(BaseModel):
    path: str = Field(description="本地天软安装地址，用于导入TSLPY3")
    host: str = Field(default="telecom.tinysoft.com.cn", description="天软接口域名")
    port: int = Field(default=443, description="天软端口号")
    username: str = Field( description="天软用户名")
    password: str = Field(description="天软密码")


class TQSDKConfig(BaseModel):
    username: str = Field(description="天勤用户名")
    password: str = Field(description="天勤密码")


class FeederConfig(BaseModel):
    tinysoft: TinysoftConfig = Field(default_factory=TinysoftConfig, description="天软连接配置")
    tqsdk: TQSDKConfig = Field(default_factory=TQSDKConfig, description="天勤连接配置")
