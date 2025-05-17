from typing import Protocol, Any


class OperationProtocol(Protocol):
    """
    通用程序执行生命周期协议接口：用于控制统一启动/关闭的模块
    """
    def start(self):
        pass

    def stop(self):
        pass

    def join(self):
        pass


class IdentityProtocol(Protocol):
    """可标识的组件，用于日志追踪、任务管理"""
    def get_name(self) -> str:
        pass

    def get_id(self) -> str:
        pass


class LoaderProtocol(Protocol):
    """
    加载器标准协议，用于统一所有加载器（配置文件加载器、初始化加载器等）
    """
    def load(self):
        pass
