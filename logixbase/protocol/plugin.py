from typing import Protocol
from .mod import OperationProtocol


class PluginProtocol(OperationProtocol, Protocol):
    """通用插件协议，支持生命周期与绑定"""
    def bind(self, engine):
        """将插件绑定到引擎"""
        self.engine = engine
        self.logger = getattr(engine, 'logger', None)
        self.config = getattr(engine, 'config', None)

    def on_init(self):
        """初始化插件"""
        pass

    def on_start(self):
        """启动插件"""
        pass

    def on_stop(self):
        """停止插件"""
        pass

    def on_exit(self):
        """退出插件"""
        pass

    def on_exception(self, exc):
        """处理异常"""
        pass
        
    def start(self):
        """符合OperationProtocol，启动插件"""
        pass
        
    def stop(self):
        """符合OperationProtocol，停止插件"""
        pass
        
    def join(self):
        """符合OperationProtocol，等待插件完成"""
        pass
