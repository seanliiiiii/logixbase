from typing import List, Dict, Any

from ..protocol.plugin import PluginProtocol


class BasePlugin(PluginProtocol):
    """
    插件基类，实现PluginProtocol协议
    用于扩展引擎功能，支持生命周期管理
    """
    plugin_type = "base"
    
    def __init__(self, name=None, **kwargs):
        """
        初始化插件
        
        Args:
            name: 插件名称，默认为类名
            **kwargs: 额外的元数据
        """
        self.name = name or self.__class__.__name__
        self.engine = None
        self.logger = None
        self.config = {}
        self.enabled = True
        self.metadata = kwargs
        self.status = "INIT"
        
    def bind(self, engine):
        """
        将插件绑定到引擎实例
        
        Args:
            engine: 引擎实例
        """
        self.engine = engine
        self.logger = getattr(engine, 'logger', None)
        self.config = getattr(engine, 'config', {}).get(self.plugin_type, {})
        
    def on_init(self):
        """
        插件初始化，在引擎初始化阶段调用
        用于加载配置、准备资源等
        """
        pass
        
    def on_start(self):
        """
        插件启动，在引擎启动阶段调用
        用于启动后台任务等
        """
        pass
        
    def on_stop(self):
        """
        插件停止，在引擎停止阶段调用
        用于停止后台任务、保存状态等
        """
        pass
        
    def on_exit(self):
        """
        插件退出，在引擎退出阶段调用
        用于释放资源等
        """
        pass
        
    def on_exception(self, exc):
        """
        异常处理，在引擎捕获到异常时调用
        
        Args:
            exc: 捕获到的异常
        """
        if self.logger:
            self.logger.ERROR(f"插件 {self.name} 发生错误: {str(exc)}")
            
    def start(self):
        """
        启动插件
        符合OperationProtocol接口
        """
        if not self.enabled:
            if self.logger:
                self.logger.WARNING(f"插件 {self.name} 已禁用，跳过启动")
            return False
            
        try:
            self.status = "STARTING"
            self.on_start()
            self.status = "RUNNING"
            if self.logger:
                self.logger.INFO(f"插件 {self.name} 已启动")
            return True
        except Exception as e:
            self.status = "ERROR"
            if self.logger:
                self.logger.ERROR(f"启动插件 {self.name} 时出错: {str(e)}")
            return False
    
    def stop(self):
        """
        停止插件
        符合OperationProtocol接口
        """
        if not self.enabled or self.status != "RUNNING":
            return
            
        try:
            self.status = "STOPPING"
            self.on_stop()
            self.status = "STOPPED"
            if self.logger:
                self.logger.INFO(f"插件 {self.name} 已停止")
        except Exception as e:
            self.status = "ERROR"
            if self.logger:
                self.logger.ERROR(f"停止插件 {self.name} 时出错: {str(e)}")
    
    def join(self):
        """
        等待插件完成
        符合OperationProtocol接口
        """
        pass
            
    def get_status(self) -> Dict[str, Any]:
        """
        获取插件状态
        
        Returns:
            包含插件状态信息的字典
        """
        return {
            "name": self.name,
            "type": self.plugin_type,
            "enabled": self.enabled,
            "status": self.status
        }
        
    def get_type(self) -> str:
        """
        获取插件类型
        
        Returns:
            插件类型标识符
        """
        return self.plugin_type
        
    def get_metadata(self) -> Dict[str, Any]:
        """
        获取插件元数据
        
        Returns:
            插件元数据字典
        """
        return self.metadata
        
    def get_dependencies(self) -> List[str]:
        """
        获取插件依赖
        
        Returns:
            依赖的插件类型列表
        """
        return []
        
    def __repr__(self):
        return f"<{self.__class__.__name__}(name='{self.name}', type='{self.plugin_type}')>" 