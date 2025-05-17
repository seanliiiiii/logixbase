from typing import Dict, List, Type, Any, Optional
from .base import BasePlugin


class PluginManager:
    """
    插件管理器，负责插件的注册、初始化、启动和停止
    """
    def __init__(self, engine):
        """
        初始化插件管理器
        
        Args:
            engine: 引擎实例
        """
        self.engine = engine
        self.plugins: Dict[str, BasePlugin] = {}
        
    def register(self, plugin_id: str, plugin: BasePlugin) -> BasePlugin:
        """
        注册插件
        
        Args:
            plugin_id: 插件ID，用于后续访问
            plugin: 插件实例
            
        Returns:
            注册的插件实例
            
        Raises:
            ValueError: 如果插件ID已经存在
        """
        if plugin_id in self.plugins:
            raise ValueError(f"插件ID '{plugin_id}' 已注册")
            
        plugin.bind(self.engine)
        self.plugins[plugin_id] = plugin
        
        if self.engine and self.engine.logger:
            self.engine.logger.INFO(f"插件 '{plugin_id}' 已注册: {plugin.get_type()}")
            
        return plugin
        
    def get_plugin(self, plugin_id: str) -> Optional[BasePlugin]:
        """
        获取插件实例
        
        Args:
            plugin_id: 插件ID
            
        Returns:
            插件实例，如果不存在则返回None
        """
        return self.plugins.get(plugin_id)
        
    def init_all(self):
        """
        初始化所有插件
        在引擎初始化阶段调用
        """
        for plugin_id, plugin in self.plugins.items():
            if plugin.enabled:
                try:
                    plugin.on_init()
                    if self.engine and self.engine.logger:
                        self.engine.logger.DEBUG(f"插件 '{plugin_id}' 已初始化")
                except Exception as e:
                    if self.engine and self.engine.logger:
                        self.engine.logger.ERROR(f"初始化插件 '{plugin_id}' 时出错: {e}")
                    raise
                
    def start_all(self):
        """
        启动所有插件
        在引擎启动阶段调用
        """
        for plugin_id, plugin in self.plugins.items():
            if plugin.enabled:
                try:
                    plugin.start()
                    if self.engine and self.engine.logger:
                        self.engine.logger.DEBUG(f"插件 '{plugin_id}' 已启动")
                except Exception as e:
                    if self.engine and self.engine.logger:
                        self.engine.logger.ERROR(f"启动插件 '{plugin_id}' 时出错: {e}")
                    raise
                
    def stop_all(self):
        """
        停止所有插件
        在引擎停止阶段调用
        """
        for plugin_id, plugin in reversed(list(self.plugins.items())):
            if plugin.enabled:
                try:
                    plugin.stop()
                    if self.engine and self.engine.logger:
                        self.engine.logger.DEBUG(f"插件 '{plugin_id}' 已停止")
                except Exception as e:
                    if self.engine and self.engine.logger:
                        self.engine.logger.ERROR(f"停止插件 '{plugin_id}' 时出错: {e}")
        
        self.join_all()
                
    def join_all(self):
        """
        等待所有插件完成
        """
        for plugin_id, plugin in reversed(list(self.plugins.items())):
            if plugin.enabled:
                try:
                    plugin.join()
                except Exception as e:
                    if self.engine and self.engine.logger:
                        self.engine.logger.ERROR(f"等待插件 '{plugin_id}' 完成时出错: {e}")
                
    def exit_all(self):
        """
        退出所有插件
        在引擎退出阶段调用
        """
        for plugin_id, plugin in reversed(list(self.plugins.items())):
            if plugin.enabled:
                try:
                    plugin.on_exit()
                    if self.engine and self.engine.logger:
                        self.engine.logger.DEBUG(f"插件 '{plugin_id}' 已退出")
                except Exception as e:
                    if self.engine and self.engine.logger:
                        self.engine.logger.ERROR(f"插件 '{plugin_id}' 退出时出错: {e}")
                
    def get_all_plugins(self) -> Dict[str, BasePlugin]:
        """
        获取所有注册的插件
        
        Returns:
            插件ID到插件实例的映射
        """
        return self.plugins.copy()
        
    def get_enabled_plugins(self) -> Dict[str, BasePlugin]:
        """
        获取所有启用的插件
        
        Returns:
            插件ID到插件实例的映射
        """
        return {k: v for k, v in self.plugins.items() if v.enabled}
        
    def get_plugins_by_type(self, plugin_type: str) -> List[BasePlugin]:
        """
        根据类型获取插件
        
        Args:
            plugin_type: 插件类型
            
        Returns:
            指定类型的插件列表
        """
        return [p for p in self.plugins.values() if p.get_type() == plugin_type]
        
    def get_all_status(self) -> Dict[str, Dict[str, Any]]:
        """
        获取所有插件状态
        
        Returns:
            插件ID到状态的映射
        """
        return {k: v.get_status() for k, v in self.plugins.items()} 