from typing import Dict, Any, Optional, List, Union

from .base import BaseEngine
from ..logger.core import LogManager


class ProcessManager:
    """
    进程管理器，用于管理多个引擎实例
    提供统一的控制接口
    """
    def __init__(self):
        """
        初始化进程管理器
        """
        self.engines: Dict[str, BaseEngine] = {}
        self.logger = LogManager.get_instance()
        
    def register(self, name: str, engine: BaseEngine) -> BaseEngine:
        """
        注册引擎
        
        Args:
            name: 引擎名称
            engine: 引擎实例
            
        Returns:
            注册的引擎实例
            
        Raises:
            ValueError: 如果引擎名称已经存在
        """
        if name in self.engines:
            raise ValueError(f"引擎 '{name}' 已注册")
            
        self.engines[name] = engine
        self.logger.INFO(f"引擎 '{name}' 已注册")
        return engine
        
    def unregister(self, name: str) -> bool:
        """
        注销引擎
        
        Args:
            name: 引擎名称
            
        Returns:
            是否成功注销
        """
        if name in self.engines:
            del self.engines[name]
            self.logger.INFO(f"引擎 '{name}' 已注销")
            return True
        return False
        
    def start(self, name: str) -> bool:
        """
        启动引擎
        
        Args:
            name: 引擎名称
            
        Returns:
            是否成功启动
        """
        if name not in self.engines:
            self.logger.WARNING(f"未找到引擎 '{name}'")
            return False
            
        try:
            self.engines[name].run()
            return True
        except Exception as e:
            self.logger.ERROR(f"启动引擎 '{name}' 时出错: {e}")
            return False
        
    def stop(self, name: str) -> bool:
        """
        停止引擎
        
        Args:
            name: 引擎名称
            
        Returns:
            是否成功停止
        """
        if name not in self.engines:
            self.logger.WARNING(f"未找到引擎 '{name}'")
            return False
            
        try:
            self.engines[name].on_stop()
            return True
        except Exception as e:
            self.logger.ERROR(f"停止引擎 '{name}' 时出错: {e}")
            return False
            
    def start_all(self) -> Dict[str, bool]:
        """
        启动所有引擎
        
        Returns:
            引擎名称到启动结果的映射
        """
        results = {}
        for name in self.engines:
            results[name] = self.start(name)
        return results
        
    def stop_all(self) -> Dict[str, bool]:
        """
        停止所有引擎
        
        Returns:
            引擎名称到停止结果的映射
        """
        results = {}
        for name in self.engines:
            results[name] = self.stop(name)
        return results
        
    def start_component(self, engine_name: str, component_id: str) -> bool:
        """
        启动引擎中的组件
        
        Args:
            engine_name: 引擎名称
            component_id: 组件ID
            
        Returns:
            是否成功启动
        """
        if engine_name not in self.engines:
            self.logger.WARNING(f"未找到引擎 '{engine_name}'")
            return False
            
        return self.engines[engine_name].start_component(component_id)
        
    def stop_component(self, engine_name: str, component_id: str) -> bool:
        """
        停止引擎中的组件
        
        Args:
            engine_name: 引擎名称
            component_id: 组件ID
            
        Returns:
            是否成功停止
        """
        if engine_name not in self.engines:
            self.logger.WARNING(f"未找到引擎 '{engine_name}'")
            return False
            
        return self.engines[engine_name].stop_component(component_id)
        
    def get_status(self, name: str = None) -> Dict[str, Any]:
        """
        获取引擎状态
        
        Args:
            name: 引擎名称，如果为None则获取所有引擎状态
            
        Returns:
            引擎状态信息
        """
        if name:
            if name not in self.engines:
                self.logger.WARNING(f"未找到引擎 '{name}'")
                return {}
            return self.engines[name].get_status()
        
        return {name: engine.get_status() for name, engine in self.engines.items()}
        
    def get_component_status(self, engine_name: str, component_id: str = None) -> Union[Dict[str, Any], List[Dict[str, Any]], None]:
        """
        获取组件状态
        
        Args:
            engine_name: 引擎名称
            component_id: 组件ID，如果为None则获取所有组件状态
            
        Returns:
            组件状态信息
        """
        if engine_name not in self.engines:
            self.logger.WARNING(f"未找到引擎 '{engine_name}'")
            return None
            
        return self.engines[engine_name].get_component_status(component_id)
        
    def get_engine(self, name: str) -> Optional[BaseEngine]:
        """
        获取引擎实例
        
        Args:
            name: 引擎名称
            
        Returns:
            引擎实例，如果不存在则返回None
        """
        return self.engines.get(name)
        
    def get_all_engines(self) -> Dict[str, BaseEngine]:
        """
        获取所有引擎实例
        
        Returns:
            引擎名称到实例的映射
        """
        return self.engines.copy() 