import uuid
import sys
import signal
import traceback
from abc import ABC, abstractmethod
from typing import Dict, Any, Union
from pathlib import Path

from .constant import Status
from .schema import EngineConfig
from ..protocol.mod import OperationProtocol
from ..configer import ConfigLoader, BaseConfig, read_config
from ..logger.core import LogManager
from ..executor import MultiTaskExecutor
from ..plugin.manager import PluginManager
from ..plugin.base import BasePlugin


class BaseEngine(ABC):
    """
    主引擎基类
    """
    def __init__(self, config_path: Union[str, Path], mode: str = "dev"):
        self.config_path = config_path
        self.mode: str = mode

        self.config: Union[None, EngineConfig] = None
        self.configer: Union[ConfigLoader, None] = None
        self.logger: Union[LogManager, None] = None
        self.executor: Union[MultiTaskExecutor, None] = None

        self.plugin_manager = PluginManager(self)  # 使用PluginManager替代plugins列表
        self.components = {}  # 组件字典

        self.status = Status.INIT  # 引擎状态
        self._register_signal_handlers()
        self._initialized = False

    def on_init(self):
        """初始化：配置解析、任务注册、资源准备"""
        self.setup_configer()
        self.setup_logger()
        self.setup_executor()
        # 初始化所有插件
        self.plugin_manager.init_all()

    @abstractmethod
    def on_start(self):
        """主逻辑执行入口：需由子类重写"""
        pass

    def on_stop(self):
        """收尾清理操作"""
        # 停止所有插件
        self.plugin_manager.stop_all()
        
        if hasattr(self.executor, "join"):
            self.executor.join()
        
        # 停止所有组件
        for component_id, component in self.components.items():
            try:
                component.stop()
                self.logger.INFO(f"组件 {component_id} 已停止")
            except Exception as e:
                self.logger.ERROR(f"停止组件 {component_id} 时出错: {e}")

    def on_exit(self):
        """退出处理：资源释放、日志关闭等"""
        # 退出所有插件
        self.plugin_manager.exit_all()
        self.status = Status.EXITED

    def on_exception(self, exc):
        """异常处理：可扩展到报警等"""
        # 所有插件处理异常
        for plugin_id, plugin in self.plugin_manager.get_all_plugins().items():
            try:
                plugin.on_exception(exc)
            except Exception as e:
                self.logger.ERROR(f"插件 '{plugin_id}' 异常处理器中出错: {e}")
        self.status = Status.ERROR

    def setup_configer(self):
        """初始化配置管理器"""
        self.configer = ConfigLoader(self.config_path, EngineConfig, self.mode)
        self.config = self.configer.load()

    def setup_logger(self):
        """初始化日志管理器"""
        # 创建单例日志管理器
        self.logger = LogManager.get_instance(self.config.logger)
        # 非多进程模式，直接启动日志管理器，否则需等待业务流程已启动多进程后再手动启动
        if not self.config.logger.multiprocess:
            self.logger.start()

    def setup_executor(self):
        mode = self.config.executor.mode
        self.executor = MultiTaskExecutor(mode, **self.config.executor.__dict__)

    def _register_signal_handlers(self):
        signal.signal(signal.SIGINT, self._handle_exit)
        signal.signal(signal.SIGTERM, self._handle_exit)

    def register_plugin(self, plugin_id: str, plugin: BasePlugin) -> BasePlugin:
        """注册插件"""
        return self.plugin_manager.register(plugin_id, plugin)

    def get_plugin(self, plugin_id: str) -> BasePlugin:
        """获取插件"""
        return self.plugin_manager.get_plugin(plugin_id)

    def get_plugins_by_type(self, plugin_type: str) -> list:
        """根据类型获取插件"""
        return self.plugin_manager.get_plugins_by_type(plugin_type)

    # 组件管理方法
    def register_component(self, component_id, component):
        """注册组件"""
        component.bind(self)
        self.components[component_id] = component
        self.logger.INFO(f"组件 {component_id} 已注册")
        return component
        
    def start_component(self, component_id):
        """启动组件"""
        if component_id in self.components:
            try:
                self.components[component_id].start()
                self.logger.INFO(f"组件 {component_id} 已启动")
                return True
            except Exception as e:
                self.logger.ERROR(f"启动组件 {component_id} 时出错: {e}")
        else:
            self.logger.WARNING(f"未找到组件 {component_id}")
        return False
        
    def stop_component(self, component_id):
        """停止组件"""
        if component_id in self.components:
            try:
                self.components[component_id].stop()
                self.logger.INFO(f"组件 {component_id} 已停止")
                return True
            except Exception as e:
                self.logger.ERROR(f"停止组件 {component_id} 时出错: {e}")
        else:
            self.logger.WARNING(f"未找到组件 {component_id}")
        return False
            
    def get_component_status(self, component_id=None):
        """获取组件状态"""
        if component_id:
            if component_id in self.components:
                try:
                    return self.components[component_id].get_status()
                except Exception as e:
                    self.logger.ERROR(f"获取组件 {component_id} 状态时出错: {e}")
                    return {"status": Status.ERROR, "error": str(e)}
            return None
        return {k: v.get_status() for k, v in self.components.items()}

    def get_status(self):
        """获取引擎状态"""
        return {
            "status": self.status,
            "plugins": len(self.plugin_manager.get_all_plugins()),
            "components": len(self.components),
            "initialized": self._initialized
        }

    def _handle_exit(self, signum, frame):
        self.logger.WARNING(f"接收到信号 {signum}, 正在退出...")
        self.on_stop()
        self.on_exit()
        sys.exit(0)

    def run(self, mode="cli"):
        try:
            self.logger.INFO("引擎初始化中...")
            self.status = Status.INITIALIZING
            self.on_init()
            self._initialized = True

            self.logger.INFO("引擎启动中...")
            self.status = Status.RUNNING
            self.on_start()

            self.logger.INFO("引擎执行完毕。")
            self.status = Status.STOPPED
            self.on_stop()

        except Exception as e:
            self.logger.ERROR("引擎运行期间发生未处理的异常")
            self.logger.ERROR(traceback.format_exc())
            self.status = Status.ERROR
            self.on_exception(e)
        finally:
            self.on_exit()


class BaseComponent(OperationProtocol):
    """
    组件基类，实现OperationProtocol协议
    用于具体业务功能的封装和管理
    """
    def __init__(self, name: str = None, **kwargs):
        """
        初始化组件
        
        Args:
            component_id: 组件唯一标识符
            name: 组件名称，默认为类名
            **kwargs: 额外的参数
        """
        self._uuid: str = str(uuid.uuid4())
        self._name = name or self.__class__.__name__
        self.status = Status.INIT
        self.metadata = kwargs
        self.engine = None
        self.logger = None
        self._thread = None

    def get_name(self) -> str:
        return self._name

    def get_id(self) -> str:
        return f"{self._name}:{self._uuid}"

    def bind(self, engine):
        """
        将组件绑定到引擎
        
        Args:
            engine: 引擎实例
        """
        self.engine = engine
        self.logger = getattr(engine, 'logger', None)
        
    def start(self):
        """
        启动组件
        """
        if self.status not in [Status.INIT, Status.STOPPED]:
            if self.logger:
                self.logger.WARNING(f"组件{self._name}已启动或已报错: {self.status}")
            return False
            
        try:
            self.status = Status.INITIALIZING
            self._execute()
            self.status = Status.RUNNING
            if self.logger:
                self.logger.INFO(f"组件{self._name}已启动")
            return True
        except Exception as e:
            self.status = Status.ERROR
            if self.logger:
                self.logger.ERROR(f"组件{self._name}启动失败: {str(e)}")
            raise
        
    def stop(self):
        """
        停止组件
        """
        if self.status not in [Status.RUNNING, Status.INITIALIZING]:
            if self.logger:
                self.logger.WARNING(f"组件{self._name}已停止: {self.status}")
            return False
            
        try:
            self.status = Status.STOPPING
            self._stop_execution()
            self.status = Status.STOPPED
            if self.logger:
                self.logger.INFO(f"组件{self._name}成功终止")
            return True
        except Exception as e:
            self.status = Status.ERROR
            if self.logger:
                self.logger.ERROR(f"组件{self._name}终止失败: {str(e)}")
            raise
        
    def join(self, timeout=None):
        """
        等待组件执行完成
        
        Args:
            timeout: 超时时间，如果为None则无限等待
        
        Returns:
            是否成功等待
        """
        if hasattr(self, '_thread') and self._thread is not None:
            if hasattr(self._thread, 'join'):
                self._thread.join(timeout)
                return not self._thread.is_alive()
        return True
            
    def get_status(self) -> Dict[str, Any]:
        """
        获取组件状态
        
        Returns:
            包含组件状态信息的字典
        """
        return {
            "id": self._uuid,
            "name": self._name,
            "status": self.status,
            "type": self.__class__.__name__
        }

    @abstractmethod
    def _execute(self):
        """
        执行组件逻辑，由子类实现
        """
        pass

    @abstractmethod
    def _stop_execution(self):
        """
        停止组件逻辑，由子类实现
        """
        pass
        
    def __repr__(self):
        return f"<{self.__class__.__name__}(id='{self._uuid}', name='{self._name}', status='{self.status}')>"