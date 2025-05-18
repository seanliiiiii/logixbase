"""
Engine模块，提供基础引擎和组件管理功能

主要组件:
- BaseEngine: 所有项目引擎的基类
- BaseComponent: 所有业务组件的基类
- ProcessManager: 进程管理器，用于管理多个引擎
"""

from .base import BaseEngine, BaseComponent
from .manager import ProcessManager
from .schema import EngineConfig

__all__ = ['BaseEngine', 'BaseComponent', 'ProcessManager', 'EngineConfig']