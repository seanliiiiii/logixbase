# -*- coding: utf-8 -*-
from typing import Any, Union


class ShareContext:
    """
    可跨进程共享的上下文对象：
    - 支持通过属性访问字段
    - 支持 pickle，用于 multiprocessing initializer 传参
    - 提供 to_kwargs() 以注入函数参数
    """

    logger = None                       # 日志管理器实例

    def __init__(self, **kwargs):
        self._values = kwargs

    def __getstate__(self):
        # 控制 pickle 行为：仅序列化字段值
        return self._values

    def __setstate__(self, state):
        # 控制 unpickle 行为
        self._values = state

    def __getattr__(self, item):
        try:
            return self._values[item]
        except KeyError:
            raise AttributeError(f"ShareContext没有指定属性：{item}")

    def __setattr__(self, key, value):
        # 确保 _values 本身正确赋值，其他字段存入 values dict
        if key == "_values":
            super().__setattr__(key, value)
            for k, v in value.items():
                if hasattr(ShareContext, k):
                    setattr(ShareContext, k, v)
        else:
            self.values[key] = value

    def to_kwargs(self) -> dict[str, Any]:
        """
        将上下文内容转换为 kwargs，用于注入任务函数
        """
        return {k: v for k, v in self._values.items() if not k.startswith("_")}


# 子进程中使用的全局上下文对象
_global_context: Union[ShareContext, None] = None


def init_worker(context_obj: ShareContext):
    """
    子进程启动时自动注入共享上下文
    """
    global _global_context
    _global_context = context_obj


def get_context_kwargs() -> dict[str, Any]:
    """
    获取当前全局上下文对应的参数字典（供任务函数注入）
    """
    return _global_context.to_kwargs() if _global_context else {}


def global_context():
    return _global_context
