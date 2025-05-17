import sys
import time
import os
from inspect import isfunction, ismethod
from types import ModuleType
from importlib.util import spec_from_file_location, module_from_spec

from path import Path
import json
import numpy as np
from typing import Union

from datetime import datetime


os.environ["NLS_LANG"] = "AMERICAN_AMERICA.AL32UTF8"


class ProcessBar:
    """
    简单的进度条实现
    Args:
        size (float): 进度条的总步数。
        bar (float, optional): 进度条的长度，默认为20。如果小于1，则自动设置为20。
        icon (str, optional): 进度条满格时使用的图标，默认为"#"。
        bar_icon (str, optional): 进度条未满时使用的图标，默认为"_"。
        title (str, optional): 进度条的标题，默认为"Progress"。
    """
    def __init__(self,
                 size: float,
                 bar: float = 20,
                 icon: str = "#",
                 bar_icon: str = "_",
                 title: str = "Progress"):
        if bar < 1:
            bar = 20

        self.__total_steps_num__ = size
        self.__bar_length__ = bar
        self.__icon__ = icon
        self.__bar_icon__ = bar_icon
        self.__title__ = title
        self.__init_time__ = time.time()

    def show(self, step: float):
        """
        显示进度条。

        Args:
            step (float): 进度条的当前进度值，范围应在0到1之间。

        Returns:
            无返回值。该方法直接通过标准输出打印进度条。
        """
        sys.stdout.write("\r" + self.__get_bar__(step))
        sys.stdout.flush()

    def __get_bar__(self,
                    step: float):
        """
        获取进度条信息。
        Args:
            step (float): 当前步骤数，用于计算进度。
        Returns:
            str: 格式化后的进度条信息字符串。
        Raises:
            Exception: 捕获到任何异常时抛出。
        """
        try:
            status = ""
            progress = float(step) / float(self.__total_steps_num__)
            if progress >= 1.0:
                progress = 1
                status = "\r\n"  # Going to the next line
            block = int(round(self.__bar_length__ * progress))
            time_elapsed = time.time() - self.__init_time__
            bar = "[{}] time: {:.2f}s {:.2f}%  {}".format(
                self.__icon__ * block
                + self.__bar_icon__ * (self.__bar_length__ - block),
                time_elapsed,
                round(progress * 100, 2),
                status,
            )
            return self.__title__ + ": " + bar

        except Exception as e:
            return e


class JsonEncoder(json.JSONEncoder):
    """
    将对象序列化为JSON格式时，处理特定类型数据的默认方法。
    这个方法根据传入对象的类型进行不同的处理：
        1. 如果对象是numpy的ndarray类型，则将其转换为列表。
        2. 如果对象是numpy的int32类型，则将其转换为Python的int类型。
        3. 如果对象是numpy的int64类型，则将其转换为Python的int类型。
        4. 如果对象是numpy的float64类型，则将其转换为Python的float类型。
        5. 如果对象是numpy的float32类型，则将其转换为Python的float类型。
        6. 如果对象是numpy的bool类型，则将其转换为Python的bool类型。
        7. 如果对象是datetime类型，则将其格式化为字符串。
        8. 如果对象是Path类型，则将其转换为字符串。
        9. 如果对象不是上述类型之一，则调用json.JSONEncoder.default方法进行处理。
    """
    def default(self, obj):
        """
        Args:
            obj: 需要序列化的对象。
        Returns:
            Any: 序列化后的数据。
        """
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, np.int32):
            return int(obj)
        elif isinstance(obj, np.int64):
            return int(obj)
        elif isinstance(obj, np.float64):
            return float(obj)
        elif isinstance(obj, np.float32):
            return float(obj)
        elif isinstance(obj, np.bool_):
            return bool(obj)
        elif isinstance(obj, datetime):
            return obj.strftime("%Y%m%d %H:%M:%S")
        elif isinstance(obj, Path):
            return str(obj)

        return json.JSONEncoder.default(self, obj)


def contains_callable_values(x):
    """
    检查数据结构 x 中是否包含可调用对象。

    Args:
        x (dict, list, tuple): 要检查的数据结构，可以是字典、列表或元组。

    Returns:
        bool: 如果 x 中包含可调用对象，则返回 True；否则返回 False。
    """
    if isinstance(x, dict):
        for value in x.values():
            if callable(value):
                return True
            if contains_callable_values(value):
                return True
    elif isinstance(x, (list, tuple)):
        for item in x:
            if contains_callable_values(item):
                return True
    return False


def has_own_method(input_cls, method: str):
    """
    检查传入的类（或其实例）是否具有自定义的方法。

    Args:
        input_cls (class or instance): 要检查的类或其实例。
        method (str): 要检查的方法名。

    Returns:
        bool: 如果传入的类（或其实例）具有自定义的指定方法，则返回 True；否则返回 False。

    说明：
        - 该函数首先检查 `input_cls` 是否具有名为 `method` 的属性。
        - 如果有，则获取该属性（即方法）的值，并检查它是否是一个方法或函数。
        - 如果是方法或函数，则进一步检查该方法的限定名（__qualname__）是否以类的名称（或实例的类名称）加上点号开头，
          以此来确定该方法是否为自定义的（即不是从父类继承而来的）。
        - 如果满足上述条件，则返回 True；否则返回 False。
    """
    if hasattr(input_cls, method):
        md = getattr(input_cls, method)
        if ismethod(md) or isfunction(md):
            cls_name = input_cls.__name__ if isinstance(input_cls, type) else input_cls.__class__.__name__
            return md.__qualname__.startswith(cls_name + ".")
    return False


def load_module_from_file(file: Union[Path, str]) -> ModuleType:
    """
    从文件中加载Python模块。

    Args:
        file (Union[Path, str]): 文件路径，可以是Path对象或字符串。

    Returns:
        ModuleType: 加载的模块对象。

    Raises:
        ValueError: 如果文件不是Python文件或文件不存在，则引发此异常。

    """

    file = Path(file)
    if not file.is_file() or not file.suffix.startswith(".py"):
        raise ValueError("文件类型非python文件")

    module_name = file.stem
    spec = spec_from_file_location(module_name, file)
    if spec is None:
        raise ImportError(f"无法为文件 {file} 创建模块规范")
    module = module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
    except Exception as e:
        del sys.modules[module_name]
        raise ImportError(f"加载模块 {module_name} 失败") from e
    return module


