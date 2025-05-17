from typing import Callable, Iterable
from time import time
import asyncio
from functools import wraps

from .tool import ProcessBar


def virtual(func: Callable) -> Callable:
    """
    将一个函数标记为“虚函数”，意味着该函数可以被重写。
    任何基类都应使用此装饰器或 @abstractmethod 装饰器来标记所有可以被子类（重新）实现的函数。

    Args:
        func (Callable): 要标记为虚函数的函数。

    Returns:
        Callable: 返回被标记为虚函数的函数。

    注意：
        虚函数是一种设计概念，用于在继承体系中提供方法重写的能力。
        这有助于实现多态性，使得子类可以提供特定于它们的实现。
    """
    return func


def timer(func):
    """
    计时器装饰器

    Args:
        func (function or class method): 需要被装饰的函数。

    Returns:
        function: 返回一个新的函数，该函数在被调用时会测量并打印原函数的执行时间。

    这是一个装饰器，用于测量并打印出被装饰函数执行所花费的时间。
    如果原函数是一个普通函数，则直接打印函数名和执行时间；
    如果原函数是一个类的方法，则打印类名和方法名以及执行时间。

    使用示例：
    ```python
    @timer
    def example_function():
        import time
        time.sleep(2)

    example_function()
    # 输出: example_function finished in 2.xx seconds...
    ```
    """
    def deco(*args, **kwargs):
        start_time = time()
        if func.__class__.__name__ == "function":
            func_name = func.__name__
        else:
            func_name = func.__self__.__class__.__name__ + "." + func.__name__

        res = func(*args, **kwargs)
        end_time = time()
        print(f"{func_name} finished in {end_time - start_time} seconds...")
        return res

    return deco


def progressor(func: Callable):
    """
    装饰器，用于为函数添加进度条功能。

    Args:
        func (Callable): 需要被装饰的函数。

    Returns:
        Callable: 包装后的函数。

    装饰器会检查被装饰函数的参数，查找第一个可迭代参数的长度作为进度条的总步数。
    如果找到可迭代参数，则会创建一个进度条，并在每次调用被装饰函数时更新进度条。
    进度条是否显示以及进度条的标题可以通过 `use_progress` 和 `title` 参数进行控制。

    Keyword Args:
        use_progress (bool, optional): 是否显示进度条。默认为 False。
        title (str, optional): 进度条的标题。默认为 "Progress"。

    Raises:
        ValueError: 如果没有找到可迭代参数，则抛出 ValueError 异常。
    """
    def wrapper(*args, **kwargs):
        # 获取 use_progress_bar 参数
        use_progress = kwargs.pop('use_progress', False)
        title = kwargs.pop("title", "Progress")

        if not use_progress:
            return func(*args, **kwargs)

        # 取第一个可迭代参数的长度作为进度条的总步数
        iterable = []
        for (ix, v) in enumerate(args):
            if isinstance(v, Iterable) and not isinstance(v, str):
                for v_ in v:
                    arg_ = {v_: v[v_]} if isinstance(v, dict) else v_
                    new_args = (tuple(list(args[:ix]) + [arg_] + list(args[ix + 1:])), kwargs)
                    iterable.append(new_args)
                break
        if not iterable:
            for (key, v) in kwargs.items():
                if isinstance(v, Iterable) and not isinstance(v, str):
                    for v_ in v:
                        arg_ = {v_: v[v_]} if isinstance(v, dict) else v_
                        kwargs_ = {i: j for (i, j) in kwargs.items() if i != key}
                        kwargs_[key] = arg_
                        new_args = (args, kwargs_)
                        iterable.append(new_args)
                    break

        if not iterable:
            raise ValueError("No iterable argument found")

        # 初始化进度条
        progress_bar = ProcessBar(size=len(iterable), title=title)

        # 包装函数，显示进度条
        results = []
        for (i, (args_, kwargs_)) in enumerate(iterable):
            result = func(*args_, **kwargs_)
            results.append(result)
            progress_bar.show(i + 1)
        return results
    return wrapper



def silence_asyncio_warning(func):
    """装饰器：抑制 'Task was destroyed but it is pending' 警告"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        loop = asyncio.get_event_loop()
        orig_handler = loop.get_exception_handler()

        def quiet_handler(loop, context):
            msg = context.get("message")
            if msg and "Task was destroyed but it is pending" in msg:
                return  # 静默处理
            if orig_handler:
                orig_handler(loop, context)
            else:
                loop.default_exception_handler(context)

        loop.set_exception_handler(quiet_handler)
        try:
            return func(*args, **kwargs)
        finally:
            # 恢复原始处理器（可选）
            loop.set_exception_handler(orig_handler)

    return wrapper
