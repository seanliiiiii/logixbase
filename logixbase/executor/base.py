# -*- coding: utf-8 -*-
import uuid
from abc import ABC, abstractmethod
from typing import Callable
from . import context as ctx
from .payload import TaskEnvelope

from ..protocol import OperationProtocol as OPP, IdentityProtocol as IDP


class BaseExecutor(ABC, OPP, IDP):
    """
    任务执行器基类
    Args:
        无
    Returns:
        无
    Attributes:
        tasks (list): 存储任务对象的列表。
        task_id_map (dict): 用于将任务ID映射到任务对象的字典。
        _task_counter (int): 用于生成唯一任务ID的计数器。
        _results (dict): 存储任务执行结果的字典。
        _statuses (dict): 存储任务执行状态的字典。
        _elapsed (dict): 存储任务执行耗时的字典。
        _memory_usage (dict): 存储任务执行时内存使用情况的字典。
        _context (object): 存储上下文信息的对象。
    """
    def __init__(self, mode: str):

        self.mode: str = mode.lower()

        if self.mode == "thread":
            self._name: str = "多线程任务"
        elif self.mode == "process":
            self._name = "多进程任务"
        elif self.mode == "pool":
            self._name = "进程池任务"
        else:
            raise ValueError("请传入正确的任务模式： thread / process / pool")

        self._uuid: str = str(uuid.uuid4())

        self.tasks = []
        self.task_id_map = {}
        self._task_counter = 0

        self._results = {}
        self._statuses = {}
        self._elapsed = {}
        self._memory_usage = {}

        self._context = None

    def get_name(self) -> str:
        return self._name

    def get_id(self) -> str:
        return f"{self._name}:{self._uuid}"

    @abstractmethod
    def start(self):
        """
        开始执行方法。
        Args:
            self: 类实例对象。
        Returns:
            无返回值。
        Raises:
            NotImplementedError: 如果子类没有实现此方法，则会抛出此异常。
        """
        pass

    @abstractmethod
    def join(self, return_results=True):
        """
        将当前任务加入到任务队列中并等待其执行完毕。
        Args:
            return_results (bool): 默认为True。若设置为True，则方法返回执行结果；
                                   若设置为False，则方法不返回执行结果。
        Returns:
            若return_results为True，则返回执行结果；
            若return_results为False，则不返回任何值。
        Raises:
            NotImplementedError: 子类必须实现该方法。
        """
        pass

    def stop(self):
        """
        关闭当前实例。
        Args:
            无
        Returns:
            无
        Raises:
            无
        """
        self.reset()


    def bind_share(self, **kwargs):
        self._context = ctx.ShareContext(**kwargs)

    def submit(self, func: Callable, *args, task_name: str=None, **kwargs):
        """
        提交一个任务到任务队列中。
        Args:
            func (Callable): 要执行的任务函数。
            *args: 传递给任务函数的可变参数。
            task_name (str, optional): 任务名称。如果未提供，则默认使用任务函数的名称。默认为None。
            **kwargs: 传递给任务函数的关键字参数。
        Returns:
            str: 任务的唯一标识符。
        """
        self._task_counter += 1
        name = task_name or func.__name__
        task_id = self._gen_task_id(name)

        func_path = f"{getattr(func, '__module__', 'unknown')}.{getattr(func, '__name__', 'anonymous')}"
        envelope = TaskEnvelope(task_id=task_id, func_path=func_path, args=args, kwargs=kwargs)

        self.task_id_map[task_id] = (name, func)
        self.tasks.append(envelope.serialize())
        self._statuses[task_id] = "pending"

        return task_id


    def _gen_task_id(self, name):
        """
        生成任务ID。
        Args:
            name (str): 任务名称。
        Returns:
            str: 生成的任务ID。
        """
        return f"子任务{self._task_counter}_{uuid.uuid4().hex[:6]}"

    def reset_tracking(self):
        """
        重置跟踪数据。
        Args:
            无
        Returns:
            无
        说明:
            该方法将重置 _results, _statuses, _elapsed 和 _memory_usage 四个属性，
            清空其中存储的所有数据。
        """
        self._results.clear()
        self._statuses.clear()
        self._elapsed.clear()
        self._memory_usage.clear()

    def reset(self):
        """
        重置方法。
        Args:
            无
        Returns:
            无
        Raises:
            无
        该函数将重置任务列表（tasks）、任务ID映射（task_id_map）和任务计数器（_task_counter）
        至初始状态，并调用 reset_tracking 方法重置跟踪状态。
        """
        self.tasks.clear()
        self.task_id_map.clear()
        self._task_counter = 0
        self.reset_tracking()

    def get_status(self, task_id):
        """
        获取指定任务的状态。
        Args:
            task_id (str): 任务的唯一标识符。
        Returns:
            str: 任务的状态，如果不存在则返回 None。
        """
        return self._statuses.get(task_id)

    def get_elapsed(self, task_id):
        """
        获取指定任务的已用时间。
        Args:
            task_id (int): 任务ID。
        Returns:
            float: 指定任务的已用时间（秒）。
        """
        return self._elapsed.get(task_id)

    def get_memory_usage(self, task_id):
        """
        获取指定任务的内存使用情况。
        Args:
            task_id (int): 任务ID。
        Returns:
            int: 指定任务的内存使用量，单位为MB。如果未找到指定任务，则返回None。
        """
        return self._memory_usage.get(task_id)

    def get_result(self, task_id):
        """
        根据任务ID获取任务结果。
        Args:
            task_id (str): 任务ID。
        Returns:
            str: 返回任务结果。
        """
        return self._results.get(task_id)

    def get_task_ids(self):
        """
        获取任务ID列表。
        Args:
            无
        Returns:
            List[int]: 包含所有任务ID的列表。
        """
        return list(self.task_id_map.keys())

    def get_task_info(self, task_id):
        """
        获取指定任务的信息。
        Args:
            task_id (str): 任务的唯一标识符。
        Returns:
            tuple: 包含任务名称和任务描述的一个元组。
                如果指定的任务不存在，则返回一个包含两个None的元组。
        """
        return self.task_id_map.get(task_id, (None, None))


