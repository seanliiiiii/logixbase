from multiprocessing import Manager
from threading import Thread
from typing import Union
from pathlib import Path
from collections import defaultdict
from typing import Callable, Dict, List, Literal, Optional
import inspect

from .main import MultiTaskExecutor
from ..protocol import OperationProtocol as OPP


def _resolve_bound_arguments(func: Callable, args: tuple, kwargs: dict, upstream_keys: Optional[List[str]] = None):
    """
    在注册阶段解析并绑定用户参数，仅针对固定部分。
    返回:
      final_kwargs: 已绑定且应用默认值的参数字典（不含上游注入）
      injected_keys: 等待从上游注入的参数字段列表
    """
    sig = inspect.signature(func)
    bound = sig.bind_partial(*args, **kwargs)
    bound.apply_defaults()
    final_kwargs = dict(bound.arguments)
    injected_keys = []
    if upstream_keys:
        for key in upstream_keys:
            if key not in final_kwargs:
                injected_keys.append(key)
    return final_kwargs, injected_keys


class Coordinator(OPP):
    """
    多任务执行器的调度器，适用于多任务统一注册、分阶段完成、具备上下游数据广播关系的任务

    多任务执行器，支持process / thread 三种模式。
    - process： 使用futures.ProcessPoolExecutor，非阻塞执行进程池
    - pool: 使用mp.Pool，阻塞执行进程池， 不支持start后启动无法pickle的实例
    - thread：非阻塞执行线程池

    task_mode:
    - once： 单次执行，用户函数只定义业务逻辑
    - loop: 持久化运行，用户函数灵活控制数据流转逻辑
    - hybrid：统一持久化运行，用户函数只定义业务逻辑

    Args:
        mode (str, optional): 模式，默认为 'process'。支持的模式包括 'process', 'pool', 'thread'。
        task_mode (str, optional): 函数执行逻辑，默认为hybrid，支持的模式包括 once / loop / hybrid
        **kwargs:
            - thread: max_worker
            - process: max_worker / maxtasksperchild / batch_size
    Raises:
        ValueError: 如果输入的模式不在支持的模式列表中时抛出。
    """
    def __init__(self,
                 mode: Literal["process", "thread"] = 'process',
                 task_mode: Literal["once", "hybrid", "loop"] = "hybrid",
                 **kwargs):
        self.mode = mode.lower()
        self.task_mode = task_mode.lower()
        self._kwargs = kwargs

        self.manager = Manager()
        self.stop_event = self.manager.Event()
        self._monitor_thread: Optional[Thread] = None

        # 每阶段存储任务定义dict: {final_kwargs, injected_keys, req_queue, pub_queue, worker}
        self._tasks_by_order: Dict[int, List[Dict]] = defaultdict(list)
        self._req_queues: Dict[int, List] = defaultdict(list)
        self._pub_queues: Dict[int, List] = defaultdict(list)
        self._task_count: Dict[int, int] = defaultdict(int)
        self.executor = MultiTaskExecutor(self.mode, **self._kwargs)
        self._started = False

    def bind_share(self, **kwargs):
        """绑定共享上下文（如 logger），传递给 executor。"""
        self.executor.bind_share(**kwargs)

    def submit(self,
               order: int,
               func: Callable,
               *args,
               upstream_keys: Optional[List[str]] = None,
               group=None,
               tags=None,
               task_name=None,
               **kwargs):
        """
        注册任务，预解析所有固定参数。
        user func 签名示例:
            def my_task(a, b, c=None, from_upstream=None):
                ...
        upstream_keys 用于指定哪些参数从上游结果中提取。"""
        if self._started:
            raise RuntimeError("任务注册必须在 start() 之前完成")
        # 参数绑定与解析
        final_kwargs, injected_keys = _resolve_bound_arguments(func, args, kwargs, upstream_keys)
        # 创建通信队列
        req_q = self.manager.Queue()
        pub_q = self.manager.Queue()
        self._req_queues[order].append(req_q)
        self._pub_queues[order].append(pub_q)

        # 提交给执行器: worker(func, final_kwargs, injected_keys, broadcast)
        broadcast = (req_q, pub_q, self.stop_event)

        task_id = self.executor.submit(
            _worker,
            self.task_mode,
            func,
            final_kwargs,
            injected_keys,
            broadcast,
            group=group,
            tags=tags,
            task_name=task_name
        )
        self._tasks_by_order[order].append(task_id)
        self._task_count[order] += 1

    def _reorder(self):
        sorted_orders = sorted(self._task_count)
        order_map = {orig: idx + 1 for idx, orig in enumerate(sorted_orders)}
        self._tasks_by_order = {order_map[k]: v for k, v in self._tasks_by_order.items()}
        self._task_count = {order_map[k]: v for k, v in self._task_count.items()}
        self._req_queues = {order_map[k]: v for k, v in self._req_queues.items()}
        self._pub_queues = {order_map[k]: v for k, v in self._pub_queues.items()}

    def start(self):
        """启动所有任务及协调线程。"""
        self._started = True
        self._reorder()
        self.executor.start()
        self._monitor_thread = Thread(target=self._run, daemon=True)
        self._monitor_thread.start()

    def _run(self):
        # 首次广播：将自身 order 值放入队列以启动 once/hybrid/loop 起始
        for order, req_q_list in self._req_queues.items():
            for req_q in req_q_list:
                req_q.put(order)

        orders = sorted(self._tasks_by_order)
        order_idx = 0
        while not self.stop_event.is_set():
            current = orders[order_idx]
            collected = []
            for pub_q in self._pub_queues[current]:
                _collected = pub_q.get()
                if _collected is not None:
                    collected.append(pub_q.get())

            if not collected:
                continue

            next_idx = order_idx + 1
            if next_idx < len(orders):
                for req_q in self._req_queues[orders[next_idx]]:
                    req_q.put(collected)
                order_idx = next_idx
            else:
                break

    def join(self):
        self.executor.join()

    def stop(self):
        self.stop_event.set()
        # 发送结束信号None给每个任务子进程
        for order, req_q_list in self._req_queues.items():
            for req_q in req_q_list:
                req_q.put(None)
        # 发送结束信号至monitor线程
        for order, pub_q_list in self._pub_queues.items():
            for pub_q in pub_q_list:
                pub_q.put(None)
        self.executor.stop()

    def report(self, log_path: Union[str, Path], filemode: str = "csv",
               to_console=False, status=None, group=None, tag=None):
        self.executor.report(log_path, filemode, to_console, status, group, tag)


def _worker(task_mode: Literal["once", "loop", "hybrid"], func, kwargs, listen_vars: list, broadcast: tuple, logger = None):
    # 选择 worker
    if task_mode == "once":
        _single_worker(func, kwargs, listen_vars, broadcast, logger)
    elif task_mode == "hybrid":
        _hybrid_worker(func, kwargs, listen_vars, broadcast, logger)
    elif task_mode == "loop":
        _loop_worker(func, kwargs, broadcast, logger)
    else:
        raise ValueError(f"不支持的 task_mode: {task_mode}")


def _single_worker(func, kwargs, listen_vars: list, broadcast: tuple, logger = None):
    req_queue, pub_queue, stop_event = broadcast
    if logger:
        kwargs["logger"] = logger

    order = req_queue.get()
    print(f"非持久化任务：当前函数处于阶段{order}")
    if order == 1:
        result = func(**kwargs)
    else:
        upstream = req_queue.get()

        if isinstance(upstream, tuple) and int(len(listen_vars)) > 1:
            for idx, key in enumerate(upstream):
                kwargs[key] = upstream[key]
        elif int(len(listen_vars)) == 1:
            kwargs[listen_vars[0]] = upstream
        else:
            raise ValueError("监听的上游数据与变量不匹配，请检查函数设置")

        result = func(**kwargs)
    pub_queue.put(result)


def _loop_worker(func, kwargs, broadcast: tuple, logger = None):
    req_queue, pub_queue, stop_event = broadcast
    kwargs["req_queue"] = req_queue
    kwargs["pub_queue"] = pub_queue
    kwargs["stop_event"] = stop_event
    if logger:
        kwargs["logger"] = logger
    order = req_queue.get()
    print(f"自定义持久化任务：当前函数处于阶段{order}")
    func(**kwargs)


def _hybrid_worker(func, kwargs, listen_vars: list, broadcast: tuple, logger = None):
    req_queue, pub_queue, stop_event = broadcast

    if logger:
        kwargs["logger"] = logger

    order = req_queue.get()
    print(f"持久化任务：当前函数处于阶段{order}")
    while not stop_event.is_set():
        try:
            if order == 1:
                result = func(**kwargs)
            else:
                upstream = req_queue.get()

                if upstream is None:
                    continue

                if isinstance(upstream, tuple) and int(len(listen_vars)) > 1:
                    for idx, key in enumerate(upstream):
                        kwargs[key] = upstream[key]
                elif int(len(listen_vars)) == 1:
                    kwargs[listen_vars[0]] = upstream
                else:
                    raise ValueError("监听的上游数据与变量不匹配，请检查函数设置")
                result = func(**kwargs)
            pub_queue.put(result)
        except Exception as e:
            if logger:
                logger.ERROR(f"阶段{order}任务执行失败 | {func.__name__} | {kwargs} | {e}")
            pub_queue.put({})
