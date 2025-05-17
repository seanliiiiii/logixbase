# -*- coding: utf-8 -*-
import time
import psutil
import traceback
import multiprocessing as mp
from typing import Union
from concurrent.futures import ThreadPoolExecutor, as_completed

from .context import init_worker, get_context_kwargs
from .base import BaseExecutor
from .payload import import_func_from_path, TaskEnvelope


class ThreadExecutor(BaseExecutor):
    def __init__(self, max_workers: int=mp.cpu_count() + 4):
        super().__init__("thread")

        self.max_workers = max_workers

        self._context = None
        self._executor: Union[ThreadPoolExecutor, None] = None
        self._futures: dict = {}

    def start(self):
        self.reset_tracking()

        self._futures = {}
        self._executor = ThreadPoolExecutor(max_workers=self.max_workers)
        init_worker(self._context)
        for task in self.tasks:
            future = self._executor.submit(self._wrap_task, task)
            self._futures[future] = task["task_id"]

    def join(self, return_results=True):
        for future in as_completed(self._futures):
            task_id, result, elapsed, memused = future.result()
            self._elapsed[task_id] = elapsed
            self._memory_usage[task_id] = memused
            if return_results:
                self._results[task_id] = result
        self._executor.shutdown(wait=True)
        return self._results if return_results else None

    def _wrap_task(self, task):
        t0 = time.time()
        proc = psutil.Process()
        mem_start = proc.memory_info().rss / (1024 * 1024)

        envelope = TaskEnvelope.deserialize(task)
        task_id = envelope.task_id
        func = import_func_from_path(envelope.func_path)

        try:
            result = func(*envelope.args, **{**envelope.kwargs, **get_context_kwargs()})
            elapsed = time.time() - t0
            mem_used = proc.memory_info().rss / (1024 * 1024) - mem_start
            if self._context and self._context.logger:
                msg = f"任务执行成功 | [{task_id}] {func.__name__} | 总耗时: {elapsed:.2f}s | 内存占用: {mem_used:.2f}MB"
                self._context.logger.INFO(msg)
            self._statuses[task_id] = "done"

            return task_id,  result if result is not None else True, elapsed, mem_used
        except Exception as e:
            elapsed = time.time() - t0
            mem_used = proc.memory_info().rss / (1024 * 1024) - mem_start
            self._statuses[task_id] = "error"
            tb = traceback.format_exc()
            if self._context and hasattr(self._context, "logger"):
                msg = f"任务执行失败 | [{task_id}] {func.__name__} | 总耗时: {elapsed:.2f}s: {e} {tb}"
                self._context.logger.ERROR(msg)
            return task_id, None, elapsed, mem_used
