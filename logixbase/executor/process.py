# -*- coding: utf-8 -*-
from multiprocessing import Pool
import time
import traceback
import psutil
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, as_completed

from .context import init_worker, get_context_kwargs, global_context
from .base import BaseExecutor
from .payload import TaskEnvelope, import_func_from_path



class ProcessExecutor(BaseExecutor):
    """
    多进程任务执行器。
    Args:
        max_workers (int, optional): 最大工作进程数，默认为 CPU 核心数减一。
        maxtasksperchild (int, optional): 每个子进程最多执行的任务数，默认为 1。
        batch_size (int, optional): 每个批次的大小，默认为 100。
    Returns:
        None
    """
    def __init__(self,
                 mode: str = "pool",
                 max_workers: int=mp.cpu_count() - 1,
                 maxtasksperchild: int=1,
                 batch_size: int=100):
        super().__init__(mode)

        self.max_workers = max_workers
        self.maxtasksperchild = maxtasksperchild
        self.batch_size = batch_size

        self._task_queue = []
        self._futures_batches = []

    def start(self):
        self.reset_tracking()

        if not self.tasks:
            return

        self._task_queue = list(self.tasks)
        self._futures_batches = []

        if self.mode == "pool":
            while self._task_queue:
                current_batch = self._task_queue[:self.batch_size]
                self._task_queue = self._task_queue[self.batch_size:]

                pool = Pool(
                    processes=self.max_workers,
                    initializer=init_worker,
                    initargs=(self._context,),
                    maxtasksperchild=self.maxtasksperchild
                )
                futures = [pool.apply_async(_pool_worker, args=(task,)) for task in current_batch]
                self._futures_batches.append((pool, futures))
        else:
            pool = ProcessPoolExecutor(max_workers=self.max_workers, initializer=init_worker, initargs=(self._context,))
            self._futures_batches.append(pool)
            for task in self._task_queue:
                future = pool.submit(_pool_worker, task)
                self._futures_batches.append(future)

        self._started = True

    def join(self, return_results=True):

        if self.mode == "pool":
            for pool, futures in self._futures_batches:
                for future in futures:
                    task_id, result, elapsed, mem_used = future.get()
                    self._statuses[task_id] = "done" if result is not None else "error"
                    self._elapsed[task_id] = elapsed
                    self._memory_usage[task_id] = mem_used
                    if return_results:
                        self._results[task_id] = result
                pool.close()
                pool.join()
        else:
            if self._futures_batches:
                pool = self._futures_batches.pop(0)
                for future in as_completed(self._futures_batches):
                    task_id, result, elapsed, mem_used = future.result()
                    self._statuses[task_id] = "done" if result is not None else "error"
                    self._elapsed[task_id] = elapsed
                    self._memory_usage[task_id] = mem_used
                    if return_results:
                        self._results[task_id] = result
                pool.shutdown()

        return self._results if return_results else None


def _pool_worker(task):
    context = global_context()
    envelope = TaskEnvelope.deserialize(task)
    task_id = envelope.task_id
    func = import_func_from_path(envelope.func_path)

    t0 = time.time()
    proc = psutil.Process()
    mem_start = proc.memory_info().rss / (1024 * 1024)

    try:
        result = func(*envelope.args, **{**envelope.kwargs, **get_context_kwargs()})
        elapsed = time.time() - t0
        mem_end = proc.memory_info().rss / (1024 * 1024)
        mem_used = mem_end - mem_start

        if context and context.logger:
            context.logger.INFO(
                f"任务执行成功 | [{task_id}] {func.__name__} | 总耗时: {elapsed:.2f}s | 内存占用: {mem_used:.2f}MB"
            )

        return task_id, result if result is not None else True, elapsed, mem_used
    except Exception as e:
        elapsed = time.time() - t0
        mem_end = proc.memory_info().rss / (1024 * 1024)
        mem_used = mem_end - mem_start

        tb = traceback.format_exc()
        if context and context.logger:
            context.logger.ERROR(f"任务执行失败 | [{task_id}] {func.__name__}: {e}\n{tb}")
        return task_id, None, elapsed, mem_used
