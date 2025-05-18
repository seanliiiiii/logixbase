# -*- coding: utf-8 -*-
import os
from datetime import datetime
import matplotlib.pyplot as plt
import json
import pandas as pd
from pathlib import Path
from collections import Counter
from pprint import pprint
from typing import Union

from .process import ProcessExecutor
from .thread import ThreadExecutor
from .task import Task, TaskRegistry
from .base import BaseExecutor


class MultiTaskExecutor(BaseExecutor):
    """
    多任务执行器，支持process / pool / thread 三种模式。
    - process： 使用futures.ProcessPoolExecutor，非阻塞执行进程池
    - pool: 使用mp.Pool，阻塞执行进程池， 不支持start后启动无法pickle的实例
    - thread：非阻塞执行线程池

    Args:
        mode (str, optional): 模式，默认为 'process'。支持的模式包括 'process', 'pool', 'thread'。
        **kwargs:
            - thread: max_worker
            - process: max_worker / maxtasksperchild / batch_size
    Raises:
        ValueError: 如果输入的模式不在支持的模式列表中时抛出。
    """
    def __init__(self, mode: str='process', **kwargs):
        super().__init__(mode)

        if self.mode in ("process", "pool"):
            kwargs = {k: v for k, v in kwargs.items() if k in ("max_workers", "maxtasksperchild", "batch_size")}
            self.executor = ProcessExecutor(mode, **kwargs)
        elif self.mode == 'thread':
            kwargs = {k: v for k, v in kwargs.items() if k in ("max_workers",)}
            self.executor = ThreadExecutor(**kwargs)
        else:
            raise ValueError(f"请输入正确的模式: Pool / Process / Thread")

        self.task_registry = TaskRegistry()

    def bind_share(self, **kwargs):
        self.executor.bind_share(**kwargs)

    def submit(self, func, *args, group=None, tags=None, task_name=None, **kwargs):
        task_id = self.executor.submit(func, *args, task_name=task_name, **kwargs)
        task = Task(task_id, func, args, kwargs, group=group, tags=tags)
        self.task_registry.register(task)
        return task_id

    def start(self):
        self.executor.start()

    def join(self, return_results=True):
        return self.executor.join(return_results=return_results)

    def get_task_ids(self):
        return self.executor.get_task_ids()

    def get_task_info(self, task_id):
        return self.executor.get_task_info(task_id)

    def get_result(self, task_id):
        return self.executor.get_result(task_id)

    def get_status(self, task_id):
        return self.executor.get_status(task_id)

    def get_elapsed(self, task_id):
        return self.executor.get_elapsed(task_id)

    def get_memory_usage(self, task_id):
        return self.executor.get_memory_usage(task_id)

    def reset(self):
        self.executor.reset()

    def stop(self):
        self.executor.stop()

    def get_by_group(self, group):
        return self.task_registry.get_by_group(group)

    def get_by_tag(self, tag):
        return self.task_registry.get_by_tag(tag)

    def is_cancelled(self, task_id):
        task = self.task_registry.get(task_id)
        return task.cancelled if task else False

    def cancel_task(self, task_id):
        self.task_registry.cancel(task_id)
        return True

    def cancel_group(self, group):
        self.task_registry.cancel_group(group)

    def retry_failed(self):
        failed_ids = [tid for tid in self.get_task_ids() if self.get_status(tid) == 'error']
        for tid in failed_ids:
            name, func = self.get_task_info(tid)
            args, kwargs = self.task_registry.get(tid).args, self.task_registry.get(tid).kwargs
            group = self.task_registry.get(tid).group
            tags = self.task_registry.get(tid).tags
            self.submit(func, *args, group=group, tags=tags, task_name=name, **kwargs)

    def rerun_cancelled(self):
        cancelled_ids = [tid for tid in self.get_task_ids() if self.is_cancelled(tid)]
        for tid in cancelled_ids:
            name, func = self.get_task_info(tid)
            args, kwargs = self.task_registry.get(tid).args, self.task_registry.get(tid).kwargs
            group = self.task_registry.get(tid).group
            tags = self.task_registry.get(tid).tags
            self.submit(func, *args, group=group, tags=tags, task_name=name, **kwargs)

    def summary(self):
        summary_data = []
        for task_id in self.get_task_ids():
            name, _ = self.get_task_info(task_id)
            status = self.get_status(task_id)
            elapsed = self.get_elapsed(task_id)
            memused = self.get_memory_usage(task_id)
            result = self.get_result(task_id)
            summary_data.append({
                "任务ID": task_id,
                "任务名称": name,
                "任务状态": status,
                "总耗时": elapsed,
                "内存占用": memused,
                "任务返回": result
            })
        return summary_data

    def summary_by_group(self):
        return self.task_registry.summary_by_group()

    def summary_by_tag(self):
        return self.task_registry.summary_by_tag()

    def to_dataframe(self):
        return pd.DataFrame(self.summary())

    def export_csv(self, folder: Union[Path, str]=".\logs"):
        df = self.to_dataframe()
        df.to_csv(str(folder) + r"\task_summary.csv", index=False)

    def export_json(self, folder: Union[Path, str]=".\logs"):
        filename = str(folder) + r"\task_summary.json"
        with open(filename, 'w', encoding='utf-8', newline='') as f:
            json.dump(self.summary(), f, indent=2, ensure_ascii=False, default=str)

    def export_html(self, folder: Union[Path, str]=".\logs"):
        df = self.to_dataframe()
        df.to_html(str(folder) + r"\task_summary.html", index=False)

    def plot_timeline(self):
        df = self.to_dataframe()
        df = df[df['总耗时'].notnull()].sort_values(by='总耗时', ascending=False)
        ax = plt.figure(figsize=(10, max(3., 0.4 * len(df))))
        plt.barh(df['任务名称'], df['总耗时'], color='skyblue')
        plt.xlabel("执行耗时 (s)")
        plt.title("任务执行时间")
        plt.tight_layout()
        return ax

    def plot_status_distribution(self):
        from collections import Counter
        counts = Counter([self.get_status(tid) for tid in self.get_task_ids()])
        if not counts:
            print("无任务状态数据画图")
            return

        labels, sizes = zip(*counts.items())
        ax = plt.figure(figsize=(6, 6))
        plt.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=140)
        plt.title("任务状态分布图")
        plt.tight_layout()
        return ax

    def plot_memory_usage(self):
        df = self.to_dataframe()
        if '任务返回' not in df.columns:
            print("无任务返回数据画图")
            return
        df = df[df['内存占用'].notnull()].sort_values(by='内存占用', ascending=False)
        if df.empty:
            print("无内存使用数据用于画图.")
            return

        ax = plt.figure(figsize=(10, max(3., 0.4 * len(df))))
        plt.barh(df['任务名称'], df['内存占用'], color='orange')
        plt.xlabel("内存占用 (MB)")
        plt.title("任务内存占用")
        plt.tight_layout()
        return ax

    def filter_summary(self, status=None, group=None, tag=None):
        filtered = []
        task_ids = set(self.get_task_ids())
        if group:
            task_ids &= set(self.get_by_group(group))
        if tag:
            task_ids &= set(self.get_by_tag(tag))
        for task_id in task_ids:
            if status and self.get_status(task_id) != status:
                continue
            name, _ = self.get_task_info(task_id)
            task = {
                "任务ID": task_id,
                "任务名称": name,
                "任务状态": self.get_status(task_id),
                "总耗时": self.get_elapsed(task_id),
                "内存占用": self.get_memory_usage(task_id),
                "任务返回": self.get_result(task_id),
                "是否取消": self.is_cancelled(task_id)
            }
            filtered.append(task)
        return filtered

    def report(self, log_path: Union[str, Path], filemode: str = "csv",
               to_console=False, status=None, group=None, tag=None):
        """
        生成并保存任务执行报告。
        Args:
            log_path (Union[str, Path]): 日志文件的路径，支持str和Path类型。
            filemode (str, optional): 文件模式，默认为"csv"。支持"csv", "json", "html"三种模式。
            to_console (bool, optional): 是否将报告打印到控制台，默认为False。
            status (Union[str, List[str]], optional): 需要过滤的任务状态，可以为单个状态或状态列表。默认为None，表示不过滤。
            group (Union[str, List[str]], optional): 需要过滤的任务分组，可以为单个分组或分组列表。默认为None，表示不过滤。
            tag (Union[str, List[str]], optional): 需要过滤的任务标签，可以为单个标签或标签列表。默认为None，表示不过滤。
        Raises:
            ValueError: 如果filemode参数不是"csv", "json", "html"之一，将抛出此异常。
        """

        # 获取数据
        if status or group or tag:
            summary = self.filter_summary(status=status, group=group, tag=tag)
        else:
            summary = self.summary()
        # 保存完整数据
        folder = Path(log_path).joinpath("task_summary").joinpath(datetime.now().strftime("%Y%m%d%H%M%S"))
        os.makedirs(folder, exist_ok=True)
        if filemode.lower() == "csv":
            self.export_csv(folder)
        elif filemode.lower() == "json":
            self.export_json(folder)
        elif filemode.lower() == "html":
            self.export_html(folder)
        else:
            raise ValueError(f"无法识别目标文件类型：{filemode}")
        # 保存统计图
        ax1 = self.plot_timeline()
        ax2 = self.plot_status_distribution()
        ax3 = self.plot_memory_usage()

        ax1.savefig(folder.joinpath("task_timeline.png"))
        ax2.savefig(folder.joinpath("task_status.png"))
        ax3.savefig(folder.joinpath("task_memory_usage.png"))

        # 报告打印
        if to_console:
            print(f"===== 任务池执行报告: {self.mode} =====")
            print("总任务数:", len(self.get_task_ids()))
            print("任务状态统计:", dict(Counter([self.get_status(tid) for tid in self.get_task_ids()])))
            print("任务分组统计:", self.summary_by_group())
            print("任务标签统计:", self.summary_by_tag())
            print("--- 任务详细统计 ---")
            pprint(summary)
