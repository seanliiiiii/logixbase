# -*- coding: utf-8 -*-

class Task:
    """
    初始化任务实例。
    Args:
        task_id (str): 任务ID。
        func (callable): 任务执行的函数。
        args (tuple): 传递给函数的位置参数。
        kwargs (dict): 传递给函数的关键字参数。
        group (Optional[str]): 任务所属的组名，默认为None。
        tags (Optional[List[str]]): 任务标签列表，默认为None。
    Attributes:
        task_id (int): 任务ID。
        func (callable): 任务执行的函数。
        args (tuple): 传递给函数的位置参数。
        kwargs (dict): 传递给函数的关键字参数。
        group (Optional[str]): 任务所属的组名，默认为None。
        tags (List[str]): 任务标签列表，默认为空列表。
        status (str): 任务状态，可选值为"pending"（待处理）、"done"（已完成）、"error"（出错），默认为"pending"。
        result (Any): 任务执行结果，默认为None。
        elapsed (Optional[float]): 任务执行耗时，单位为秒，默认为None。
        cancelled (bool): 任务是否被取消，默认为False。
    """

    def __init__(self, task_id, func, args, kwargs, group=None, tags=None):

        self.task_id: str = task_id
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.group = group
        self.tags = tags or []
        self.status = "pending"         # pending / done / error
        self.result = None
        self.elapsed = None
        self.cancelled = False

    def mark_cancelled(self):
        self.cancelled = True


class TaskRegistry:
    """
    任务注册组件，并创建三个空字典来存储任务、分组和标签。
    Args:
        无参数。
    Returns:
        无返回值。
    """

    def __init__(self):

        self.tasks = {}
        self.groups = {}
        self.tags = {}

    def register(self, task: Task):
        """
        注册一个任务。
        Args:
            task (Task): 要注册的任务对象。
        Returns:
            None
        """
        self.tasks[task.task_id] = task
        if task.group:
            self.groups.setdefault(task.group, []).append(task.task_id)
        for tag in task.tags:
            self.tags.setdefault(tag, []).append(task.task_id)

    def get_by_group(self, group):
        """
        根据分组名获取分组中的元素列表。
        Args:
            group (str): 分组名。
        Returns:
            list: 分组中的元素列表。如果分组不存在，则返回一个空列表。
        """
        return self.groups.get(group, [])

    def get_by_tag(self, tag):
        """
        根据标签获取对应的标签列表。
        Args:
            tag (str): 要获取的标签。
        Returns:
            list: 对应标签下的内容列表，若标签不存在则返回空列表。
        """
        return self.tags.get(tag, [])

    def get(self, task_id):
        """
        根据任务ID获取任务。
        Args:
            task_id (str): 任务ID。
        Returns:
            Task: 返回对应的任务字典，如果任务不存在则返回None。
        """
        return self.tasks.get(task_id)

    def cancel(self, task_id):
        """
        取消任务。
        Args:
            task_id (int): 任务ID。
        Returns:
            None
        Raises:
            KeyError: 如果指定的任务ID不存在。
        """
        if task_id in self.tasks:
            self.tasks[task_id].mark_cancelled()

    def cancel_group(self, group):
        """
        取消特定组的所有任务。
        Args:
            group (str): 要取消的任务组的标识。
        Returns:
            None
        """
        for tid in self.groups.get(group, []):
            self.cancel(tid)

    def summary_by_group(self):
        """
        按组生成摘要。
        Args:
            无
        Returns:
            dict: 包含每个组及其对应tids长度的字典。
        """
        return {group: len(tids) for group, tids in self.groups.items()}

    def summary_by_tag(self):
        """
        根据标签统计并返回每个标签对应的项的数量。
        Args:
            无参数。
        Returns:
            dict: 一个字典，键为标签名称，值为该标签对应的项的数量。
        """
        return {tag: len(tids) for tag, tids in self.tags.items()}
