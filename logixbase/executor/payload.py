from ..protocol import SerializeProtocol


class TaskEnvelope(SerializeProtocol):
    """
    标准任务封装结构：
    用于封装 func 路径、args、kwargs，支持跨进程传输、插件通信、UI 请求等。
    """
    def __init__(self, task_id: str, func: str, args=None, kwargs=None):
        self.task_id: str = task_id
        self.func = func  # e.g. 'logixsignal.tasks.run_signal'
        self.args = args or []
        self.kwargs = kwargs or {}

    def serialize(self) -> dict:
        return {
            "task_id": self.task_id,
            "func": self.func,
            "args": self.args,
            "kwargs": self.kwargs,
        }

    @classmethod
    def deserialize(cls, data: dict):
        return cls(
            task_id=data["task_id"],
            func=data["func"],
            args=data.get("args", []),
            kwargs=data.get("kwargs", {})
        )


def import_func_from_path(path: str):
    """从字符串路径动态导入函数，例如 'mypkg.module.run_signal'"""
    import importlib
    mod_path, func_name = path.rsplit(".", 1)
    module = importlib.import_module(mod_path)
    return getattr(module, func_name)
