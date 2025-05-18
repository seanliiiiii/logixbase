from pydantic import BaseModel, Field
import multiprocessing as mp


class ExecutorConfig(BaseModel):
    mode: str = Field(default="thread", description="任务执行模式：process / pool / thread")
    max_workers: int = Field(default=mp.cpu_count() - 1, description="最大使用CPU数量")
    batch_size: int = Field(default=100, description="每个批次的执行任务数量")
    maxtasksperchild: int = Field(default=1, description="每个子进程最多执行的任务数")
