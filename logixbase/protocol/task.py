from typing import Protocol
from abc import abstractmethod


class SerializeProtocol(Protocol):
    """
    任务可序列化对象接口协议SLP
    可序列化对象，用于多进程/多线程安全传参、跨模块通信、分布式通信
    """
    def serialize(self) -> dict:
        pass

    @classmethod
    @abstractmethod
    def deserialize(cls, data: dict):
        pass