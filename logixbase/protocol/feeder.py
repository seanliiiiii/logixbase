from abc import ABC, abstractmethod


class FeederProtocol(ABC):
    """
    数据库接口协议DBP
    """
    @abstractmethod
    def on_init(self):
        pass

    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def disconnect(self):
        pass
