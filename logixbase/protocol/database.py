from abc import ABC, abstractmethod


class DatabaseProtocol(ABC):
    """
    数据库接口协议DBP
    """
    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def disconnect(self):
        pass

    @abstractmethod
    def is_connected(self):
        pass

    @abstractmethod
    def check_connect(self):
        pass

    @abstractmethod
    def exec_query(self, query: str):
        pass

    @abstractmethod
    def exec_insert(self, insert: str, data: tuple):
        pass

    @abstractmethod
    def exec_multi_insert(self, insert: str, datas: list[tuple], insert_lim: int = 10000):
        pass

    @abstractmethod
    def exec_nonquery(self, query: str):
        pass
