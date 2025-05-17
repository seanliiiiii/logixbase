from abc import ABC, abstractmethod


class GatewayProtocol(ABC):
    """
    数据库接口协议DBP
    """
    @abstractmethod
    def connect(self) -> None:
        """连接到接口"""
        pass

    @abstractmethod
    def join(self):
        """持久化"""
        pass

    @abstractmethod
    def disconnect(self):
        """断开连接"""
        pass

    @abstractmethod
    def subscribe(self, *args, **kwargs):
        """订阅合约"""
        pass

    @abstractmethod
    def query_instrument(self):
        """查询活跃合约"""
        pass

    @abstractmethod
    def send_order(self, req):
        """发送订单"""
        pass

    @abstractmethod
    def cancel_order(self, req):
        """撤销订单"""
        pass

    @abstractmethod
    def query_account(self):
        """查询账户信息"""
        pass

    @abstractmethod
    def query_position(self):
        """查询持仓信息"""
        pass
