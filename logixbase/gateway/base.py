from abc import abstractmethod
from typing import Union, Callable
import uuid

from ..trader import TickData
from ..protocol import GatewayProtocol as GTP, IdentityProtocol as IDP


class BaseGateway(GTP, IDP):
    """
    接口基类
    """
    exchanges: list = []

    def __init__(self, gateway_name: str, config, logger = None) -> None:
        self._config = config
        self._name: str = gateway_name
        self._uuid: str = str(uuid.uuid4())

        self.logger = logger


        self._config.mdfront = self.format_front(self._config.mdfront)
        self._config.tdfront = self.format_front(self._config.tdfront)

        self._callback: Union[Callable, None] = None

    def INFO(self, msg: str):
        if self.logger is not None:
            self.logger.INFO(msg)
        else:
            print(f"[{self._name}Gateway] [INFO] {msg}")

    def ERROR(self, msg: str):
        if self.logger is not None:
            self.logger.ERROR(msg)
        else:
            print(f"[{self._name}Gateway] [ERROR] {msg}")

    @staticmethod
    def format_front(fronts) -> list:
        """处理前置柜台地址"""
        if not isinstance(fronts, (tuple, list)):
            fronts = [fronts]

        new_front = []
        for front in fronts:
            if (not front.startswith("tcp://")
                    and not front.startswith("ssl://")
                    and not front.startswith("socks")):
                front = "tcp://" + front
            new_front.append(front)

        return new_front

    def get_name(self) -> str:
        return self._name

    def get_id(self) -> str:
        return f"{self._name}:{self._uuid}"

    @abstractmethod
    def connect(self) -> None:
        """创建连接"""
        pass

    def register_callback(self, callback: Callable) -> None:
        self._callback = callback

    def on_tick(self, tick: Union[TickData, tuple]):
        """"""
        self._callback(tick)

    @abstractmethod
    def join(self):
        pass

    @abstractmethod
    def disconnect(self) -> None:
        """关闭连接"""
        pass

    @abstractmethod
    def subscribe(self, instruments: list) -> None:
        """订阅合约"""
        pass

    @abstractmethod
    def query_instrument(self) -> None:
        """查询合约信息"""
        pass

    @abstractmethod
    def send_order(self, req) -> None:
        """委托下单"""
        pass

    @abstractmethod
    def cancel_order(self, req) -> None:
        """委托撤单"""
        pass

    @abstractmethod
    def query_account(self) -> None:
        """查询账户资金信息"""
        pass

    @abstractmethod
    def query_position(self) -> None:
        """查询持仓"""
        pass


