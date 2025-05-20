from abc import abstractmethod
import uuid
from typing import Union
from datetime import datetime

import pandas as pd

from ..protocol import FeederProtocol as FDP, IdentityProtocol as IDP


class BaseFeeder(FDP, IDP):

    def __init__(self, name: str, config, logger = None):
        self.conn_cfg = config
        self._logger = logger

        self._name: str = name
        self._uuid: str = str(uuid.uuid4())

    def INFO(self, msg: str):
        if self._logger is not None:
            self._logger.INFO(msg)
        else:
            print(f"[{self._name}Feeder] [INFO] {msg}")

    def ERROR(self, msg: str):
        if self._logger is not None:
            self._logger.ERROR(msg)
        else:
            print(f"[{self._name}Feeder] [ERROR] {msg}")

    def get_id(self):
        return f"{self._name}:{self._uuid}"

    def get_name(self):
        return self._name

    def on_init(self):
        pass

    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def disconnect(self):
        pass

    def is_connected(self):
        pass

    def check_connect(self):
        pass

    def all_tradeday(self, start: Union[str, datetime, int], end: Union[str, datetime, int], fmt: str = "datetime",
                      freq: str = "D", day: int = 1, market: str = "China"):
        pass

    @abstractmethod
    def asset_ticker(self, asset: str, active: bool) -> list:
        pass

    @abstractmethod
    def asset_info(self, asset: str, info: str = "basic") -> pd.DataFrame:
        """
        获取资产信息。
        Args:
            asset: str: 资产类型，包括Future, Stock, Index, Option, ETF。
            info (str, optional): 指定要获取的资产信息类型。可以是 "basic" 或 "trade"。默认为 "basic"
        Returns:
            None
        """
        pass

    @abstractmethod
    def asset_quote(self, asset: str, start: Union[datetime, str, int], end: Union[datetime, str, int],
                    interval: Union[str, int], ticker: Union[list, tuple] = None, underlying: Union[list, tuple] = None) -> pd.DataFrame:
        """
        获取资产行情数据。
        Args:
            asset (str): 资产类型，例如股票、债券、外汇等。
            start (Union[datetime, str, int]): 查询开始时间，可以是datetime对象、时间字符串或时间戳。
            end (Union[datetime, str, int]): 查询结束时间，可以是datetime对象、时间字符串或时间戳。
            interval (Union[str, int]): 查询的时间间隔，可以是字符串（如'1d'、'1w'等）或整数（表示秒数）。
            ticker (list, optional): 通用合约标识符列表，默认为None。如果指定，则只返回该列表中股票的数据。
            underlying (list, optional): 仅期权使用，期权底层合约列表，默认为None。如果指定，则只返回该列表中产品类型的数据。
        Returns:
            None

        Raises:
            NotImplementedError: 子类必须实现此方法。
        """
        pass

    def asset_main_ticker(self, asset: str, start: Union[datetime, str, int], end: Union[datetime, str, int],
                          ticker: Union[list, tuple] = None, underlying: Union[list, tuple] = None) -> Union[pd.DataFrame, None]:
        pass

    def edb_info(self, tickers: list = None, use_schema: bool = False):
        pass

    def edb_data(self, start: Union[datetime, str, int], end: Union[datetime, str, int], tickers: list):
        """获取万得EDB的经济数据"""
        pass

    def treasury_ctd(self, ticker: str, start: Union[datetime, str, int], end: Union[datetime, str, int]):
        """获取国债期货最廉交割券数据"""
        pass

    def asset_coef_adj(self, asset: str, product: Union[list, tuple], start: Union[datetime, str, int],
                       end: Union[datetime, str, int]):
        """获取资产除权系数"""
        pass