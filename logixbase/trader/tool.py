from datetime import timedelta, datetime
from typing import Union, Callable
from copy import copy
import numpy as np
from numba import njit

from ..utils import has_own_method
from .schema import QmFactorInfo, EdbInfo, Interval, BarData, TickData, Exchange
from .utils import round_to


class StrategyTemplate:
    """
    Template for each strategy
    """
    parameters: list = []
    variables: list = []

    def __init__(self,
                 engine,
                 name: tuple,
                 interval: str,
                 product: Union[str, list, tuple],
                 setting: dict):
        self.engine = engine
        self.interval: str = interval
        self.name: tuple = name
        self.product: [tuple, str] = str(product) if isinstance(product, (str, int)) else [str(k) for k in product]
        self.setting: dict = setting

        self.inited: bool = False
        self.trading: bool = False
        self.weight: dict = {}
        self.pos: dict = {}

        self.is_on_bar: bool = False                                            # 是否在bar上交易
        self.is_on_tick: bool = False                                           # 是否在tick上交易

        self.variables = copy(self.variables)
        self.variables.insert(0, "inited")
        self.variables.insert(1, "trading")
        self.variables.insert(2, "weight")
        self.variables.insert(3, "pos")
        self.variables.insert(4, "is_on_bar")
        self.variables.insert(5, "is_on_tick")

        self.update_setting(setting)

    @property
    def asset(self) -> str:
        return self.engine.ASSET

    @property
    def ticker_info_all(self) -> dict:
        """所有合约信息"""
        return self.engine.info["ticker_info"]

    @property
    def trade_ticker_all(self) -> dict:
        """所有交易合约"""
        return self.engine.info["trade_ticker"]

    def ticker_info(self, ticker: str):
        """指定合约信息"""
        return self.engine.info["ticker_info"][ticker]

    def trade_ticker(self, product: str) -> str:
        """品种交易合约"""
        return self.engine.info["trade_ticker"][product]

    def qm_factor_info(self, product: str, factor: str) -> QmFactorInfo:
        """品种基本面因子信息"""
        return self.engine.info["qm_factor_info"][product][factor]

    def index_info(self, index_id: str) -> EdbInfo:
        """基本面指标信息"""
        return self.engine.info["edb_info"][index_id]

    def update_setting(self, setting: dict) -> None:
        """"""
        self.parameters = [k.lower() for k in self.parameters]
        for name in self.parameters:
            if name in setting:
                setattr(self, name, setting[name])

        self.is_on_bar = has_own_method(self, "on_bar")
        self.is_on_tick = has_own_method(self, "on_tick")

    def on_init(self) -> None:
        """Callback when strategy is inited"""
        pass

    def on_start(self) -> None:
        """Callback when strategy is started"""
        pass

    def on_bar(self, *args) -> None:
        """Callback of new bar data update, args: bar / edb, factor"""
        pass

    def on_tick(self, *args) -> None:
        """Callback of new tick data update, args: tick"""
        pass

    def send_weight(self, weight: dict) -> None:
        """
        发送目标持仓，通过组合管理模块轧差后统一市价执行交易
        :param weight: dict, {品种： 目标权重}
        """
        self.engine.on_target_weight(self.name, weight)
        self.weight = weight

    def send_pos(self, account: str, pos: dict) -> None:
        """
        发送直接下单指令，单策略独立通过执行策略执行
        :param account: 仓位对应的账户名
        :param pos: {品种: 目标仓位}
        :return:
        """
        self.engine.on_target_pos(self.name, account, pos)
        self.pos[account] = pos

    @staticmethod
    def update_array(array: np.ndarray, value: float):
        """更新array"""
        array[:-1] = array[1:]
        array[-1] = value

    def write_log(self, msg: str) -> None:
        """Write log event"""
        self.engine.write_log(msg)

    def write_error(self, msg: str) -> None:
        """Write error event"""
        self.engine.write_error(msg)


class BarGenerator:
    """
    Generate 1-min bar data from tick data and x-min/hour/day bar from 1min bar data
    * for x minute bar, x must be int
    ** Must start from the very first beginning of each trade day!!
    """
    def __init__(self, info, callback: Callable, interval: Interval):
        self.ticker: str = info.ticker                               # 标准合约代码
        self.info = info                                             # 合约信息类
        self.interval: Interval = interval                           # 行情周期

        self.callback: Callable = callback                           # 回调函数
        self.last_tick: Union[TickData, None] = None                 # 上一个tick
        self.bar: Union[BarData, None] = None                        # 最新的Bar
        self.last_bar: Union[BarData, None] = None                   # 上一根bar
        self.count: int = 0                                          # Bar数量统计
        self.executed = False                                        # Bar是否执行完成

        self.inited: bool = False

    def update_tick(self, tick: TickData) -> None:
        """Update new tick data into generator"""
        new_minute = False

        # Filter tick data with 0 last price
        if not tick.last_price:
            return
        # First bar in day
        if not self.bar:
            new_minute = True
        # 1-min Bar finished
        elif self.bar.bartime < tick.ticktime:
            # Complete 1-min bar and execute on_bar
            if not self.executed:
                self.on_new_bar()
            new_minute = True

        if new_minute:
            self.executed = False
            if self.bar:
                self.last_bar = BarData(**{k: v for (k, v) in self.bar.__dict__.items()})
            self.bar = BarData(asset=self.info.asset,
                               product=self.info.product,
                               ticker=self.ticker,
                               bartime=(tick.ticktime + timedelta(minutes=1)).replace(second=0, microsecond=0),
                               tradeday=tick.tradeday,
                               exchange=tick.exchange,
                               interval=Interval("1min"),
                               open=tick.last_price,
                               high=tick.last_price,
                               low=tick.last_price,
                               close=tick.last_price,
                               openinterest=tick.openinterest,
                               prevclose=self.last_bar.close if self.last_bar else tick.prevclose,
                               prevsettle=self.last_bar.settle if self.last_bar else tick.prevsettle)
            self.inited = True
        else:
            self.bar.high = max(self.bar.high, tick.last_price)
            self.bar.low = min(self.bar.low, tick.last_price)
            self.bar.close = tick.last_price
            self.bar.openinterest = tick.openinterest

        # Update bar's volume / amount with tick change
        if self.last_tick:
            # Not first tick in a trading day
            volume_chng = tick.volume - self.last_tick.volume
            self.bar.volume += max(volume_chng, 0)
            amount_chng = tick.amount - self.last_tick.amount
            self.bar.amount += max(amount_chng, 0)
        else:
            # First tick in a trading day
            self.bar.volume = tick.volume
            self.bar.amount = tick.amount

        # Record last tick
        self.last_tick = tick

    def update_bar(self, bar: BarData) -> None:
        """Update new 1min bar data to n-min bar data into generator"""
        # Filter duplicated bar
        if (self.bar is not None) and (bar.bartime <= self.bar.bartime):
            return

        # If not inited create window bar object
        if not self.bar:
            if self.last_bar:
                prevsettle = self.last_bar.settle
            else:
                prevsettle = 0

            self.bar = BarData(asset=self.info.asset,
                               product=bar.product,
                               ticker=bar.ticker,
                               bartime=bar.bartime,
                               tradeday=bar.tradeday,
                               exchange=bar.exchange,
                               interval=self.interval,
                               open=bar.open,
                               high=bar.high,
                               low=bar.low,
                               prevclose=bar.prevclose,
                               prevsettle=prevsettle)
            self.inited = True
        else:
            self.bar.bartime = bar.bartime
            self.bar.high = max(self.bar.high, bar.high)
            self.bar.low = min(self.bar.low, bar.low)
        # Update values into window bar
        self.bar.close = bar.close
        self.bar.volume += int(bar.volume)
        self.bar.amount += int(bar.amount)
        self.bar.openinterest = bar.openinterest
        self.bar.close_ret = round(np.log(self.bar.close) - np.log(self.bar.prevclose), 4)
        # Record temporary settle return of bar
        if not self.last_bar:
            self.bar.settleret = round((self.bar.settleret + 1) * (bar.settleret + 1) - 1, 4)
        # Update used 1min bar
        self.count += 1
        # Check if window bar completed
        finished = False
        if not self.count % self.interval.window:
            finished = True
            self.count = 0
        # Complete x-min bar
        if finished:
            self.on_new_bar_xmin()

    def on_new_bar(self):
        """Complete min bar data and execute to call on_bar"""
        self.executed = True
        if self.bar.volume:
            self.bar.settle = self.bar.amount / self.bar.volume / self.info.multiplier
        else:
            self.bar.settle = (self.bar.open
                                     + self.bar.high
                                     + self.bar.low
                                     + self.bar.close) / 4
        self.bar.settle = round_to(self.bar.settle, self.info.pricetick)
        # Update bar close
        self.bar.close_ret = round(np.log(self.bar.close) - np.log(self.bar.prevclose), 4)
        self.bar.settleret = round(np.log(self.bar.settle) - np.log(self.bar.prevsettle), 4)
        # Execute on_bar callback function
        self.callback(self.bar)

    def on_new_bar_xmin(self):
        """Complete xmin bar data and execute to call on_bar"""
        self.bar.bartime = self.bar.bartime.replace(second=0, microsecond=0)
        # Bar finished, update settle / prevsettle
        if self.bar.volume:
            self.bar.settle = self.bar.amount / self.bar.volume / self.info.multiplier
        else:
            self.bar.settle = (self.bar.open
                                     + self.bar.high
                                     + self.bar.low
                                     + self.bar.close) / 4
        self.bar.settle = round_to(self.bar.settle, self.info.pricetick)
        # Update prev-bar's settle
        if not self.last_bar:
            self.bar.prevsettle = round_to(self.bar.settle / (1 + self.bar.settleret), self.info.pricetick)
        else:
            self.bar.settleret = round(np.log(self.bar.settle) - np.log(self.bar.prevsettle), 4)
        # Execute on_bar callback function
        self.callback(self.bar)
        # Update bar objects
        self.last_bar = self.bar
        self.bar = None

    def default_base_bar(self, **kwargs):
        """Generate default 1min bar"""
        if self.bar is not None:
            bar = BarData(asset=self.info.asset,
                          product=self.bar.product,
                          ticker=self.ticker,
                          bartime=self.bar.bartime + timedelta(minutes=1),
                          tradeday=self.bar.tradeday,
                          exchange=self.bar.exchange,
                          interval=Interval("1min"),
                          open=self.bar.close,
                          high=self.bar.close,
                          low=self.bar.close,
                          close=self.bar.close,
                          prevclose=self.bar.close,
                          settle=self.bar.close,
                          prevsettle=self.bar.settle,
                          openinterest=self.bar.openinterest)
        else:
            exchange = Exchange(kwargs["Exchange"]) if isinstance(kwargs["Exchange"], str) else kwargs["Exchange"]
            bar = BarData(asset=self.info.asset,
                          product=kwargs["Contract"],
                          ticker=self.ticker,
                          bartime=kwargs["DateTime"],
                          tradeday=kwargs["TradeDay"],
                          exchange=exchange,
                          interval=Interval("1min"),
                          open=kwargs["Close"],
                          high=kwargs["Close"],
                          low=kwargs["Close"],
                          close=kwargs["Close"],
                          settle=kwargs["Close"],
                          prevsettle=kwargs["Settle"],
                          openinterest=kwargs["OpenInterest"])
        bar.settleret = round(np.log(bar.settle) - np.log(bar.prevsettle), 4)
        return bar


class TickArrayManager:
    """
    Time series container of tick data, could be used to calculate technical indicators
    """
    def __init__(self, product: str, size: int = 1500):
        """Constructor"""
        self.count: int = 0
        self.size: int = size
        self.inited: bool = False
        self.product: str = product

        self.ticktime_arr: np.array = np.array([datetime(1990, 5, 22)] * self.size, dtype=datetime)
        self.timestamp_arr: np.array = np.full(self.size, np.nan)
        self.instrument_arr: np.array = np.empty(self.size, dtype=object)

        self.last_price_arr: np.array = np.full(self.size, np.nan)
        self.bid_price_1_arr: np.array = np.full(self.size, np.nan)
        self.bid_price_2_arr: np.array = np.full(self.size, np.nan)
        self.bid_price_3_arr: np.array = np.full(self.size, np.nan)
        self.bid_price_4_arr: np.array = np.full(self.size, np.nan)
        self.bid_price_5_arr: np.array = np.full(self.size, np.nan)
        self.ask_price_1_arr: np.array = np.full(self.size, np.nan)
        self.ask_price_2_arr: np.array = np.full(self.size, np.nan)
        self.ask_price_3_arr: np.array = np.full(self.size, np.nan)
        self.ask_price_4_arr: np.array = np.full(self.size, np.nan)
        self.ask_price_5_arr: np.array = np.full(self.size, np.nan)
        self.bid_volume_1_arr: np.array = np.full(self.size, np.nan)
        self.bid_volume_2_arr: np.array = np.full(self.size, np.nan)
        self.bid_volume_3_arr: np.array = np.full(self.size, np.nan)
        self.bid_volume_4_arr: np.array = np.full(self.size, np.nan)
        self.bid_volume_5_arr: np.array = np.full(self.size, np.nan)
        self.ask_volume_1_arr: np.array = np.full(self.size, np.nan)
        self.ask_volume_2_arr: np.array = np.full(self.size, np.nan)
        self.ask_volume_3_arr: np.array = np.full(self.size, np.nan)
        self.ask_volume_4_arr: np.array = np.full(self.size, np.nan)
        self.ask_volume_5_arr: np.array = np.full(self.size, np.nan)

        self.open_price_arr: np.array = np.full(self.size, np.nan)
        self.high_price_arr: np.array = np.full(self.size, np.nan)
        self.low_price_arr: np.array = np.full(self.size, np.nan)
        self.volume_arr: np.array = np.full(self.size, np.nan)
        self.amount_arr: np.array = np.full(self.size, np.nan)
        self.open_interest_arr: np.array = np.full(self.size, np.nan)
        self.average_price_arr: np.array = np.full(self.size, np.nan)
        self.coef_adj_arr: np.array = np.full(self.size, np.nan)

        self.limit_up: float = 0                                   # Tradeday's price upper limit
        self.limit_down: float = 0                                 # Tradeday's price lower limit
        self.prev_close: float = 0                                 # Previous tradeday's close
        self.prev_settle: float = 0                                # Previous tradeday's total settle price
        self.prev_open_interest: float = 0                         # Previous tradeday's open interest at close time

    def update_tick(self, tick: TickData, coefadj: float) -> None:
        """Update new tick data into array manager"""
        self.instrument_arr[:-1] = self.instrument_arr[1:]
        self.ticktime_arr[:-1] = self.ticktime_arr[1:]
        self.timestamp_arr[:-1] = self.timestamp_arr[1:]
        self.last_price_arr[:-1] = self.last_price_arr[1:]
        self.bid_price_1_arr[:-1] = self.bid_price_1_arr[1:]
        self.ask_price_1_arr[:-1] = self.ask_price_1_arr[1:]
        self.bid_volume_1_arr[:-1] = self.bid_volume_1_arr[1:]
        self.ask_volume_1_arr[:-1] = self.ask_volume_1_arr[1:]

        self.open_price_arr[:-1] = self.open_price_arr[1:]
        self.high_price_arr[:-1] = self.high_price_arr[1:]
        self.low_price_arr[:-1] = self.low_price_arr[1:]
        self.volume_arr[:-1] = self.volume_arr[1:]
        self.amount_arr[:-1] = self.amount_arr[1:]
        self.open_interest_arr[:-1] = self.open_interest_arr[1:]
        self.average_price_arr[:-1] = self.average_price_arr[1:]
        self.coef_adj_arr[:-1] = self.coef_adj_arr[1:]

        t = tick.ticktime
        self.ticktime_arr[-1] = t
        self.timestamp_arr[-1] = (np.datetime64(t) - np.datetime64('1970-01-01T00:00:00')) / np.timedelta64(1, "s")
        self.instrument_arr[-1] = tick.instrument
        self.last_price_arr[-1] = tick.last_price
        self.bid_price_1_arr[-1] = tick.bid_price_1
        self.ask_price_1_arr[-1] = tick.ask_price_1
        self.bid_volume_1_arr[-1] = tick.bid_volume_1
        self.ask_volume_1_arr[-1] = tick.ask_volume_1
        self.open_price_arr[-1] = tick.open
        self.high_price_arr[-1] = tick.high
        self.low_price_arr[-1] = tick.low
        self.volume_arr[-1] = tick.volume
        self.amount_arr[-1] = tick.amount
        self.open_interest_arr[-1] = tick.openinterest
        self.average_price_arr[-1] = tick.settle

        self.coef_adj_arr[-1] = round(coefadj, 4)
        self.limit_up = tick.limit_up
        self.limit_down = tick.limit_down
        self.prev_close = tick.prevclose
        self.prev_settle = tick.prevsettle
        self.prev_open_interest = tick.prevopeninterest

        if tick.bid_price_2 or tick.ask_price_2:
            self.bid_price_2_arr[:-1] = self.bid_price_2_arr[1:]
            self.bid_price_3_arr[:-1] = self.bid_price_3_arr[1:]
            self.bid_price_4_arr[:-1] = self.bid_price_4_arr[1:]
            self.bid_price_5_arr[:-1] = self.bid_price_5_arr[1:]
            self.ask_price_2_arr[:-1] = self.ask_price_2_arr[1:]
            self.ask_price_3_arr[:-1] = self.ask_price_3_arr[1:]
            self.ask_price_4_arr[:-1] = self.ask_price_4_arr[1:]
            self.ask_price_5_arr[:-1] = self.ask_price_5_arr[1:]
            self.bid_volume_2_arr[:-1] = self.bid_volume_2_arr[1:]
            self.bid_volume_3_arr[:-1] = self.bid_volume_3_arr[1:]
            self.bid_volume_4_arr[:-1] = self.bid_volume_4_arr[1:]
            self.bid_volume_5_arr[:-1] = self.bid_volume_5_arr[1:]
            self.ask_volume_2_arr[:-1] = self.ask_volume_2_arr[1:]
            self.ask_volume_3_arr[:-1] = self.ask_volume_3_arr[1:]
            self.ask_volume_4_arr[:-1] = self.ask_volume_4_arr[1:]
            self.ask_volume_5_arr[:-1] = self.ask_volume_5_arr[1:]

            self.bid_price_2_arr[-1] = tick.bid_price_2
            self.bid_price_3_arr[-1] = tick.bid_price_3
            self.bid_price_4_arr[-1] = tick.bid_price_4
            self.bid_price_5_arr[-1] = tick.bid_price_5
            self.ask_price_2_arr[-1] = tick.ask_price_2
            self.ask_price_3_arr[-1] = tick.ask_price_3
            self.ask_price_4_arr[-1] = tick.ask_price_4
            self.ask_price_5_arr[-1] = tick.ask_price_5
            self.bid_volume_2_arr[-1] = tick.bid_volume_2
            self.bid_volume_3_arr[-1] = tick.bid_volume_3
            self.bid_volume_4_arr[-1] = tick.bid_volume_4
            self.bid_volume_5_arr[-1] = tick.bid_volume_5
            self.ask_volume_2_arr[-1] = tick.ask_volume_2
            self.ask_volume_3_arr[-1] = tick.ask_volume_3
            self.ask_volume_4_arr[-1] = tick.ask_volume_4
            self.ask_volume_5_arr[-1] = tick.ask_volume_5

        self.count += 1

        if (not self.inited) and (self.count >= self.size):
            self.inited = True

    @property
    def ticktime(self):
        """"""
        return self.ticktime_arr

    @property
    def timestamp(self):
        return self.timestamp_arr

    @property
    def last_price(self):
        return self.last_price_arr

    @property
    def bid_1(self):
        return self.bid_price_1_arr

    @property
    def bid_2(self):
        return self.bid_price_2_arr

    @property
    def bid_3(self):
        return self.bid_price_3_arr

    @property
    def bid_4(self):
        return self.bid_price_4_arr

    @property
    def bid_5(self):
        return self.bid_price_5_arr

    @property
    def ask_1(self):
        return self.ask_price_1_arr

    @property
    def ask_2(self):
        return self.ask_price_2_arr

    @property
    def ask_3(self):
        return self.ask_price_3_arr

    @property
    def ask_4(self):
        return self.ask_price_4_arr

    @property
    def ask_5(self):
        return self.ask_price_5_arr

    @property
    def bid_volume_1(self):
        return self.bid_volume_1_arr

    @property
    def bid_volume_2(self):
        return self.bid_volume_2_arr

    @property
    def bid_volume_3(self):
        return self.bid_volume_3_arr

    @property
    def bid_volume_4(self):
        return self.bid_volume_4_arr

    @property
    def bid_volume_5(self):
        return self.bid_volume_5_arr

    @property
    def ask_volume_1(self):
        return self.ask_volume_1_arr

    @property
    def ask_volume_2(self):
        return self.ask_volume_2_arr

    @property
    def ask_volume_3(self):
        return self.ask_volume_3_arr

    @property
    def ask_volume_4(self):
        return self.ask_volume_4_arr

    @property
    def ask_volume_5(self):
        return self.ask_volume_5_arr

    @property
    def open(self):
        return self.open_price_arr

    @property
    def high(self):
        return self.high_price_arr

    @property
    def low(self):
        return self.low_price_arr

    @property
    def volume(self):
        return self.volume_arr

    @property
    def amount(self):
        return self.amount_arr

    @property
    def openinterest(self):
        return self.open_price_arr

    @property
    def settle(self):
        return self.average_price_arr


class BarArrayManager:
    """
    Time series container of bar data, could be used to calculate technical indicators
    """
    def __init__(self, size: int = 1500):
        """Constructor"""
        self.count: int = 0
        self.size: int = size
        self.inited: bool = False

        self.bartime_arr: np.array = np.array([datetime(1990, 5, 22)] * self.size, dtype=datetime)
        self.timestamp_arr: np.array = np.full(self.size, np.nan)
        self.tickers: np.array = np.empty(self.size, dtype=object)
        self.open_arr: np.array = np.full(self.size, np.nan)
        self.high_arr: np.array = np.full(self.size, np.nan)
        self.low_arr: np.array = np.full(self.size, np.nan)
        self.close_arr: np.array = np.full(self.size, np.nan)
        self.prev_close_arr: np.array = np.full(self.size, np.nan)
        self.close_ret_arr: np.array = np.full(self.size, np.nan)
        self.settle_arr: np.array = np.full(self.size, np.nan)
        self.prev_settle_arr: np.array = np.full(self.size, np.nan)
        self.settle_ret_arr: np.array = np.full(self.size, np.nan)
        self.volume_arr: np.array = np.full(self.size, np.nan)
        self.amount_arr: np.array = np.full(self.size, np.nan)
        self.open_interest_arr: np.array = np.full(self.size, np.nan)
        self.coef_adj_arr: np.array = np.full(self.size, np.nan)
        self.open_adj_arr: np.array = np.full(self.size, np.nan)
        self.close_adj_arr: np.array = np.full(self.size, np.nan)
        self.high_adj_arr: np.array = np.full(self.size, np.nan)
        self.low_adj_arr: np.array = np.full(self.size, np.nan)

    def update_bar(self,
                   bar: BarData,
                   coef_adj: float) -> None:
        """Update new bar data into array manager"""
        if bar.bartime <= self.bartime_arr[-1]:
            return

        self.bartime_arr[:-1] = self.bartime_arr[1:]
        self.timestamp_arr[:-1] = self.timestamp_arr[1:]
        self.tickers[:-1] = self.tickers[1:]
        self.open_arr[:-1] = self.open_arr[1:]
        self.high_arr[:-1] = self.high_arr[1:]
        self.low_arr[:-1] = self.low_arr[1:]
        self.close_arr[:-1] = self.close_arr[1:]
        self.close_ret_arr[:-1] = self.close_ret_arr[1:]
        self.prev_close_arr[:-1] = self.prev_close_arr[1:]
        self.settle_arr[:-1] = self.settle_arr[1:]
        self.settle_ret_arr[:-1] = self.settle_ret_arr[1:]
        self.prev_settle_arr[:-1] = self.prev_settle_arr[1:]
        self.volume_arr[:-1] = self.volume_arr[1:]
        self.amount_arr[:-1] = self.amount_arr[1:]
        self.open_interest_arr[:-1] = self.open_interest_arr[1:]
        self.coef_adj_arr[:-1] = self.coef_adj_arr[1:]
        self.open_adj_arr[:-1] = self.open_adj_arr[1:]
        self.close_adj_arr[:-1] = self.close_adj_arr[1:]
        self.high_adj_arr[:-1] = self.high_adj_arr[1:]
        self.low_adj_arr[:-1] = self.low_adj_arr[1:]

        t = bar.bartime
        self.bartime_arr[-1] = t
        self.timestamp_arr[-1] = (np.datetime64(t) - np.datetime64('1970-01-01T00:00:00')) / np.timedelta64(1, "s")
        self.tickers[-1] = bar.ticker
        self.open_arr[-1] = bar.open
        self.high_arr[-1] = bar.high
        self.low_arr[-1] = bar.low
        self.close_arr[-1] = bar.close
        self.prev_close_arr[-1] = bar.prevclose
        self.close_ret_arr[-1] = bar.closeret
        self.settle_arr[-1] = bar.settle
        self.prev_settle_arr[-1] = bar.prevsettle
        self.settle_ret_arr[-1] = bar.settleret
        self.volume_arr[-1] = bar.volume
        self.amount_arr[-1] = bar.amount
        self.open_interest_arr[-1] = bar.openinterest

        # Check coefadj
        if coef_adj != 1 and self.tickers[-2] == self.tickers[-1]:
            coef_adj = 1

        adj = 1 if np.isnan(self.coef_adj_arr[-2]) else self.coef_adj_arr[-2] * coef_adj

        self.coef_adj_arr[-1] = round(adj, 4)
        self.open_adj_arr[-1] = round(bar.open * adj, 2)
        self.high_adj_arr[-1] = round(bar.high * adj, 2)
        self.low_adj_arr[-1] = round(bar.low * adj, 2)
        self.close_adj_arr[-1] = round(bar.close * adj, 2)

        self.count += 1

        if (not self.inited) and (self.count >= self.size):
            self.inited = True

    @property
    def bartime(self) -> np.array:
        """Get bartime list time series"""
        return self.bartime_arr

    @property
    def bartimestamp(self) -> np.array:
        """Get timestamp list of bartime"""
        return self.timestamp_arr

    @property
    def ticker(self) -> np.array:
        return self.tickers

    @property
    def open(self) -> np.array:
        """Get open price time series"""
        return self.open_arr

    @property
    def high(self) -> np.array:
        """Get high price time series"""
        return self.high_arr

    @property
    def low(self) -> np.array:
        """Get low price time series"""
        return self.low_arr

    @property
    def close(self) -> np.array:
        """Get close price time series"""
        return self.close_arr

    @property
    def prevclose(self) -> np.array:
        """Get previous close price time series"""
        return self.prev_close_arr

    @property
    def settle(self) -> np.array:
        """Get previous close price time series"""
        return self.settle_arr

    @property
    def prevsettle(self) -> np.array:
        """Get previous close price time series"""
        return self.prev_settle_arr

    @property
    def settleret(self) -> np.array:
        """Get previous close price time series"""
        return self.settle_ret_arr

    @property
    def closeret(self) -> np.array:
        """Get close return time series"""
        return self.close_ret_arr

    @property
    def volume(self) -> np.array:
        """Get volume time series"""
        return self.volume_arr

    @property
    def amount(self) -> np.array:
        """Get amount time series"""
        return self.amount_arr

    @property
    def openinterest(self) -> np.array:
        """Get open interest time series"""
        return self.open_interest_arr

    @property
    def coefadj(self) -> np.array:
        """Get adjustment coefficients time series"""
        return self.coef_adj_arr

    @property
    def openadj(self) -> np.array:
        """Get adjustment open price time series"""
        return self.open_adj_arr

    @property
    def closeadj(self) -> np.array:
        """Get adjustment open price time series"""
        return self.close_adj_arr

    @property
    def highadj(self) -> np.array:
        """Get adjustment open price time series"""
        return self.high_adj_arr

    @property
    def lowadj(self) -> np.array:
        """Get adjustment open price time series"""
        return self.low_adj_arr


@njit
def adjust_econ_data(index_data: np.ndarray,
                     seasonality: int,
                     pct_app: int) -> float:
    """处理经济数据"""
    if np.isnan(index_data[-1]):
        return np.nan
    d = index_data[-1]
    # 季节性调整
    if seasonality:
        d = season_adj(index_data, pct_app)
    return d


@njit
def season_adj(index_data: np.ndarray, pct_app: int):
    """Adjust seasonality with YoY pct / diff"""
    if index_data.shape[0] < 365:
        print("指标数据长度不足，无法进行季节性调整")
        return np.nan
    div = np.nanmean(index_data[-395:-365])
    if np.isnan(div):
        return np.nan
    if pct_app:
        d = index_data[-1] / div - 1 if div != 0 else np.nan
    else:
        d = index_data[-1] - div
    return d


class EdbArrayManager:
    """
    Time series manager of EDB data with index ID as key, calendar datetime as union dt
    """
    def __init__(self, size: int = 1000):
        """Constructor"""
        self.size: int = size

        self.dt_arr: dict = {}                          # Calendar dates of EDB data
        self.index_arr: dict = {}                       # EDB data recorder
        self.index_adj: dict = {}                       # 属性调整后的经济数据

        self.count: dict = {}                           # 每个index数据的长度

        self.factor_info: dict = {}                     # Information of each product factor
        self.index_info: dict = {}                      # Information of each index
        self.index_factors: dict = {}                   # 指标对应的因子 {b1: [(RB, Factor1), (I, Factor20)]}

        self.update: list = []                          # Last updated index

    def set_info(self, **kwargs):
        """Initialize EDB array manager"""
        self.index_info = kwargs["index_info"]
        factor_info = kwargs["factor_info"]
        for (product, factors) in factor_info.items():
            for (factor, info_) in factors.items():
                factor_tag = (product, factor)
                self.factor_info[factor_tag] = info_
                index_id = info_.index_id
                self.index_factors.setdefault(index_id, [])
                self.index_factors[index_id].append(factor_tag)

    def update_data(self, data: dict) -> None:
        """Update EDB data into array manager"""
        dt = data["DateTime"]
        data.pop("DateTime")

        for (index_id, d) in data.items():
            if index_id not in self.index_arr:
                self.index_arr[index_id] = np.full(self.size, np.nan)
                self.index_adj[index_id] = np.full(self.size, np.nan)
                self.dt_arr[index_id] = np.full(self.size, datetime(1990, 5, 22), dtype=datetime)
                self.count[index_id] = 0

            self.dt_arr[index_id][:-1] = self.dt_arr[index_id][1:]
            self.dt_arr[index_id][-1] = dt

            self.index_arr[index_id][:-1] = self.index_arr[index_id][1:]
            self.index_arr[index_id][-1] = d

            # 调整数据
            info: EdbInfo = self.index_info[index_id]
            seasonality = info.seasonality
            pct_app = info.applicable_pct

            self.index_adj[index_id][:-1] = self.index_adj[index_id][1:]
            self.index_adj[index_id][-1] = adjust_econ_data(self.index_arr[index_id], seasonality, pct_app)

            if self.count[index_id] > 0 or np.isfinite(d):
                self.count[index_id] += 1

        self.update = [i for (k, v) in data.items() for i in self.index_factors[k] if np.isfinite(v)]

    @property
    def index_data(self):
        """Return all EDB data arrays"""
        return self.index_arr

    @property
    def index_data_adj(self):
        """"""
        return self.index_adj

    @property
    def index_information(self):
        """Return information of index IDs"""
        return self.index_info

    @property
    def update_dt(self):
        """Return calendar datetime of EDB array"""
        return self.dt_arr

    @property
    def factor_information(self):
        """Return information of product factors"""
        return self.factor_info

    def get_factor_info(self, product: str, factor: str):
        """"""
        return self.factor_info[(product, factor)]

    def get_factor_data(self, product: str, factor: str) -> np.ndarray:
        """获取指定品种因子的历史数据"""
        index_id = self.factor_info[(product, factor)].index_id
        return self.index_data[index_id]

    def get_factor_data_adj(self, product: str, factor: str) -> np.ndarray:
        """获取指定品种因子的历史数据"""
        index_id = self.factor_info[(product, factor)].index_id
        return self.index_adj[index_id]

    def get_factor_time(self, product: str, factor: str) -> np.ndarray:
        """获取品种因子数据的时间戳"""
        index_id = self.factor_info[(product, factor)].index_id
        return self.update_dt[index_id]

    def is_update(self, product: str, factor: str) -> bool:
        """返回当前品种因子是否有数据更新"""
        return (product, factor) in self.update

    def factor_count(self, product: str, factor: str) -> int:
        """返回当前品种因子的数据更新"""
        index_id = self.factor_info[(product, factor)].index_id
        return self.count[index_id]

    def get_index_info(self, index_id: str):
        """"""
        return self.index_info[index_id]

    def is_inited(self, product: str, factor: str, size: int) -> bool:
        """返回品种因子数据是否已完成初始化"""
        index_id = self.factor_info[(product, factor)].index_id
        count = self.count[index_id]
        return count >= size
