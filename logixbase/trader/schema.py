from dataclasses import field, dataclass
from typing import Any

from pydantic import BaseModel
from datetime import datetime

from .utils import ticker_to_instrument, instrument_to_ticker
from .constant import (OptionType, Asset, Exchange, Interval, OrderType, Direction, Offset, Status, StopOrderStatus,
                       OptionExecType)



ACTIVE_STATUSES = {Status.SUBMITTING,
                   Status.NOTTRADED,
                   Status.PARTTRADED}


class FutureInfo(BaseModel):
    """Contains basic information about each contract traded"""
    asset: Asset
    instrument: str
    product: str
    exchange: Exchange

    multiplier: float
    pricetick: float
    margin: float                               # 最低保证金率
    listdate: int = None                        # 合约上市日
    delistdate: int = None                       # 合约退市日
    deliverdate: int = None                      # 合约交割日

    ticker: str = ""
    minmarketordervolume: float = 1             # 最小交易单位（手）
    maxmarketordervolume: float = 0
    minlimitordervolume: float = 1
    maxlimitordervolume: float = 0

    pricelimit: float = 1                       # 涨跌停比例

    classlevel1: str = ""                      # 所属板块
    classlevel2: str = ""                      # 所属细分板块

    comm_open_fix: float = 0                        # 固定开仓手续费
    comm_open_pct: float = 0.0003                       # 浮动开仓手续费
    comm_close_fix: float = 0                       # 固定平仓手续费
    comm_close_pct: float = 0.0003                       # 固定平仓手续费
    comm_closetoday_fix: float = 0                  # 平今固定平仓手续费
    comm_closetoday_pct: float = 0                 # 平今浮动平仓手续费

    def model_post_init(self, __context: Any) -> None:
        dl_day = self.delistdate or self.deliverdate
        if not self.ticker and dl_day and int(len(str(dl_day))) >= 4:
            self.ticker = instrument_to_ticker(self.asset.value, self.exchange.value,
                                               self.instrument, str(self.delistdate)[:4])


class OptionInfo(BaseModel):
    instrument: str
    exchange: Exchange                          # 交易所
    asset: Asset                                # 资产类别
    product: str                                # 品种
    underlying: str                             # 底层合约
    underlyingclass: str = ""                  # 底层合约类别

    multiplier: float                           # 合约乘数
    underlyingmultiplier: float
    pricetick: float                            # 合约跳价单位

    exercisetype: OptionExecType = "european"                          # 行权类型
    strike: float                               # 行权价格
    optiontype: OptionType                            # 期权类型

    listdate: int = None                          # 合约上市日
    delistdate: int = None                       # 期权到期日
    deliverdate: int = None                         # 行权日

    ticker: str = ""                                # 标准合约代码
    comm_open_fix: float = 0                        # 固定开仓手续费
    comm_open_pct: float = 0.0003                        # 浮动开仓手续费
    comm_close_fix: float = 0                       # 固定平仓手续费
    comm_close_pct: float = 0.0003                      # 固定平仓手续费
    comm_closetoday_fix: float = 0.                  # 平今固定平仓手续费
    comm_closetoday_pct: float = 0.                # 平今浮动平仓手续费

    def model_post_init(self, __context: Any) -> None:
        dl_day = self.delistdate or self.deliverdate
        if not self.ticker and dl_day and int(len(str(dl_day))) >= 4:
            self.ticker = instrument_to_ticker(self.asset.value, self.exchange.value,
                                               self.instrument, str(self.delistdate)[:4])


class StockInfo(BaseModel):
    instrument: str                             # 交易所代码
    exchange: Exchange                          # 交易所
    asset: Asset                                # 资产类别

    ticker: str = ""                            # 标准合约代码
    name: str = ""                              # 股票名
    listdate: int                          # 股票挂牌日
    status: str = ""                            # 股票当前状态
    establishdate: int                     # 公司成立日

    multiplier: float = 100                     # 股票乘数
    pricetick: float = 0.01                      # 股票跳价单位
    allshare: float = 0.0                       # 总股本
    flowshare: float = 0.0                      # 流通股

    sw_class_level1: str = ""                   # 申万一级行业分类
    sw_class_level2: str = ""                   # 申万二级行业分类
    sw_class_level3: str = ""                   # 申万三级行业分类

    comm_open_fix: float = 0                        # 固定开仓手续费
    comm_open_pct: float = 0.00015                    # 浮动开仓手续费
    comm_close_fix: float = 0                        # 固定平仓手续费
    comm_close_pct: float = 0.00015                      # 固定平仓手续费
    comm_closetoday_fix: float = 0.00015                  # 平今固定平仓手续费
    comm_closetoday_pct: float = 0.00015                  # 平今浮动平仓手续费

    def model_post_init(self, __context: Any) -> None:
        if not self.ticker:
            self.ticker = instrument_to_ticker(self.asset.value, self.exchange.value, self.instrument)


class IndexInfo(BaseModel):
    instrument: str                             # 交易所代码
    exchange: Exchange                          # 交易所
    asset: Asset                                # 资产类别
    name: str                                   # 指数名
    listdate: int                          # 指数挂牌日
    beginpoint: float                           # 指数起点
    samplesize: float                           # 样本大小

    ticker: str = ""                            # 标准合约代码
    class_level1: str = ""                      # 一级指数分类
    class_level2: str = ""                      # 二级指数分类

    def model_post_init(self, __context: Any) -> None:
        if not self.ticker:
            self.ticker = instrument_to_ticker(self.asset.value, self.exchange.value, self.instrument)


class EtfInfo(BaseModel):
    instrument: str                             # 交易所代码
    exchange: Exchange                          # 交易所
    asset: Asset                                # 资产类别
    name: str                                   # ETF名
    listdate: int                          # ETF挂牌日
    style: str                                # ETF类型
    allshares: float                            # 总份额
    benchmark: str                              # 基准指数

    ticker: str = ""                            # 标准合约代码
    pricetick: float = 0.01
    multiplier: float  = 1

    comm_open_fix: float = 0                        # 固定开仓手续费
    comm_open_pct: float = 0.0003                    # 浮动开仓手续费
    comm_close_fix: float = 0                        # 固定平仓手续费
    comm_close_pct: float = 0.0003                      # 固定平仓手续费
    comm_closetoday_fix: float = 0.0003                  # 平今固定平仓手续费
    comm_closetoday_pct: float = 0.0003                  # 平今浮动平仓手续费

    def model_post_init(self, __context: Any) -> None:
        if not self.ticker:
            self.ticker = instrument_to_ticker(self.asset.value, self.exchange.value, self.instrument)


class QmFactorInfo(BaseModel):
    contract: str
    factor: str
    factor_id: int
    index_id: str
    dimension: str
    correlation: int
    description: str
    note: str = ""


class EdbInfo(BaseModel):
    index_id: str
    industry: str
    description: str
    source: str
    ticker: str
    frequency: str
    timetag: str
    publishlag: int
    applicable_pct: bool
    quote_unit: str = ""
    note: str = ""
    table: str = ""


@dataclass
class BarData:
    """Candlestick bar data of a certain trading period"""
    asset: Asset
    ticker: str
    bartime: datetime
    tradeday: int
    interval: Interval

    product: str = ""
    instrument: str = None
    exchange: Exchange = None
    open: float = 0
    high: float = 0
    low: float = 0
    close: float = 0
    prevclose: float = 0
    settle: float = 0
    prevsettle: float = 0
    volume: float = 0
    amount: float = 0
    openinterest: float = 0

    closeret: float = 0
    settleret: float = 0

    coefadj: float = 1.

    def __post_init__(self):
        """"""
        if not self.instrument:
            self.instrument = ticker_to_instrument(self.ticker)


class TickData(BaseModel):
    """
    Tick data contains information about:
        * last trade in market
        * orderbook snapshot
        * intraday market statistics.
    """
    instrument: str
    ticktime: datetime
    tradeday: int
    actionday: int
    exchange: Exchange

    product: str
    lastprice: float = 0
    bid1: float = 0
    bid2: float = 0
    bid3: float = 0
    bid4: float = 0
    bid5: float = 0
    ask1: float = 0
    ask2: float = 0
    ask3: float = 0
    ask4: float = 0
    ask5: float = 0

    bidvolume1: float = 0
    bidvolume2: float = 0
    bidvolume3: float = 0
    bidvolume4: float = 0
    bidvolume5: float = 0
    askvolume1: float = 0
    askvolume2: float = 0
    askvolume3: float = 0
    askvolume4: float = 0
    askvolume5: float = 0

    limitup: float = 0                         # 涨停板价格
    limitdown: float = 0                       # 跌停板价格

    open: float = 0                             # 当前交易日开盘价
    high: float = 0                             # 当前交易日最高价
    low: float = 0                              # 当前交易日最低价
    volume: float = 0                           # 当前交易日累计成交量
    amount: float = 0                           # 当前交易日累计成交金额
    openinterest: float = 0                    # 当前持仓量
    settle: float = 0                           # 当前交易日累计平均价格

    prevclose: float = 0                       # 前一交易日收盘价
    prevsettle: float = 0                      # 前一交易日结算价
    prevopeninterest: float = 0               # 前一交易日持仓量

    coefadj: float = 1.
    ticker: str = ""                           # 系统通用标准合约代码： 交易所.品种.合约


class AccountInfo(BaseModel):
    """
    账户信息类
    """
    broker_id: str
    account_id: str
    prev_mortgage: float
    prev_credit: float
    prev_deposit: float
    prev_balance: float
    prev_margin: float
    interestbase: float
    interest: float
    deposit: float
    withdraw: str
    frozen_margin: float
    frozen_cash: float
    frozen_commission: float
    current_margin: float
    cashin: float
    commission: float
    close_profit: float
    position_profit: float
    balance: float
    available: str
    withdrawquota: float
    reserve: float
    tradeday: datetime
    settlement_id: str
    credit: float
    mortgage: float
    exchange_margin: float
    delivery_margin: float
    exchange_delivery_margin: float
    reserve_balance: float
    currency_id: str
    prefund_mortgagein: float
    prefund_mortgageout: float
    fund_mortgage_in: float
    fund_mortgage_out: float
    fund_mortgage_available: float
    mortgageable_fund: float
    specproduct_margin: float
    specproduct_frozenmargin: float
    specproduct_commission: float
    specproduct_frozencommission: float
    specproduct_positionprofit: float
    specproduct_closeprofit: float
    specproduct_positionprofitbyalg: float
    specproduct_exchangemargin: float
    biztype: int
    frozen_swap: float
    remain_swap: float


class AccountPosition(BaseModel):
    broker_id: str
    account_id: str
    hold_direction: str
    hedge_flag: str
    hold_date: datetime
    prev_volume: int
    volume: int
    long_frozen: int
    short_frozen: int
    long_frozen_amount: float
    short_frozen_amount: float
    open_volume: int
    close_volume: int
    open_amount: float
    close_amount: float
    hold_cost: float
    prev_margin: float
    use_margin: float
    frozen_margin: float
    frozen_cash: float
    frozen_commission: float
    cashin: float
    commission: float
    close_profit: float
    hold_profit: float
    prev_settle_price: float
    settle_price: float
    tradeday: datetime
    settlement_id: str
    open_cost: float
    exchange_margin: float
    comb_position: int
    comb_long_frozen: int
    comb_short_frozen: int
    close_profit_bydate: float
    close_profit_bytrade: float
    today_hold: int
    marginrate_bymoney: float
    marginrate_byvolume: float
    strike_frozen: float
    strike_frozen_amount: float
    abandon_frozen: float
    exchange_id: str
    prev_strike_frozen: float
    invest_unit_id: int
    position_cost_offset: float
    tas_position: int
    tas_position_cost: float
    instrument_id: str


class OrderData(BaseModel):
    """Contains information for tracking lastest status of a specific order"""
    asset: str
    order_id: int
    product: str
    ticker: str
    instrument: str = None
    ordertime: datetime
    exchange: Exchange

    order_type: OrderType
    direction: Direction
    offset: Offset
    status: Status = Status.SUBMITTING

    price: float = 0
    volume: float = 0
    traded: float = 0

    def model_post_init(self, _context = None):
        """"""
        if self.instrument is None:
            self.instrument = ticker_to_instrument(self.ticker)

    def is_active(self) -> bool:
        """Check if the order is active"""
        if self.status in ACTIVE_STATUSES:
            return True
        else:
            return False


class StopOrder(BaseModel):
    asset: str
    order_id: int
    product: str
    ticker: str
    instrument: str = None
    ordertime: datetime
    exchange: Exchange

    order_type: OrderType
    fill_order_type: OrderType
    direction: Direction
    offset: Offset
    status: StopOrderStatus = StopOrderStatus.WAITING

    price: float = 0
    volume: float = 0
    lock: bool = False
    fill_order_id: list = field(default_factory=list)

    def model_post_init(self, _context = None):
        """"""
        if not self.instrument:
            self.instrument = ticker_to_instrument(self.ticker)


class TradeData(BaseModel):
    """
    Contains information of a fill of an order, One order can have several trade fills
    """
    asset: str
    order_id: int
    trade_id: int
    product: str
    ticker: str
    instrument: str = None
    filltime: datetime
    exchange: Exchange
    direction: Direction

    offset: Offset
    price: float = 0
    volume: float = 0

    def model_post_init(self, _context = None):
        """"""
        if not self.instrument:
            self.instrument = ticker_to_instrument(self.ticker)
