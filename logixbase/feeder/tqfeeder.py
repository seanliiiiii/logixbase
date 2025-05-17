import traceback
import time
from datetime import datetime, timedelta
import warnings
import pandas as pd
from typing import Union
from collections import defaultdict
warnings.filterwarnings("ignore")


from ..utils import unify_time, progressor, all_tradeday, silence_asyncio_warning
from ..trader import instrument_to_ticker, instrument_to_product, INSTRUMENT_FORMAT, Interval


from .schema import TQSDKConfig
from .base import BaseFeeder


class TqsdkFeeder(BaseFeeder):
    """
    天勤量化API接口
    https://doc.shinnytech.com/tqsdk/latest/quickstart.html#section-4
    get_kline_serial， 单次查询合约上限48个
    """
    def __init__(self, conn_cfg: TQSDKConfig, logger=None):
        if isinstance(conn_cfg, dict):
            conn_cfg = TQSDKConfig(**conn_cfg)
        super().__init__("TQSDK", conn_cfg, logger)

        self._api = None

        self.last_tick: dict = {}
        self.day_bar: dict = {}

    @silence_asyncio_warning
    def connect(self):
        """Start api"""
        try:
            from tqsdk import TqApi, TqAuth
            self._api = TqApi(auth=TqAuth(self._conn_cfg.username, self._conn_cfg.password), disable_print=True)
            self.INFO(f"天勤API连接成功: 用户{self._conn_cfg.username}")
        except Exception as e:
            self.ERROR(f"天勤API连接失败: {e}")

    @silence_asyncio_warning
    def disconnect(self):
        """Stop api"""
        self._api.close()

    @staticmethod
    def _parse_future_ticker(ticker: Union[list, tuple]):
        """将标准合约代码解析为天勤合约代码"""
        instrument = []
        products = []
        for k in ticker:
            parts = k.split(".")
            if int(len(parts)) < 3:
                products.append(k)
                continue
            if not parts[-1].isdigit() or int(len(parts[-1])) < 3:
                products.append(k)
                continue
            exchange = parts[0].upper()
            if exchange in INSTRUMENT_FORMAT:
                fmt, digits = INSTRUMENT_FORMAT[exchange]
                product = eval(f"parts[1].{fmt}()")
                contract = eval(f"parts[2][-{digits}:]")
                instrument.append(exchange + "." + product + contract)
            else:
                instrument.append(k)
        return products, instrument

    def _parse_interval(self, start: Union[datetime, str, int], end: Union[datetime, str, int], interval: str):
        # 解析interval
        interval = Interval(interval)
        if interval.pattern == "day":
            window = interval.window * 24 * 3600
        elif interval.pattern == "minute":
            window = interval.window * 60
        elif interval.pattern == "second":
            window = interval.window
        else:
            self.ERROR(f"[TQSDK] [ERROR] 暂不支持输入的行情周期：{interval.value}")
            return
        # 解析天勤的size参数
        start_ = unify_time(start, fmt="str", mode=3, dot="")
        end_ = unify_time(end, fmt="str", mode=3, dot="")

        trade_days = all_tradeday(start_, end_)
        if interval.pattern == "day":
            size = int(len(trade_days)) + 15
        else:
            size = min(8000, int(60 * 8 * (int(len(trade_days)) + 5)))
        return window, size

    @silence_asyncio_warning
    @progressor
    def _query_quote_future(self, window: int, size: int, instrument: str) -> pd.DataFrame:
        """"""
        # Query data
        d = self._api.get_kline_serial(instrument, window, size)
        # Format data
        d.columns = d.columns.str.capitalize()
        d = d[d["Datetime"] != 0].rename(columns={"Datetime": "DateTime", "Close_oi": "OpenInterest",
                                                  "Symbol": "Instrument"})
        d["DateTime"] = pd.to_datetime(d["DateTime"]) + timedelta(hours=8)
        d["PrevClose"] = d["Close"].shift(1)
        d["Exchange"] = d["Instrument"].map(lambda x: x.split(".")[0])
        d['Instrument'] = d['Instrument'].map(lambda x: x.split(".")[1])
        time.sleep(5)
        return d

    @silence_asyncio_warning
    def asset_ticker(self, asset: str, active: bool = False, offshore: bool = False):
        """获取所有合约代码（交易所合约代码）"""
        # Get TQ instruments
        tq_instruments = self.all_tq_instrument(asset, not active)
        # Filter offshore instruments
        if not offshore:
            tq_instruments = [k for k in tq_instruments if k.split(".")[0].upper() != "KQD"]
        # Delete exchange
        instruments = sorted([k.split(".")[-1] for k in tq_instruments])
        return instruments

    @silence_asyncio_warning
    def all_tq_instrument(self, asset: str, active: bool = False, offshore: bool = False):
        """Get all TQ instruments ID list： FUTURE / OPTION / COMBINE"""
        instruments = self._api.query_quotes(ins_class=asset.upper(), expired=False)
        if not active:
            instruments.extend(self._api.query_quotes(ins_class=asset.upper(), expired=True))
        if not offshore:
            instruments = [k for k in instruments if k.split(".")[0].upper() != "KQD"]
        return sorted(instruments)

    @silence_asyncio_warning
    def get_last_tick(self, instrument: list):
        """Get last tick of each instrument"""
        for instrument_ in instrument:
            if instrument_ not in self.last_tick:
                try:
                    q = self._api.get_quote(instrument_)
                    self.last_tick[instrument_] = q
                except Exception as e:
                    tb = traceback.format_exc()
                    self.INFO(f"[TQSDK] 最新Tick数据读取获取失败: {e}  /n {tb}")

    @silence_asyncio_warning
    def asset_info(self, asset: str, info: str = "basic", ticker: list = None):
        asset = asset.lower()
        info = info.lower()

        if info == "basic":
            if asset in ["future"]:
                info_data = eval(f"self.{asset}_info_basic")(ticker)
            else:
                self.ERROR("资产类型无法识别：仅支持future（期货）")
                return
        elif info == "trade":
            if asset == "future":
                info_data = eval(f"self.{asset}_info_trade")(ticker)
            else:
                self.ERROR("交易信息仅适用于期货")
                return
        else:
            self.ERROR("信息类型无法识别：仅支持basic（基础合约信息）/trade（合约交易信息）")
            return
        self.INFO(f"{asset.capitalize()}资产{info.capitalize()}信息获取完成: {int(len(info_data))}条记录")

        return info_data

    @silence_asyncio_warning
    def future_info_basic(self, ticker: list = None):
        """Get all basic information of domestic future contracts in history"""
        cols = ["Ticker", "Product", "Instrument", "DelistDate", "DeliverDate", "Multiplier", "PriceTick",
                "Exchange", "MinMargin", "PriceLimit", "Asset", "ClassLevel2"]
        # 获取目标的天勤合约代码列表
        if ticker:
            products, instruments = self._parse_future_ticker(ticker)
            if products:
                instruments_all = self.all_tq_instrument("Future", True, False)
                instruments_all.extend(self.all_tq_instrument("Future", False, False))
                products = [".".join(k.split(".")[:2]) for k in products]
                instruments_ = [i for k in products for i in instruments_all if i.startswith(k) and i.lstrip(k).isdigit()]
                instruments.extend(instruments_)
        else:
            instruments = self.all_tq_instrument("Future", True, False)
            instruments.extend(self.all_tq_instrument("Future", False, False))

        # 获取每个合约的最后tick
        self.get_last_tick(instruments)
        info = []
        for instrument in instruments:
            q = self.last_tick.get(instrument, None)
            if q is None:
                self.INFO(f"[TQSDK] 期货基础信息获取失败: {instrument}")
                continue
            # Parse information
            exchange, symbol = instrument.split(".")
            ticker_ = instrument_to_ticker("future", exchange, symbol, q.delivery_year)
            product = q.product_id.upper()
            # Parse key date
            delist_date = datetime.fromtimestamp(q.expire_datetime)
            delist_date = delist_date.replace(hour=0, minute=0, second=0, microsecond=0)
            deliver_date = datetime(q.delivery_year, q.delivery_month, delist_date.day)
            if not ((exchange in ["CFFEX"]) and (not product.startswith("T"))):
                deliver_date += timedelta(days=5)
            # Parse instrument setting
            margin = round(q.margin / (q.pre_settlement * q.volume_multiple), 2)
            limit = round(q.upper_limit / q.pre_settlement - 1, 2)
            info.append([ticker_, product, instrument.split(".")[1], delist_date, deliver_date,
                         q.volume_multiple, q.price_tick, exchange, margin, limit, q.ins_class,
                         q.categories[0]['id'] if q.categories else ""])
        return pd.DataFrame(info, columns=cols)

    @silence_asyncio_warning
    def future_info_trade(self, ticker: list = None):
        """Get trade information"""
        cols = ["Ticker", "Margin", "Commission_Pct", "Commission_Fix", "MaxLimitOrderVolume","MaxMarketOrderVolume",
                "MinLimitOrderVolume","MinMarketOrderVolume"]
        # 获取目标的天勤合约代码列表
        if ticker:
            products, instruments = self._parse_future_ticker(ticker)
            if products:
                instruments_all = self.all_tq_instrument("Future", True, False)
                instruments_all.extend(self.all_tq_instrument("Future", False, False))
                products = [".".join(k.split(".")[:2]) for k in products]
                instruments_ = [i for k in products for i in instruments_all if i.startswith(k) and i.lstrip(k).isdigit()]
                instruments.extend(instruments_)
        else:
            instruments = self.all_tq_instrument("Future", True, False)
            instruments.extend(self.all_tq_instrument("Future", False, False))
        # Get last tick of each instrument
        self.get_last_tick(instruments)
        info = []
        for instrument in instruments:
            q = self.last_tick.get(instrument, None)
            if q is None:
                self.INFO(f"[TQSDK] 期货基础信息获取失败: {instrument}")
                continue
            # Parse data
            exchange, symbol = instrument.split(".")
            ticker_ = instrument_to_ticker("future", exchange, symbol, q.delivery_year)
            amount = (q.pre_settlement * q.volume_multiple)
            margin = q.margin / amount
            # Check if fix commission or pct commission
            comm_decimal = int(str(q.commission).split(".")[-1])
            if comm_decimal == 0:
                comm_fix = q.commission
                comm_pct = 0
            else:
                comm_pct = q.commission / amount
                comm_fix = 0
            # Prepare data
            info_ = [ticker_, margin, comm_pct, comm_fix, q.max_limit_order_volume,
                     q.max_market_order_volume, q.min_limit_order_volume, q.min_market_order_volume]
            info.append(info_)
        trade_info = pd.DataFrame(info, columns=cols)
        return trade_info

    def asset_quote(self, asset: str, start: Union[datetime, str, int], end: Union[datetime, str, int],
                    interval: Union[str, int], ticker: Union[list, tuple] = None, underlying: Union[list, tuple] = None):
        asset = asset.lower()
        try:
            interval = Interval(interval)
        except ValueError:
            self.ERROR(f"行情周期无法识别: {interval}")
            return

        quote = eval(f"self.{asset}_quote(start, end, interval.value, ticker)")
        self.INFO(f"{asset.capitalize()}资产{interval.value}行情数据获取完成: {int(len(quote))}条记录")
        quote = quote.drop_duplicates(subset=["DateTime", "Ticker"])
        return quote

    def future_quote(self, start: Union[datetime, str, int], end: Union[datetime, str, int], interval: str,
                     ticker: Union[list, tuple]):
        # 统一日期格式
        window, size = self._parse_interval(start, end, interval)
        # 获取目标的天勤合约代码列表
        instrument_dict = defaultdict(list)
        if ticker:
            products, instruments = self._parse_future_ticker(ticker)
            instrument_dict.update({k: [k] for k in instruments})
            if products:
                instruments_all = self.all_tq_instrument("Future", True, False)
                instruments_all.extend(self.all_tq_instrument("Future", False, False))
                for k in products:
                    k_ = ".".join(k.split(".")[:2])
                    for i in set(instruments_all):
                        if i.startswith(k_) and i.lstrip(k_).isdigit():
                            instrument_dict[i].append(k)
        else:
            instruments = self.all_tq_instrument("Future", True, False)
            instruments.extend(self.all_tq_instrument("Future", False, False))
            instrument_dict.update({k: [k] for k in instruments})

        self.get_last_tick(list(instrument_dict.keys()))

        data = pd.DataFrame(columns=['DateTime',"Ticker","Product","Instrument","Exchange","Open","High","Low","Close",
                                     "PrevClose","Volume","OpenInterest"])

        for instrument, tickers in instrument_dict.items():
            n = 1
            while n <= 5:
                try:
                    d = self._query_quote_future(window, size, instrument, use_progress=False)
                    info = self.last_tick[instrument]
                    d["DeliverYear"] = info.delivery_year
                    d["Product"] = d["Instrument"].map(lambda x: instrument_to_product("future", x)).str.upper()
                    d["Ticker"] = d[["Exchange", "Instrument", "DeliverYear"]].apply(
                        lambda x: instrument_to_ticker("future", *x), axis=1)
                    data = pd.concat([data, d], join="inner")
                    break
                except Exception as e:
                    self.connect()
                    if n == 5:
                        tb = traceback.format_exc()
                        self.ERROR(f"[TQSDK] ERROR: 行情数据获取失败：{instrument} | {interval}: {tb} {e}")
                    n += 1
        data = data[(data['DateTime'] >= unify_time(start)) & (data["DateTime"] <= unify_time(end))]
        return data.sort_values(by="DateTime")
