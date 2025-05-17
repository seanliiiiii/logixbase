import time, re
from datetime import (datetime, timedelta)
import json
from typing import Union
import pandas as pd
from pathlib import Path
from typing import Literal
import warnings

from ..configer import read_config
from ..utils import unify_time, load_module_from_file, select_date
from .base import BaseFeeder
from .schema import TinysoftConfig
from ..trader import parse_ticker, Interval


LOCAL_DIR = Path(__file__).parent
warnings.simplefilter(action='ignore')


class TinysoftFeeder(BaseFeeder):
    """
    Data API for Tinysoft
    """

    def __init__(self, conn_cfg: TinysoftConfig, logger = None, max_iter: int = 3, heartbeat: int = 5):
        super().__init__("Tinysoft", conn_cfg, logger)

        self._max_iter = max_iter
        self._heartbeat = heartbeat

        self._use_table: dict = {}
        self._column_map: dict = {}
        self._decode: dict = {}
        self._status_map: dict = {}
        self._exchange_map: dict = {}

        self._future: dict = {}
        self._etf: dict = {}
        self._stock: dict = {}
        self._index: dict = {}
        self._option: dict = {}

        self._tsl = None  # TS api module
        self.on_init()

    def on_init(self):
        """Initialize engine"""
        cfg = read_config(LOCAL_DIR / "config/tinysoft.yaml")
        self._use_table = cfg["table"]
        self._exchange_map = cfg["exchange"]
        self._column_map = cfg["column"]
        self._decode = cfg["decode"]
        self._status_map = cfg["status"]
        self._stock = cfg["stock"]
        self._future = cfg["future"]
        self._etf = cfg["etf"]
        self._index = cfg["index"]
        self._option = cfg["option"]
        
    def _decode_data(self, data):
        """Decode data according to column"""
        if isinstance(data, pd.DataFrame):
            decode_cols = [x for x in data.columns if x in self._decode]
            for col in decode_cols:
                data[col] = data[col].map(lambda x: x.decode(self._decode[col], errors="ignore").upper() if isinstance(x, bytes) else x)
        elif isinstance(data, list):
            for i, x in enumerate(data):
                if isinstance(x, dict):
                    x = {k.decode("utf8"): v.decode(self._decode[k.decode("utf8")], errors="ignore").upper()
                    if k.decode("utf8") in self._decode else v for k, v in x.items()}
                    data[i] = x
                elif isinstance(x, bytes):
                    for encoding in ['utf8', 'gbk', 'big5', 'shift_jis', 'iso-8859-1']:
                        try:
                            data[i] = x.decode(encoding)
                            break
                        except UnicodeDecodeError:
                            continue
        return data

    @property
    def is_connected(self):
        """Check login status of tinysoft"""
        return self._tsl.Logined()

    def connect(self):
        """Create Tinysoft connection"""
        self._tsl = load_module_from_file(Path(self._conn_cfg.path).joinpath("TSLPy3.pyd"))
        # Try to connect to tinysoft
        status = False
        i = 1
        while i < (self._max_iter + 1):
            self._tsl.ConnectServer(self._conn_cfg.host, self._conn_cfg.port)
            self._tsl.LoginServer(self._conn_cfg.username, self._conn_cfg.password)

            status = self.is_connected
            if status:
                self.INFO(f"天软连接成功：尝试{i}次，用户名{self._conn_cfg.username}")
                break
            else:
                time.sleep(self._heartbeat)
                i += 1
                # Return connection result
        if not status:
            self.ERROR(f"天软连接失败：用户名{self._conn_cfg.username} failed...")
            return False

        return status

    def disconnect(self):
        """Disconnect tinysoft connection"""
        self._tsl.Disconnect()
        if not self.is_connected():
            self.INFO(f"天软连接已断开：用户名{self._conn_cfg.username}")

    def check_connect(self):
        """Check login status and ensure connection"""
        if not self.is_connected:
            self.INFO(f"天软连接已断开，尝试重新连接: 用户名{self._conn_cfg.username}")
            self.connect()

    def exec_query(self, query: str, msg: str = ""):
        """Execute query data from tinysoft"""
        dl = self._tsl.RemoteExecute(query, {})

        if dl[0] == 0:
            return dl[1]
        else:
            self.INFO(f"查询失败：{msg}, 错误代码：{dl[2]}")

    @staticmethod
    def _parse_to_tstime(start: Union[datetime, int, str], end: Union[datetime, int, str], interval: str):
        # 统一时间
        dt_start = unify_time(start)
        t_ = f"({dt_start.hour} + {dt_start.minute} / 60) / 24" if interval.lower() != "day" else "0"
        dt_end = unify_time(end)
        t_1 = f"({dt_end.hour} + {dt_end.minute} / 60) / 24"
        if eval(t_1) == 0:
            t_1 = "0.9999"
        dt_start = unify_time(start, fmt="str", mode=3, dot="")
        dt_end = unify_time(end, fmt="str", mode=3, dot="")

        ts_start = f"inttodate({dt_start}) + {t_}"
        ts_end = f"inttodate({dt_end}) + {t_1}"

        return ts_start, ts_end

    def _parse_to_tsticker(self, asset: str, ticker: list):
        if not ticker:
            return f"GETBK('{self._use_table[asset.lower()]}')"

        product, ticker_lst = parse_ticker(asset, ticker)
        all_tk = eval(f"self.{asset}_ticker()")
        ts_ticker = []
        if product:
            ts_ticker.extend([i for k in product for i in all_tk if i.startswith(k) and i.lstrip(k).isdigit()])
        if ticker_lst:
            ts_ticker.extend([k for k in ticker_lst if k.upper() in all_tk])
        invalid = [k for k in ticker_lst if k not in ts_ticker]
        if invalid:
            self.ERROR(f"无法识别以下{asset.capitalize()}合约: {invalid}")
        return f"'{';'.join(ts_ticker)}'"

    def all_tradeday(self, start: Union[str, datetime, int], end: Union[str, datetime, int], fmt: str = "datetime",
                      freq: str = "D", day: int = 1, market: str = "China"):
        """Query domestic trade dates"""
        if market.lower() != "china":
            self.ERROR(f"天软仅支持查询中国交易日: {market}")

        self.check_connect()
        # Union time format
        start = unify_time(start, fmt="str", mode=3, dot="")
        end = unify_time(end, fmt="str", mode=3, dot="")

        # Query data from Tinysoft
        query = f"""
                BEGT:= {start};
                ENDT:= {end};
                DAYARR:= SSELECT ['截止日'] FROM INFOTABLE 753 OF 'SH000001'
                         WHERE ['截止日'] >= BEGT 
                         AND ['截止日'] <= ENDT 
                         AND ['是否交易日'] = 1 
                         ORDER BY ['截止日'] END; 
                IF NOT ISTABLE(DAYARR) THEN ENDT1:= ENDT; 
                ELSE ENDT1:= DAYARR[0]; 
                HISARR:= MARKETTRADEDAYQK(INTTODATE(BEGT), INTTODATE(ENDT1));
                RETURN DATETOINT(HISARR) UNION2 DAYARR;
                """

        dts = self.exec_query(query, msg="获取交易日数据")

        if freq.upper() == "D":
            dts = sorted([unify_time(k, fmt=fmt, mode=3) for k in dts])
        else:
            dts = [unify_time(k, fmt=fmt, mode=3) for k in select_date(dts, freq=freq, day=day)]

        self.INFO(f"获取{start}至{end}期间的交易日数据: {int(len(dts))}个交易日")
        return dts

    def asset_info(self, asset: str, info: Literal["basic", "trade"] = "basic", date: datetime = None):
        asset = asset.lower()
        info = info.lower()
        if asset not in ["future", "stock", "etf", "index", "option"]:
            self.ERROR("资产类型无法识别：仅支持future（期货）/stock（股票）/etf（ETF）/index（指数）/option（期权）")
            return

        if info not in ["basic", "trade"]:
            self.ERROR("信息类型无法识别：仅支持basic（基础合约信息）/trade（合约交易信息）")
            return

        if info == "trade" and asset not in ["future", "option"]:
            self.ERROR("交易信息仅适用于期货和期权")
            return

        func = eval(f"self.{asset}_info_{info}")

        if asset == "option":
            info_data = func(date)
        else:
            info_data = func()
        self.INFO(f"{asset.capitalize()}资产{info.capitalize()}信息获取完成: {int(len(info_data))}条记录")
        return info_data

    def future_info_basic(self):
        """Get all basic information of domestic future contracts in history"""
        self.check_connect()
        cols = ["Ticker", "Contract", "ListDate", "DelistDate", "DeliverDate", "Multiplier", "TickSize",
                "Exchange", "MinMargin", "PriceLimit", "QuoteUnit", "MultiplierUnit", "FutureClass", "ComFutureClass"]
        # Query data from Tinysoft
        use_table = self._use_table["future"]
        query = f"""
                RETURN QUERY(
                '{use_table}', '' , TRUE, '', 
                '代码', DEFAULTSTOCKID(),
                '变动日(初值)', BASE(703002,0),
                '最后交易日(末值)', BASE(703018),
                '最后交割日(末值)', BASE(703024),
                '交易代码(末值)', BASE(703003),
                '合约乘数(末值)', BASE(703007),
                '合约乘数单位(末值)', BASE(703008),
                '报价单位(末值)', BASE(703009),
                '最小变动价位(末值)', BASE(703010),
                '最低交易保证金(%%)(末值)', BASE(703025),
                '上市地(末值)', BASE(703027),
                '期货类别(末值)', BASE(703028),
                '商品期货类别(末值)', BASE(703029),
                '涨跌停板幅度(%%)(末值)', BASE(710003)
                );
                """
        data = self.exec_query(query, msg="期货基础信息")

        if data:
            # Format future basic info data
            basic_info = pd.DataFrame(data)
            basic_info.columns = [self._column_map[i.decode("gbk")] for i in basic_info.columns]
            # Decode data
            basic_info = self._decode_data(basic_info)
            # Filter invalid data
            basic_info = basic_info[~((basic_info["ListDate"] == 0) & (basic_info["DelistDate"] == 0))]
            # Data map
            basic_info.loc[:, "Exchange"] = basic_info.loc[:, "Exchange"].map(self._exchange_map)
            basic_info.loc[:, "FutureClass"] = basic_info.loc[:, "FutureClass"].map(self._future["class"])
            basic_info.loc[:, "ComFutureClass"] = basic_info.loc[:, "ComFutureClass"].map(self._future["class"])
            # Deal with missing datetime data
            for tag in ["ListDate", "DelistDate"]:
                mask = basic_info[tag] == 0
                info_ = basic_info.loc[mask, "Ticker"]
                dt_ = ["20" + re.findall(r"\d+", x)[0] + "15" for x in info_]

                if tag == "ListDate":
                    dt_ = [(unify_time(x) + timedelta(days=-365)).strftime("%Y%m%d") for x in dt_]

                dt_ = [int(x) for x in dt_]

                if dt_:
                    basic_info.loc[mask, tag] = dt_

            adj_cols = ["ListDate", "DelistDate", "DeliverDate"]
            basic_info.loc[:, adj_cols] = basic_info[adj_cols].applymap(lambda x: unify_time(x, fmt="str", mode=3))

            # Unit adjust to real float
            basic_info.loc[:, ["MinMargin", "PriceLimit"]] = basic_info.loc[:, ["MinMargin", "PriceLimit"]] / 100

            basic_info = basic_info.loc[:, cols]
        else:
            basic_info = pd.DataFrame(columns=cols)

        return basic_info

    def future_info_trade(self):
        """Get trade clearing parameters of all domestic futures contracts"""
        self.check_connect()
        cols = ["Ticker", "ExecDate", "LongMargin", "ShortMargin", "OpenCommission_Pct", "OpenCommission_Fix",
                "CloseCommission_Pct", "CloseCommission_Fix", "IntradayOpenCommission_Pct",
                "IntradayOpenCommission_Fix", "IntradayCloseCommission_Pct", "IntradayCloseCommission_Fix"]

        # Query data from
        use_table = self._use_table["future"]
        query = f"""
                FUTURES:= GETBK('{use_table}');
                DATA:= SELECT * FROM INFOTABLE 704 OF FUTURES END;
                RETURN DATA;        
                """
        data = self.exec_query(query, msg="期货交易信息")

        if data:
            # Format future trade info data
            trade_info = pd.DataFrame(data)
            trade_info.columns = [self._column_map[i.decode("gbk")] for i in trade_info.columns]
            # Decode data
            trade_info = self._decode_data(trade_info)
            # Filter invalid data
            trade_info = trade_info[(trade_info["ExecDate"] != 0)]
            # Adjust format of data
            trade_info["ExecDate"] = trade_info["ExecDate"].map(lambda x: unify_time(x, fmt="str", mode=3))
            trade_info.loc[:, ["LongMargin", "ShortMargin"]] = trade_info.loc[:, ["LongMargin", "ShortMargin"]] / 100
            pct_cols = [x for x in trade_info.columns if "Pct" in x]
            trade_info.loc[:, pct_cols] = (trade_info.loc[:, pct_cols] / 10000).applymap(lambda x: round(x, 6))

            trade_info = trade_info.loc[:, cols]
        else:
            trade_info = pd.DataFrame(columns=cols)

        return trade_info

    def stock_info_basic(self):
        """Get all basic information of domestic stocks in history"""
        self.check_connect()
        cols = ["Ticker", "Code", "RegisteredCapital", "Representative", "EstablishDate", "ListDate", "Exchange",
                "Board",
                "TotalShares", "TotalTradeableShares", "MainBusiness", "Status", "ShenwanIndustryLevel1",
                "ShenwanIndustryLevel2", "ShenwanIndustryLevel3"]
        # Query data from Tinysoft
        use_table = self._use_table["stock"]
        query = f"""
                 RETURN QUERY(
                 '{use_table}', '' , TRUE, '', 
                 '代码', DEFAULTSTOCKID(),
                 '名称', BASE(10004),
                 '注册资本', BASE(10008),
                 '法定代表人', BASE(10009),
                 '成立日期', BASE(10010),
                 '变动日(初值)', BASE(12017),
                 '上市地(末值)', BASE(12022),
                 '板块', BASE(10028),
                 '总股本', BASE(16002),
                 '流通A股', BASE(16017),
                 '主营业务', BASE(10014),
                 '当前状态', BASE(10026),
                 '申万一级行业代码', BASE(10035),
                 '申万二级行业代码', BASE(10036),
                 '申万三级行业代码', BASE(10037)
                 );
                 """

        data = self.exec_query(query, msg="股票基础信息")

        if data:
            # Format stock basic info data
            basic_info = pd.DataFrame(data)
            basic_info.columns = [self._column_map[i.decode("gbk")] for i in basic_info.columns]            #
            # Decode data
            basic_info = self._decode_data(basic_info)
            # Filter invalid data
            basic_info = basic_info[~(basic_info["ListDate"] == 0)]
            # Data map
            basic_info.loc[:, "Exchange"] = basic_info.loc[:, "Exchange"].map(self._exchange_map)
            basic_info.loc[:, "Board"] = basic_info.loc[:, "Board"].map(self._stock["board"])
            basic_info.loc[:, "Status"] = basic_info.loc[:, "Status"].map(self._status_map)
            basic_info["EstablishDate"][basic_info["EstablishDate"] == 0] = basic_info["ListDate"]

            adj_cols = ["ListDate", "EstablishDate"]
            basic_info.loc[:, adj_cols] = basic_info[adj_cols].applymap(lambda x: unify_time(x, fmt="str", mode=3))

            basic_info = basic_info.loc[:, cols]
        else:
            basic_info = pd.DataFrame(columns=cols)

        return basic_info

    def etf_info_basic(self):
        """Get all basic information of domestic ETFs in history"""
        self.check_connect()
        cols = ["Ticker", "Code", "EstablishDate", "ListDate", "Exchange", "Style", "Target", "TotalIssuedShares",
                "BenchMark", "TradeSymbol"]
        # Query data from Tinysoft
        use_table = self._use_table["etf_info"]
        query = f"""
                 RETURN QUERY(
                 '{use_table}', '' , TRUE, '', 
                 '一级市场代码', DEFAULTSTOCKID(),
                 '名称', BASE(302001),
                 '成立日期', BASE(302003),
                 '变动日(初值)', BASE(302004),
                 '上市地(末值)', BASE(302030),
                 '投资风格', BASE(302014),
                 '投资目标', BASE(302015),
                 '募集总份额', BASE(302049),
                 '标的指数代码', BASE(302039),
                 '代码', BASE(302033)
                 );
                 """
        data = self.exec_query(query, msg="ETF基础信息")

        if data:
            # Format stock basic info data
            basic_info = pd.DataFrame(data)
            basic_info.columns = [self._column_map[i.decode("gbk")] for i in basic_info.columns]
            # # Decode data
            basic_info = self._decode_data(basic_info)
            # Filter invalid data
            basic_info = basic_info[~(basic_info["ListDate"] == 0)]
            # Data map
            basic_info.loc[:, "Exchange"] = basic_info.loc[:, "Exchange"].map(self._exchange_map)
            basic_info.loc[:, "Style"] = basic_info.loc[:, "Style"].map(self._etf["class"]["style"])
            basic_info["EstablishDate"][basic_info["EstablishDate"] == 0] = basic_info["ListDate"]
            adj_cols = ["ListDate", "EstablishDate"]
            basic_info.loc[:, adj_cols] = basic_info[adj_cols].applymap(lambda x: unify_time(x, fmt="str", mode=3))

            basic_info = basic_info.loc[:, cols]
        else:
            basic_info = pd.DataFrame(columns=cols)

        return basic_info

    def index_info_basic(self):
        """Get all basic information of domestic ETFs in history"""
        self.check_connect()
        cols = ["Ticker", "Code", "Exchange", "EstablishDate", "ListDate", "BeginPoint", "SampleSize", "IndexClassL1",
                "IndexClassL2", "IndexClassL3"]
        # Query data from Tinysoft
        use_table = self._use_table["index"]
        query = f"""
                 RETURN QUERY(
                 '{use_table}', '' , TRUE, '', 
                 '代码', DEFAULTSTOCKID(),
                 '名称', BASE(750000),
                 '指数所属公司(末值)',base(750003),
                 '成立日期', BASE(750005),
                 '变动日(初值)', BASE(750004),
                 '指数起始点', BASE(750006),
                 '样本个数', BASE(750008),
                 '指数类型', BASE(750013),
                 '指数类型2', BASE(750014),
                 '指数类型3', BASE(750015)
                 );
                 """
        data = self.exec_query(query, msg="指数基础信息")

        if data:
            # Format index basic info data
            basic_info = pd.DataFrame(data)
            basic_info.columns = [self._column_map[i.decode("gbk")] for i in basic_info.columns]
            # # Decode data
            basic_info = self._decode_data(basic_info)
            basic_info = basic_info[~basic_info['Ticker'].str.contains('CNY|USD|HKD|GBP|EUR')]
            basic_info = basic_info[
                ~basic_info.loc[:, 'Code'].str.contains('人民币|美元|港元|欧元|英镑|HK|香港|U$|CNY|USD|HKD|GBP|EUR|AUD')]
            basic_info = basic_info[basic_info.loc[:, 'IndexClassL2'].isin(['综合指数', '规模指数']) |
                                    (basic_info.loc[:, 'IndexClassL3'].isin(['一级行业指数', '二级行业指数']) &
                                     basic_info.loc[:, 'Ticker'].str.contains('SW'))]

            # Filter invalid data
            basic_info = basic_info[~(basic_info.loc[:, "ListDate"] == 0)]
            # Data map
            basic_info.loc[:, "Exchange"] = basic_info.loc[:, "Exchange"].map(self._exchange_map)
            basic_info.loc[:, "IndexClassL1"] = basic_info.loc[:, "IndexClassL1"].map(self._index["class"]["level1"])
            basic_info.loc[:, "IndexClassL2"] = basic_info.loc[:, "IndexClassL2"].map(self._index["class"]["level2"])
            basic_info.loc[:, "IndexClassL3"] = basic_info.loc[:, "IndexClassL3"].map(self._index["class"]["level3"])
            basic_info["EstablishDate"][basic_info["EstablishDate"] == 0] = basic_info["ListDate"]
            adj_cols = ["ListDate", "EstablishDate"]
            basic_info.loc[:, adj_cols] = basic_info[adj_cols].applymap(lambda x: unify_time(x, fmt="str", mode=3))

            basic_info = basic_info.loc[:, cols]
        else:
            basic_info = pd.DataFrame(columns=cols)

        return basic_info

    def option_info_basic(self, date):
        self.check_connect()
        cols = ["TradeDay", "Ticker", "Underlying", "UnderlyingClass", "ExerciseType", "CallorPut", "Multiplier",
                "Strike", "ListDate", "DelistDate", "ExpiryDate", "TickSize", "Exchange"]

        date = unify_time(date, mode=3)
        basic_info = pd.DataFrame()
        underlying = self.option_underlying()
        for ticker in underlying:
            # Query data from Tinysoft
            query = """
                   EndT:= %dT;
                   t:=exportjsonstring(OP_GetOptionChain('%s',EndT));
                   return t;
                   """ % (date.year * 10000 + date.month * 100 + date.day, ticker)
            data = self.exec_query(query, msg="期权基础信息")
            if data and (data != b'[]'):
                # Format option basic info data
                info_ = pd.DataFrame(json.loads(data))
                info_.columns = [self._column_map[i] for i in info_.columns]
                # Filter invalid data
                info_ = info_[~((info_["ListDate"] == 0) & (info_["DelistDate"] == 0))]
                # Data map
                info_.loc[:, "Exchange"] = info_.loc[:, "Exchange"].map(self._exchange_map)
                info_.loc[:, "ExerciseType"] = info_.loc[:, "ExerciseType"].map(self._option["exercise_type"])
                info_.loc[:, "CallorPut"] = info_.loc[:, "CallorPut"].map(self._option["direction_type"])
                info_.loc[:, "UnderlyingClass"] = info_.loc[:, "UnderlyingClass"].map(self._option["class"])
                info_["TradeDay"] = date
                # There is no TickSize from SZSE, so set it to 0.0001
                info_["TickSize"].loc[info_["Exchange"] == "SZSE"] = 0.0001
                # Deal with missing datetime data
                for tag in ["ListDate", "DelistDate"]:
                    mask = info_[tag] == 0
                    ticker_ = info_.loc[mask, "Ticker"]
                    dt_ = ["20" + re.findall(r"\d+", x)[0] + "15" for x in ticker_]
                    if tag == "ListDate":
                        dt_ = [(unify_time(x) + timedelta(days=-365)).strftime("%Y%m%d") for x in dt_]
                    dt_ = [int(x) for x in dt_]
                    if dt_:
                        info_.loc[mask, tag] = dt_
                adj_cols = ["ListDate", "DelistDate", "ExpiryDate", "TradeDay"]
                info_.loc[:, adj_cols] = info_[adj_cols].applymap(lambda x: unify_time(x, fmt="str", mode=3))
                info_ = info_.loc[:, cols]
            else:
                info_ = pd.DataFrame(columns=cols)

            if info_.any:
                basic_info = pd.concat([basic_info, info_], axis=0)
        return basic_info

    def asset_quote(self, asset: str, start: Union[datetime, str, int], end: Union[datetime, str, int],
                    interval: Union[str, int], ticker: Union[list, tuple] = None, underlying: Union[list, tuple] = None):
        asset = asset.lower()

        if underlying and asset == "option":
            self.ERROR("天软不支持指定底层合约的期权行情数据获取，默认返回所有合约数据")

        if asset not in ["future", "stock", "etf", "index", "option"]:
            self.ERROR("资产类型无法识别：仅支持future（期货）/stock（股票）/etf（ETF）/index（指数）/option（期权）")
            return

        interval = Interval(interval)

        if interval.pattern == "minute":
            ts_interval = f"{interval.window}m"
        elif interval.pattern == "day":
            ts_interval = "day"
        else:
            self.ERROR("时间间隔无法识别：仅支持daily / int(min)")
            return
        func = eval(f"self.{asset}_quote")
        quote = func(start, end, ts_interval, ticker)
        if quote is not None:
            self.INFO(f"{asset.capitalize()}资产{ts_interval}行情数据获取完成: {int(quote.shape[0])}条记录")
            return quote

    def future_quote(self, start: Union[datetime, str, int], end: Union[datetime, str, int], interval: str,
                     ticker: Union[list, tuple] = None):
        self.check_connect()
        cols=['DateTime','Ticker','Open','Close','High','Low','PrevClose','Settle','PrevSettle','Volume','Amount',
              'OpenInterest']
        # 统一时间
        ts_start, ts_end = self._parse_to_tstime(start, end, interval)
        # 解析为天软识别的ticker
        ts_tickers = self._parse_to_tsticker("future", ticker)

        query = f"""
                SETSYSPARAM(PN_CYCLE(), CY_{interval}());
                STOCKS:= {ts_tickers};
                BEGT:= {ts_start};
                ENDT:= {ts_end};
                DATA:= SELECT DATETIMETOSTR(['DATE']) AS 'DateTime',
                              ['STOCKID'] AS 'Ticker',
                              ['OPEN'] AS 'Open',
                              ['CLOSE'] AS 'Close',
                              ['HIGH'] AS 'High',
                              ['LOW'] AS 'Low',
                              ['YCLOSE'] AS 'PrevClose',
                              ['SYL1'] AS 'Settle',
                              ['SYL2'] AS 'PrevSettle',                            
                              ['VOL'] AS 'Volume',
                              ['AMOUNT'] AS 'Amount',
                              ['SECTIONAL_CJBS'] AS 'OpenInterest'
                        FROM MARKETTABLE DATEKEY BEGT TO ENDT OF STOCKS END;
                RETURN DATA;
                """
        data = self.exec_query(query, f"期货{interval}行情数据")
        if data:
            # Format future daily quote data
            quote = pd.DataFrame(data)
            quote.columns = [i.decode("utf8") for i in quote.columns]
            quote = self._decode_data(quote).loc[:, cols]
            quote["DateTime"] = quote["DateTime"].map(unify_time)
            quote.iloc[:, 2:] = quote.iloc[:, 2:].astype(float)
        else:
            quote = pd.DataFrame(columns=cols)
        return quote.sort_values(by="DateTime").loc[:, cols]

    def stock_quote(self, start: Union[datetime, str, int], end: Union[datetime, str, int], interval: str,
                    ticker: Union[list, tuple] = None):
        self.check_connect()
        cols = ["DateTime","Ticker","Open","Close","High","Low","PrevClose","Volume","Amount","DealNumber","Committee",
                "QuantityRelative", "BuyVolume", "BuyAmount","SaleVolume", "SaleAmount", "CommitBuy","CommitSale"]
        # 统一时间
        ts_start, ts_end = self._parse_to_tstime(start, end, interval)
        # 解析为天软识别的ticker
        ts_tickers = self._parse_to_tsticker("stock", ticker)

        query = f"""
                 SETSYSPARAM(PN_CYCLE(),CY_{interval}()); 
                 STOCKS:= {ts_tickers};
                 BEGT:= {ts_start};
                 ENDT:= {ts_end};
                 DATA:= SELECT DATETIMETOSTR(['DATE']) AS 'DateTime',
                               ['STOCKID'] AS 'Ticker',
                               ['OPEN'] AS 'Open',
                               ['CLOSE'] AS 'Close',
                               ['HIGH'] AS 'High',
                               ['LOW'] AS 'Low',
                               ['YCLOSE'] AS 'PrevClose',
                               ['VOL'] AS 'Volume',
                               ['AMOUNT'] AS 'Amount',
                               ['CJBS'] AS 'DealNumber',
                               ['WB'] AS 'Committee',
                               ['LB'] AS 'QuantityRelative',
                               ['BUY_VOL'] AS 'BuyVolume',
                               ['BUY_AMOUNT'] AS 'BuyAmount',
                               ['SALE_VOL'] AS 'SaleVolume',
                               ['SALE_AMOUNT'] AS 'SaleAmount',
                               ['W_BUY'] AS 'CommitBuy',
                               ['W_SALE'] AS 'CommitSale'                                                    
                        FROM MARKETTABLE DATEKEY BEGT TO ENDT OF STOCKS END;
                 RETURN DATA;
                """
        data = self.exec_query(query, f"股票{interval}行情")
        if data:
            # Format future daily quote data
            quote = pd.DataFrame(data)
            quote.columns = [i.decode("utf8") for i in quote.columns]
            quote = self._decode_data(quote).loc[:, cols]
            quote["DateTime"] = quote["DateTime"].map(unify_time)
            quote.iloc[:, 2:] = quote.iloc[:, 2:].astype(float)
        else:
            quote = pd.DataFrame(columns=cols)
        return quote.sort_values(by="DateTime")

    def etf_quote(self, start: Union[datetime, str, int], end: Union[datetime, str, int], interval: str,
                  ticker: Union[list, tuple] = None):
        self.check_connect()
        cols = ["DateTime", "Ticker", "Open", "Close", "High", "Low", "PrevClose", "Volume", "Amount", "DealNumber",
                "Committee", "QuantityRelative", "BuyVolume", "BuyAmount", "SaleVolume", "SaleAmount", "CommitBuy",
                "CommitSale"]
        # 统一时间
        ts_start, ts_end = self._parse_to_tstime(start, end, interval)
        # 解析为天软识别的ticker
        ts_tickers = self._parse_to_tsticker("etf", ticker)

        query = f"""
                 SETSYSPARAM(PN_CYCLE(),CY_{interval}()); 
                 STOCKS:= {ts_tickers};
                 BEGT:= {ts_start};
                 ENDT:= {ts_end};
                 DATA:= SELECT DATETIMETOSTR(['DATE']) AS 'DateTime',
                               ['STOCKID'] AS 'Ticker',
                               ['OPEN'] AS 'Open',
                               ['CLOSE'] AS 'Close',
                               ['HIGH'] AS 'High',
                               ['LOW'] AS 'Low',
                               ['YCLOSE'] AS 'PrevClose',
                               ['VOL'] AS 'Volume',
                               ['AMOUNT'] AS 'Amount',
                               ['CJBS'] AS 'DealNumber',
                               ['WB'] AS 'Committee',
                               ['LB'] AS 'QuantityRelative',
                               ['BUY_VOL'] AS 'BuyVolume',
                               ['BUY_AMOUNT'] AS 'BuyAmount',
                               ['SALE_VOL'] AS 'SaleVolume',
                               ['SALE_AMOUNT'] AS 'SaleAmount',
                               ['W_BUY'] AS 'CommitBuy',
                               ['W_SALE'] AS 'CommitSale'                              
                        FROM MARKETTABLE DATEKEY BEGT TO ENDT OF STOCKS END;
                 RETURN DATA;
                """
        data = self.exec_query(query, f"ETF {interval}行情")
        if data:
            # Format future daily quote data
            quote = pd.DataFrame(data)
            quote.columns = [i.decode("utf8") for i in quote.columns]
            quote = self._decode_data(quote).loc[:, cols]
            quote["DateTime"] = quote["DateTime"].map(unify_time)
            quote.iloc[:, 2:] = quote.iloc[:, 2:].astype(float)
        else:
            quote = pd.DataFrame(columns=cols)
        return quote.sort_values(by="DateTime")

    def index_quote(self, start: Union[datetime, str, int], end: Union[datetime, str, int], interval: str,
                    ticker: Union[list, tuple] = None):
        self.check_connect()
        cols = ["DateTime", "Ticker", "Open", "Close", "High", "Low", "PrevClose", "Volume", "Amount", "DealNumber",
                "Committee", "QuantityRelative", "BuyVolume", "BuyAmount", "SaleVolume", "SaleAmount", "CommitBuy",
                "CommitSale"]
        # 统一时间
        ts_start, ts_end = self._parse_to_tstime(start, end, interval)
        # 解析为天软识别的ticker
        ts_tickers = self._parse_to_tsticker("index", ticker)

        query = f"""
                 SETSYSPARAM(PN_CYCLE(),CY_{interval}()); 
                 STOCKS:= {ts_tickers};
                 BEGT:= {ts_start};
                 ENDT:= {ts_end};
                 DATA:= SELECT DATETIMETOSTR(['DATE']) AS 'DateTime',
                               ['STOCKID'] AS 'Ticker',
                               ['OPEN'] AS 'Open',
                               ['CLOSE'] AS 'Close',
                               ['HIGH'] AS 'High',
                               ['LOW'] AS 'Low',
                               ['YCLOSE'] AS 'PrevClose',
                               ['VOL'] AS 'Volume',
                               ['AMOUNT'] AS 'Amount',
                               ['CJBS'] AS 'DealNumber',
                               ['WB'] AS 'Committee',
                               ['LB'] AS 'QuantityRelative',
                               ['BUY_VOL'] AS 'BuyVolume',
                               ['BUY_AMOUNT'] AS 'BuyAmount',
                               ['SALE_VOL'] AS 'SaleVolume',
                               ['SALE_AMOUNT'] AS 'SaleAmount',
                               ['W_BUY'] AS 'CommitBuy',
                               ['W_SALE'] AS 'CommitSale'                              
                        FROM MARKETTABLE DATEKEY BEGT TO ENDT OF STOCKS END;
                 RETURN DATA;
                """
        data = self.exec_query(query, f"指数{interval}行情")
        if data:
            # Format future daily quote data
            quote = pd.DataFrame(data)
            quote.columns = [i.decode("utf8") for i in quote.columns]
            quote = self._decode_data(quote).loc[:, cols]
            quote["DateTime"] = quote["DateTime"].map(unify_time)
            quote.iloc[:, 2:] = quote.iloc[:, 2:].astype(float)
        else:
            quote = pd.DataFrame(columns=cols)
        return quote.sort_values(by="DateTime")

    def option_quote(self, start: Union[datetime, str, int], end: Union[datetime, str, int], interval: str,
                     ticker: Union[list, tuple] = None):
        self.check_connect()
        cols = ["DateTime", "Ticker", "Open", "Close", "High", "Low", "PrevClose", "Settle", "PrevSettle",
                "Volume", "Amount", "OpenInterest"]
        # 统一时间
        ts_start, ts_end = self._parse_to_tstime(start, end, interval)
        # 解析为天软识别的ticker
        ts_tickers = self._parse_to_tsticker("option", ticker)

        query = f"""
                SETSYSPARAM(PN_CYCLE(), CY_{interval}());
                STOCKS:= {ts_tickers};
                BEGT:= {ts_start};
                ENDT:= {ts_end};
                DATA:= SELECT DATETIMETOSTR(['DATE']) AS 'DateTime',
                              ['STOCKID'] AS 'Ticker',
                              ['OPEN'] AS 'Open',
                              ['CLOSE'] AS 'Close',
                              ['HIGH'] AS 'High',
                              ['LOW'] AS 'Low',
                              ['YCLOSE'] AS 'PrevClose',
                              ['SYL1'] AS 'Settle',
                              ['SYL2'] AS 'PrevSettle',      
                              ['VOL'] AS 'Volume',
                              ['AMOUNT'] AS 'Amount',
                              ['SECTIONAL_CJBS'] AS 'OpenInterest'
                        FROM MARKETTABLE DATEKEY BEGT TO ENDT OF STOCKS END;
                RETURN DATA;
                """

        data = self.exec_query(query, f"期权{interval}行情数据")
        if data:
            # Format future daily quote data
            quote = pd.DataFrame(data)
            quote.columns = [i.decode("utf8") for i in quote.columns]
            quote = self._decode_data(quote).loc[:, cols]
            quote["DateTime"] = quote["DateTime"].map(unify_time)
            quote.iloc[:, 2:] = quote.iloc[:, 2:].astype(float)
        else:
            quote = pd.DataFrame(columns=cols)
        return quote.sort_values(by="DateTime")

    def asset_ticker(self, asset: str, active: bool):
        asset = asset.lower()
        if asset not in ["future", "stock", "etf", "index", "option"]:
            self.ERROR("资产类型无法识别：仅支持future（期货）/stock（股票）/etf（ETF）/index（指数）/option（期权）")
            return
        tickers = eval(f"self.{asset}_ticker")(active)
        self.INFO(f"{asset.capitalize()}资产{'活跃' if active else '全部'}合约获取完成: {int(len(tickers))}条记录")
        return tickers

    def future_ticker(self, active: bool = False):
        if active:
            table = self._use_table["alive"]["future"]
        else:
            table = self._use_table["future"]
        tickers = self._decode_data(self._tsl.RemoteCallFunc("getbk", [table], {})[1])
        return [k.upper() for k in tickers]

    def stock_ticker(self, active: bool = False):
        """"""
        if active:
            table = self._use_table["alive"]["stock"]
        else:
            table = self._use_table["stock"]
        tickers = self._decode_data(self._tsl.RemoteCallFunc("getbk", [table], {})[1])
        tickers = [''.join(re.findall(r'\d', k)) for k in tickers]
        return tickers

    def etf_ticker(self, active: bool = False):
        """"""
        if active:
            table = self._use_table["alive"]["etf_quote"]
        else:
            table = self._use_table["etf_quote"]
        tickers = self._decode_data(self._tsl.RemoteCallFunc("getbk", [table], {})[1])
        tickers = [''.join(re.findall(r'\d', k)) for k in tickers]
        return tickers

    def index_ticker(self, active: bool = False):
        if active:
            table = self._use_table["alive"]["index"]
        else:
            table = self._use_table["index"]
        tickers = self._decode_data(self._tsl.RemoteCallFunc("getbk", [table], {})[1])
        return tickers

    def option_ticker(self, active: bool=False):
        """"""
        if active:
            table = self._use_table["alive"]["option"]
        else:
            table = self._use_table["option"]
        tickers = self._decode_data(self._tsl.RemoteCallFunc("getbk", [table], {})[1])
        return tickers

    def option_underlying(self):
        query = """
            t:=exportjsonstring(OP_GetUnderlyingSecurity());
            return t;
        """
        tickers = self.exec_query(query, msg="获取全部期权底层资产代码")
        tickers = json.loads(tickers)
        self.INFO(f"期权底层资产列表获取完成: {int(len(tickers))}条记录")
        return tickers

    def option_ticker_spec(self, date, underlying):
        day = unify_time(date, mode=3)
        query = """
            EndT:= %dT;
            t:=exportjsonstring(OP_GetOptionChain('%s',EndT));
            return t;
        """ % (day.year * 10000 + day.month * 100 + day.day, underlying)
        tickers = self.exec_query(query, msg="获取期权合约")
        tickers = json.loads(tickers)
        result = []
        for i in tickers:
            result.append(i['StockID'])
        self.INFO(f"{underlying.upper()}期权{unify_time(day, 'str', 3)}底层资产列表获取完成: {int(len(result))}条记录")
        return result

    def treasury_ctd(self, ticker: str, day: Union[datetime, str, int], end: Union[datetime, str, int] = None):
        """获取国债期货最廉交割券数据"""
        self.check_connect()
        if end is not None:
            self.INFO("天软国债期货最廉交割券数据仅支持单日查询，end参数无效")
        dt = unify_time(day, fmt="str", mode=3, dot="")
        ctd_df = pd.DataFrame(columns=["TtlPrice", "TFactor", "CTD_IRR", "Ticker", "TradePrice", "BondCode",
                                       "FsSpread", "CTD_MD", "Code", "Basis", "CTD_BNOC", "IRR", "NetBasis",
                                       "BookInterest", "AdjDuration"])
        # Query data from Tinysoft
        query = f"""
                 SETSYSPARAM(PN_STOCK(), "{ticker}");
                 ENDT:={dt}T;
                 RETURN BondFutureBasicindicators2(ENDT,-1,1,2);
                """
        data = self.exec_query(query, "查询国债基差")

        if data:
            ctd_ = pd.DataFrame(data)
            ctd_.columns = [self._column_map.get(i.decode("gbk"), i.decode("gbk")) for i in ctd_.columns]
            ctd_ = self._decode_data(ctd_)
            ctd_ = ctd_.loc[ctd_["TtlPrice"] != 0]
            ctd_df = pd.concat([ctd_df, ctd_])

        return ctd_df

    def stock_coefadj(self, end: Union[datetime, str, int], start: Union[datetime, str, int] = None):
        self.check_connect()
        cols = ["DateTime", "Ticker", "CoefAdj"]
        # 统一时间
        if start:
            dt_start = f"{unify_time(start, fmt='str', mode=3, dot='')}T"
        else:
            dt_start = "firstday()"
        dt_end = unify_time(end, fmt="str", mode=3, dot="")

        query = f"""
                  begt := datetoint({dt_start});
                  endt := datetoint({dt_end}T);
                  stocks := GETBK('{self._use_table['stock']}');
                  data := array();
                  k := 0;
                  for i := 0 to length(stocks)-1 do
                  begin
                    stockid := stocks[i];
                    setsysparam(pn_stock(),stockid);
                    info := select ['除权除息日']from infotable 18 of stockid
                            where['除权除息日']<> 0 and['除权除息日']>= begt and['除权除息日']<= endt end;
                    if not istable(info) then continue;
                    for j := 0 to length(info)-1 do
                    begin
                      date:=info[j,'除权除息日'];
                      data[k,'DateTime']:= date;
                      data[k,'Ticker']:= stockid;
                      setsysparam(pn_date(),IntToDate(date));
                      data[k,'CoefAdj']:= ref(close(),1)/sys_prevclose();
                      k++;
                    end;
                  end;
                  return data;
                """
        data = self.exec_query(query, "查询股票后复权因子")

        if data:
            coef_adj = pd.DataFrame(data)
            coef_adj.columns = [i.decode("utf8") for i in coef_adj.columns]
            coef_adj = self._decode_data(coef_adj)
            coef_adj = coef_adj.loc[:, cols]
            coef_adj['DateTime'] = coef_adj['DateTime'].map(unify_time)
            coef_adj = coef_adj[(coef_adj["CoefAdj"] != 0) & (~coef_adj["CoefAdj"].isna())]
        else:
            coef_adj = pd.DataFrame(columns=cols)
        return coef_adj.sort_values(by="DateTime")