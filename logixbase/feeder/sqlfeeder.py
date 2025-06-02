from path import Path
from typing import Union, List, get_type_hints
from collections import defaultdict
from datetime import (datetime, timedelta)
import pandas as pd
import numpy as np
from dataclasses import asdict

from .base import BaseFeeder
from ..utils import (DatabaseConfig, DealWithSql, unify_time, select_date, transform_time_range, all_calendar)
from ..configer import read_config
from ..trader import (FutureInfo, StockInfo, IndexInfo, EtfInfo, OptionInfo, BarData, Interval, EdbInfo,
                      instrument_to_ticker, instrument_to_product, Asset, parse_ticker, parse_exchange)

LOCAL_DIR = Path(__file__).parent


class SqlServerFeeder(BaseFeeder):
    """
    Sql Server 数据库接口
    """

    def __init__(self, conn_cfg: DatabaseConfig, logger=None):
        if isinstance(conn_cfg, dict):
            conn_cfg = DatabaseConfig(**conn_cfg)
        super().__init__("SqlServer", conn_cfg, logger)
        self._api: Union[DealWithSql, None] = None

        self._edb: dict = {}

        self.on_init()

    def on_init(self):
        cfg = read_config(LOCAL_DIR / "config/database.yaml")
        self._edb = cfg["edb"]["map"]

    def connect(self):
        self._api = DealWithSql(self.conn_cfg)
        self._api.connect()

    def disconnect(self):
        if self._api is not None:
            self._api.disconnect()

    def check_login_status(self):
        if not self._api.check_connect():
            pass

    def create_table(self, asset: str, file: Union[list, tuple], context: dict = None):
        """创建数据库表格"""
        for file_ in file:
            file_dir = Path(__file__).parent.joinpath("sql", asset.lower(), f"{file_.lower().rstrip('.sql')}.sql")
            self._api.execute_sqlfile(file_dir, context)

    def _check_db_exist(self, db: str, table: str = None, schema: str = "dbo"):
        query = f"SELECT 1 FROM master.sys.databases WHERE name = '{db}'"
        res = not self._api.exec_query(query).empty

        if not res:
            self.ERROR(f"SqlServer数据库不存在： {db}")

        if res and table:
            query = f"SELECT 1 FROM [{db}].INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='{schema}' AND TABLE_NAME='{table}'"
            res = not self._api.exec_query(query).empty
            if not res:
                self.ERROR(f"SqlServer数据库表格不存在：{db}.{schema}.{table}")
        return res

    def dump_to_csv(self, quote: Union[pd.DataFrame, List], file_path: Union[str, Path]):
        quote = quote.copy()
        if isinstance(quote, list):
            quote = self.parse_bar_schema(quote).rename(columns={"Amount": "turnover", "OpenInterest": "open_interest"})
        quote["date"] = quote["DateTime"].map(lambda x: unify_time(x, fmt="int", mode=3))
        quote["time"] = quote["DateTime"].map(lambda x: x.strftime("%H:%M:%S"))
        quote["TradeDay"] = quote["TradeDay"].map(lambda x: unify_time(x, fmt="int", mode=3))
        quote["Interval"] = quote["Interval"].map(lambda x: Interval(x).value)
        quote.columns = quote.columns.str.lower()
        quote = quote.loc[:, ["date", "time"] + quote.columns.tolist()[1:-2]]

        tickers = quote[["ticker", "interval"]].apply(tuple, axis=1).unique()

        for ticker, interval in tickers:
            quote.to_csv(Path(file_path).joinpath(f"{ticker}_{interval}.csv"))
        self.INFO(f"行情数据已保存至本地csv: 共计{int(len(tickers))}个文件")

    def all_tradeday(self, start: Union[str, datetime, int], end: Union[str, datetime, int], fmt: str = "datetime",
                     freq: str = "D", day: int = 1, market: str = "China"):

        start_sql = unify_time(start, fmt="str", mode=3, dot="")
        end_sql = unify_time(end, fmt="str", mode=3, dot="")
        end_ = unify_time(end, mode=3)
        check_dt = unify_time(end_ - timedelta(days=20), fmt="str", mode=3)
        # 建立数据库连接并执行SQL查询
        query = f"""
                IF EXISTS(SELECT [Date] 
                          FROM [FUTURE_RESEARCH_DAILY].[dbo].[AssetTradeDate_UTCGMT8] 
                          WHERE [Date] >= '{check_dt}')
                SELECT [Date]
                FROM [FUTURE_RESEARCH_DAILY].[dbo].[AssetTradeDate_UTCGMT8] 
                WHERE [Date] >= '{start_sql}' AND [Date] <= '{end_sql}' 
                    AND [{market}] = 1
                ORDER BY [Date]
                """
        data = self._api.exec_query(query)
        # 检查SQL查询结果是否为空
        if data is None:
            self.ERROR("SQL Server 无法生成未来交易日")
            return

        if freq.upper() == "D":
            dts = sorted([unify_time(k, fmt=fmt, mode=3) for k in data["Date"].tolist()])
        else:
            dts = sorted([unify_time(k, fmt=fmt, mode=3) for k in select_date(data["Date"].tolist(), freq, day)])
        return dts

    def is_tradeday(self, day: Union[str, datetime, int], market: str = "China"):
        today = unify_time(day, fmt="str", mode=3)
        query = f"""
                SELECT [Date] FROM [FUTURE_RESEARCH_DAILY].[dbo].[AssetTradeDate_UTCGMT8]
                WHERE [Date] = '{today}' AND [{market}] = 1
                """
        data = self._api.exec_query(query)
        return not data.empty

    def get_tradeday(self, today: Union[datetime, str, int] = datetime.now(), n: int = 0, market: str = "China"):
        """
        获取指定日期n个交易日后的日期

        Args:
            today (Union[datetime, str, int], optional): 指定日期，可以是datetime对象、字符串或整数。默认为datetime.now()，即当前日期。
            n (int, optional): 指定日期后的n个交易日。如果n为正数，则返回指定日期后的第n个交易日；如果n为负数，则返回指定日期前的第abs(n)个交易日。默认为0。
            market (str, optional): 指定市场。默认为"China"，表示中国市场。
        Returns:
            datetime: 返回指定日期n个交易日后的日期。
        Raises:
            NotImplementedError: 如果未能找到指定数量的交易日数据，则抛出此异常。

        """
        day = unify_time(today, fmt="str", mode=3)
        query = f"""
                SELECT TOP({abs(n + int(n < 0)) + 1}) b.[Date]
                FROM (SELECT DATEADD(mi, 30, DATEADD(hh, 15, [Date])) [Date], [{market}]
                      FROM [FUTURE_RESEARCH_DAILY].[dbo].[AssetTradeDate_UTCGMT8]) b
                """
        if n >= 0:
            query += f"WHERE b.[Date] >= '{day}' AND [{market}] = 1 ORDER BY DATE"
        else:
            query += f"WHERE b.[Date] < '{day}' AND [{market}] = 1 ORDER BY DATE DESC"
        data = self._api.exec_query(query)
        if data.any:
            trade_day = data["Date"].tolist()[-1].to_pydatetime()
            return trade_day.replace(hour=0, minute=0, second=0, microsecond=0)
        else:
            raise NotImplementedError(f"未能找到{n}个交易日的数据")

    def trade_time(self, asset: str, ticker: Union[list, tuple] = None, call_auction: bool = True):
        """
        从数据库中获取交易时间数据。
        Args:
            asset (str): 资产类别
            ticker (list, optional): 合同列表，指定需要查询的合同。默认为None，表示查询所有合同。
            call_auction (bool, optional): 是否包含集合竞价时间。默认为True，表示包含集合竞价时间。
        Returns:
            dict: 返回一个字典，键为合同名称，值为包含每个市场交易时间的字典。
        """
        # SQL查询语句，用于从数据库中获取交易时间数据
        query = f"""
                SELECT [Product], [Day], [Night]
                FROM [{asset.upper()}_RESEARCH_DAILY].[dbo].[AssetTradeTime_UTCGMT8]
                """
        # 如果传入了contract参数，则在SQL查询语句中添加WHERE子句，限定合同范围
        if ticker is not None:
            product = [k.split(".")[1].upper() if int(len(k.split("."))) > 1 else k for k in ticker]
            query += f" WHERE [Product] in ({str([k.upper().split('_')[0] for k in product])[1:-1]})"""

        # 执行SQL查询，并将结果转换为字典格式
        data = self._api.exec_query(query).set_index("Product").applymap(eval).T.to_dict()
        # 初始化字典，用于存储每个合同的交易时间
        trade_time = {k: {} for k in data}
        # 遍历查询结果，对每个合同的每个市场进行遍历
        for (k, v) in data.items():
            for (mkt, t_rng) in v.items():
                # 如果交易时间范围为空，则跳过
                if not t_rng:
                    continue
                # 将交易时间范围转换为所需的格式，并存储在trade_time字典中
                trade_time[k][mkt] = transform_time_range(t_rng, call_auction)
        return trade_time

    def min_bar_product(self, asset: str, ticker: list):
        """获取每分钟线有数据的交易品种"""
        # 根据原始品种代码获取品种交易时间
        product_raw = [k.split(".")[1].upper() if int(len(k.split("."))) > 1 else k for k in ticker]
        trade_time = self.trade_time(asset, product_raw, False)

        minbar_product = defaultdict(list)
        for (product, td_t) in trade_time.items():
            t_intervals = [i for k in td_t.values() for i in k]
            min_bar = [i if i < 86400 else i - 86400 for (s, e) in t_intervals for i in range(s + 60, e + 1, 60)]
            for t in min_bar:
                minbar_product[int(t)].append(product)
        minbar_product = dict(sorted(minbar_product.items(), key=lambda x: x[0]))
        return minbar_product

    def interval_tradetime(self, asset: str, interval: str, ticker: list = None):
        """获取每个品种的指定周期的Bar结束时间"""
        interval = Interval(interval)
        interval_t = {}
        itv_sec = interval.window * 60
        # Get all trade time
        trade_time = self.trade_time(asset, ticker, call_auction=False)
        for (product, mkt_t) in trade_time.items():
            t_rng = mkt_t.get("Night", []) + mkt_t["Day"]
            product_t = []
            res = 0
            for rng in t_rng:
                curr_t = rng[0] + itv_sec - res
                while curr_t <= rng[1]:
                    product_t.append(curr_t)
                    curr_t += itv_sec
                res = rng[1] - product_t[-1]
            if res != 0:
                product_t.append(t_rng[-1][-1])
            interval_t[product] = product_t
        return interval_t

    def asset_ticker(self, asset: str, active: bool):
        asset = asset.lower()
        if asset not in ["future", "stock", "etf", "index", "option"]:
            self.ERROR("资产类型无法识别：仅支持future（期货）/stock（股票）/etf（ETF）/index（指数）/option（期权）")
            return
        if asset in ("etf", "index"):
            tickers = eval(f"self.{asset}_ticker")()
        else:
            tickers = eval(f"self.{asset}_ticker")(active)
        self.INFO(f"{asset.capitalize()}资产{'活跃' if active else '全部'}合约获取完成: {int(len(tickers))}条记录")
        return tickers

    def future_ticker(self, active: bool):
        query = f"SELECT DISTINCT [Instrument], [Exchange], [DelistDate] FROM [FUTURE_RESEARCH_DAILY].[dbo].[FUTUREINFO_BASIC]"
        if active:
            today = unify_time(datetime.now(), fmt="str", mode=3, dot="")
            query += f" WHERE [DelistDate] >= '{today}'"
        data = self._api.exec_query(query)
        data = data.to_dict(orient="records")
        data = [instrument_to_ticker("future", k["Exchange"], k["Instrument"], k["DelistDate"]) for k in data]
        return sorted(data)

    def stock_ticker(self, active: bool):
        query = f"SELECT DISTINCT [Ticker],[Exchange] FROM [STOCK_RESEARCH_DAILY].[dbo].[STOCKINFO_BASIC]"
        if active:
            query += " WHERE [Status] = 'Listed'"
        data = self._api.exec_query(query)
        mask = data["Exchange"].isna()
        data.loc[mask, "Exchange"] = data.loc[mask, "Ticker"].map(lambda x: parse_exchange("stock", x))
        data = data.to_dict(orient="records")
        data = [instrument_to_ticker("stock", k["Exchange"], k["Ticker"]) for k in data]
        return sorted(data)

    def index_ticker(self):
        query = f"SELECT DISTINCT [Ticker], [Exchange] FROM [INDEX_RESEARCH_DAILY].[dbo].[INDEXINFO_BASIC]"
        data = self._api.exec_query(query)
        data = data.to_dict(orient="records")
        data = [instrument_to_ticker("index", k["Exchange"], k["Ticker"]) for k in data]
        return sorted(data)

    def etf_ticker(self):
        query = f"SELECT DISTINCT [Ticker], [Exchange] FROM [ETF_RESEARCH_DAILY].[dbo].[ETFINFO_BASIC]"
        data = self._api.exec_query(query)
        data = data.to_dict(orient="records")
        data = [instrument_to_ticker("etf", k["Exchange"], k["Ticker"]) for k in data]
        return sorted(data)

    def option_ticker(self, active: bool):
        query = (f"SELECT DISTINCT [Instrument], [Exchange], [DelistDate]"
                 f" FROM [OPTION_RESEARCH_DAILY].[dbo].[OPTIONINFO_BASIC]")
        if active:
            today = unify_time(datetime.now(), fmt="str", mode=3, dot="")
            query += f" WHERE [DelistDate] >= '{today}'"
        data = self._api.exec_query(query)
        data = data.to_dict(orient="records")
        data = [instrument_to_ticker("option", k["Exchange"], k["Instrument"], k["DelistDate"]) for k in data]
        return sorted(data)

    def asset_info(self, asset: str, info: str = "basic", ticker: list = None,
                   underlying: list = None, use_schema: bool = False):
        asset = asset.lower()
        info = info.lower()

        if info == "basic":
            if asset in ["stock", "etf", "index", "future"]:
                info_data = eval(f"self.{asset}_info_basic")(ticker)
            elif asset == "option":
                info_data = eval(f"self.{asset}_info_basic")(ticker, underlying)
            else:
                self.ERROR("资产类型无法识别：仅支持future（期货）/stock（股票）/etf（ETF）/index（指数）/option（期权）")
                return
        elif info == "trade":
            if asset == "future":
                info_data = eval(f"self.{asset}_info_trade")(ticker)
            else:
                self.ERROR("交易信息仅适用于期货和期权")
                return
        else:
            self.ERROR("信息类型无法识别：仅支持basic（基础合约信息）/trade（合约交易信息）")
            return
        self.INFO(f"{asset.capitalize()}资产{info.capitalize()}信息获取完成: {int(len(info_data))}条记录")
        if use_schema and info == "basic":
            info_data = self.create_info_schema(asset, info_data)

        return info_data

    def future_info_basic(self, ticker: list = None):
        query = """
                SELECT [Ticker], [Instrument] AS [Instrument], [Product], [Exchange], 
                       [ListDate], [DelistDate], [DeliverDate], 
                       [Multiplier], [PriceTick], [MinMargin], [PriceLimit],
                       [QuoteUnit], [MultiplierUnit], [ClassLevel1], [ClassLevel2]
                FROM [FUTURE_RESEARCH_DAILY].[dbo].[FUTUREInfo_Basic]
                """
        if ticker:
            product_lst, ticker_lst = parse_ticker("future", ticker)
            query_all = []
            if product_lst:
                query_all.append(
                    query + f" WHERE [Product] in ({str(set([x.split('_')[0] for x in product_lst]))[1:-1]})")
            if ticker_lst:
                query_all.append(query + f" WHERE [Ticker] in ({str(set(ticker_lst))[1:-1]})")
            query = " UNION ALL ".join(query_all)
        data = self._api.exec_query(query)
        return data

    def stock_info_basic(self, ticker: list = None):
        query = """
                SELECT [Ticker], [Name],[RegisteredCapital], [Representative], [EstablishDate],
                        [ListDate], [Exchange], [Board], [TotalShares], [TotalTradeableShares], [MainBusiness],
                        [Status], [SWClassLevel1], [SWClassLevel2], [SWClassLevel3]
                        FROM [STOCK_RESEARCH_DAILY].[dbo].[StockInfo_Basic]
                """
        if ticker:
            ticker_lst = parse_ticker("stock", ticker)[1]
            query += f"WHERE [Ticker] in ({str(set(ticker_lst))[1:-1]})"
        data = self._api.exec_query(query)
        # 交易所信息缺失
        mask = data["Exchange"].isna()
        data.loc[mask, "Exchange"] = data.loc[mask, "Ticker"].map(lambda x: parse_exchange("stock", x))
        return data

    def etf_info_basic(self, ticker: list = None):
        query = """
                SELECT [Ticker], [Name], [EstablishDate],[ListDate],[Exchange],[Style],[Target],
                        [TotalIssuedShares],[BenchMark],[TradeSymbol]  
                FROM [ETF_RESEARCH_DAILY].[dbo].[ETFInfo_Basic]
                """
        if ticker:
            ticker_lst = parse_ticker("etf", ticker)[1]
            query += f"WHERE [Ticker] in ({str(set(ticker_lst))[1:-1]})"
        data = self._api.exec_query(query)
        # 交易所信息缺失
        mask = data["Exchange"].isna()
        data.loc[mask, "Exchange"] = data.loc[mask, "Ticker"].map(lambda x: parse_exchange("stock", x))
        return data

    def index_info_basic(self, ticker: list = None):
        query = """
                SELECT [Ticker], [Name], [Exchange], [EstablishDate],[ListDate],[BeginPoint],[SampleSize],
                        [IndexClass1],[IndexClass2],[IndexClass3] 
                FROM [INDEX_RESEARCH_DAILY].[dbo].[IndexInfo_Basic]
                """
        if ticker:
            ticker_lst = parse_ticker("index", ticker)[1]
            query += f"WHERE [Ticker] in ({str(set(ticker_lst))[1:-1]})"
        data = self._api.exec_query(query)
        return data

    def option_info_basic(self, ticker: list = None, underlying: list = None):
        query = """
                SELECT [Ticker],[Instrument], [Product], [Underlying],[UnderlyingClass],
                        [Adjusted],[ExerciseType],[CallorPut],[Multiplier],[Strike],[ListDate],[DelistDate],[ExpiryDate],
                        [PriceTick],[Exchange],[ExecDate]
                  FROM [OPTION_RESEARCH_DAILY].[dbo].[OptionInfo_Basic]
                """
        if ticker:
            ticker_lst = parse_ticker("option", ticker)[1]
            query += f" WHERE [Ticker] in ({str(set(ticker_lst))[1:-1]})"
        elif underlying:
            query += f" WHERE [Underlying] in ({str(set(underlying))[1:-1]})"
        data = self._api.exec_query(query)
        return data

    def future_info_trade(self, ticker: list = None):
        query = """
                SELECT [Ticker],[Instrument],[Product],[ExecDate],[LongMargin],[ShortMargin],
                    [OpenCommission_Pct],[OpenCommission_Fix],[CloseCommission_Pct],[CloseCommission_Fix],
                    [IntradayOpenCommission_Pct],[IntradayOpenCommission_Fix],[IntradayCloseCommission_Pct],
                    [IntradayCloseCommission_Fix] ,[Exchange]
                FROM [FUTURE_RESEARCH_DAILY].[dbo].[FUTUREInfo_Trade]
                """
        if ticker:
            product_lst, ticker_lst = parse_ticker("future", ticker)
            query_all = []
            if product_lst:
                query_all.append(
                    query + f" WHERE [Product] in ({str(set([x.split('_')[0] for x in product_lst]))[1:-1]})")
            if ticker_lst:
                query_all.append(query + f" WHERE [Ticker] in ({str(set(ticker_lst))[1:-1]})")
            query = " UNION ALL ".join(query_all)
        data = self._api.exec_query(query)
        return data

    def create_info_schema(self, asset: str, info: pd.DataFrame):
        asset = asset.lower()

        if asset == "future":
            # 补充交易信息数据
            if "OpenCommission_Pct" not in info.columns:
                trade_info = self.future_info_trade(ticker=info["Ticker"].unique().tolist())
                trade_info = trade_info.sort_values("ExecDate").groupby("Instrument", as_index=False).tail(1)
                trade_info = trade_info.loc[:, ["Ticker", "Product", "ExecDate", "OpenCommission_Pct",
                                                "OpenCommission_Fix", "CloseCommission_Pct", "CloseCommission_Fix",
                                                "IntradayCloseCommission_Pct", "IntradayCloseCommission_Fix"]]
                fill = trade_info.sort_values("ExecDate").groupby("Product", as_index=False).tail(1).set_index(
                    "Product")
                col_to_fill = ["OpenCommission_Pct", "OpenCommission_Fix", "CloseCommission_Pct",
                               "CloseCommission_Fix", "IntradayCloseCommission_Pct", "IntradayCloseCommission_Fix"]
                info = pd.merge(info, trade_info.drop(columns=["Product"]), on="Ticker", how="left")
                for col in col_to_fill:
                    # 按行填补：若缺失，则根据该行的 Product 从 fill_data 中取值
                    info[col] = info.apply(
                        lambda row: fill.loc[row["Product"], col] if pd.isna(row[col]) else row[col],
                        axis=1
                    )
                info = info.reset_index(drop=True)
            info.columns = ["ticker", "instrument", "product", "exchange", "listdate", "delistdate", "deliverdate",
                            "multiplier", "pricetick", "margin", "pricelimit", "quoteunit", "multiplierunit",
                            "class_level1", "class_level2", "execdate", "comm_open_pct", "comm_open_fix",
                            "comm_close_pct", "comm_close_fix", "comm_closetoday_pct", "comm_closetoday_fix"]
            info["asset"] = "future"
            dt_cols = ["listdate", "delistdate", "deliverdate"]
            info[dt_cols] = info[dt_cols].map(lambda x: x.strftime("%Y%m%d")).astype(int)
            info_list = info.fillna("").to_dict(orient="records")
            info_list = [FutureInfo(**k) for k in info_list]
            return {k.ticker: k for k in info_list}
        elif asset == "stock":
            info = info.loc[:, ["Ticker", "Name", "EstablishDate", "ListDate", "Exchange", "TotalShares",
                                "TotalTradeableShares", "Status", "SWClassLevel1", "SWClassLevel2",
                                "SWClassLevel3"]].rename(columns={"Ticker": "Instrument",
                                                                  "TotalTradeableShares": "flowshare",
                                                                  "TotalShares": "allshare",
                                                                  "SWClassLevel1": "sw_class_level1",
                                                                  "SWClassLevel2": "sw_class_level2",
                                                                  "SWClassLevel3": "sw_class_level3"})
            info["asset"] = "stock"
            dt_cols = ["EstablishDate", "ListDate"]
            info[dt_cols] = info[dt_cols].map(lambda x: x.strftime("%Y%m%d")).astype(int)
            info.columns = info.columns.str.lower()
            info_list = info.fillna("").to_dict(orient="records")
            info_list = [StockInfo(**k) for k in info_list]
            return {k.ticker: k for k in info_list}
        elif asset == "index":
            info["asset"] = "index"
            info["ListDate"] = info["ListDate"].map(lambda x: x.strftime("%Y%m%d")).astype(int)
            info.columns = info.columns.str.lower()
            info = info.rename(
                columns={"indexclass1": "class_level1", 'indexclass2': "class_level2", "ticker": "instrument"})
            info_list = info.fillna("").to_dict(orient="records")
            info_list = [IndexInfo(**k) for k in info_list]
            return {k.ticker: k for k in info_list}
        elif asset == "etf":
            info["asset"] = "etf"
            info["ListDate"] = info["ListDate"].map(lambda x: x.strftime("%Y%m%d")).astype(int)
            info.columns = info.columns.str.lower()
            info = info.rename(columns={"totalissuedshares": "allshares", "ticker": "instrument"})
            info_list = info.fillna("").to_dict(orient="records")
            info_list = [EtfInfo(**k) for k in info_list]
            return {k.ticker: k for k in info_list}
        elif asset == "option":
            info.columns = info.columns.str.lower()
            info["asset"] = "option"
            info["type"] = info["type"].map({"P": "put", "C": "call"})
            info = info.rename(columns={"callorput": "type"})
            dt_cols = ["listdate", "delistdate", "deliverdate"]
            info[dt_cols] = info[dt_cols].map(lambda x: x.strftime("%Y%m%d")).astype(int)
            info_list = info.fillna("").to_dict(orient="records")
            info_list = [OptionInfo(**k) for k in info_list]
            return {k.ticker: k for k in info_list}

    def asset_main_ticker(self, asset: str, start: Union[datetime, str, int], end: Union[datetime, str, int],
                          ticker: Union[list, tuple] = None, underlying: Union[list, tuple] = None):
        asset = asset.lower()

        if asset == "future":
            func = eval(f"self.{asset}_main_ticker")
            return func(start, end, ticker)
        elif asset in ("stock", "index", "etf"):
            self.ERROR("股票/指数/ETF不支持主连行情")
            return
        elif asset == "option":
            self.ERROR("期权主连合约功能暂不支持")
            return

    def future_main_ticker(self, start: Union[datetime, str, int], end: Union[datetime, str, int],
                           ticker: Union[list, tuple] = None):
        # 统一日期格式
        start_ = unify_time(start, fmt="str", mode=3, dot="")
        end_ = unify_time(end, fmt="str", mode=3, dot="")
        # 解析品种列表
        product = [k for k in parse_ticker("future", ticker)[0] if "_" in k]
        raw_product = [k.rstrip("_HOT") for k in set(product) if k.endswith("HOT")]
        calendar_product = [k for k in set(product) if not k.endswith("HOT")]

        query_raw = f"""
                     SELECT [TradeDay], [Product] + '.' + 'HOT' AS [Product], [Ticker]
                     FROM [FUTURE_RESEARCH_DAILY].[dbo].[FUTUREMainTicker]
                     WHERE [TradeDay] >= '{start_}' AND [TradeDay] <= '{end_}'
                     {f'AND [Product] IN ({str(raw_product)[1:-1]})' if raw_product else ''}
                     """
        query_calendar = f"""
                          SELECT [TradeDay], [Product] + '.' + [Calendar] AS [Product], [Ticker]
                          FROM [FUTURE_RESEARCH_DAILY].[dbo].[FUTUREMainTicker_Calendar]
                          WHERE [TradeDay] >= '{start_}' AND [TradeDay] <= '{end_}'
                          {f"AND [Product] + '_' + [Calendar] IN ({str(calendar_product)[1:-1]})"
        if calendar_product else ''} 
                          """

        if (raw_product and calendar_product) or (not ticker):
            data = self._api.exec_query(query_raw + " UNION ALL " + query_calendar + "ORDER BY [TradeDay]")
        elif raw_product:
            data = self._api.exec_query(query_raw + "ORDER BY [TradeDay]")
        elif calendar_product:
            data = self._api.exec_query(query_calendar + "ORDER BY [TradeDay]")
        else:
            self.ERROR("目标品种列表数据有误，请检查输入")
            return

        info = self.future_info_basic(ticker=ticker).set_index("Ticker")
        exchange = info["Exchange"].to_dict()
        instrument = info["Instrument"].to_dict()
        data["Instrument"] = data["Ticker"].map(instrument)
        data["Exchange"] = data["Ticker"].map(exchange)
        data["Product_"] = data["Instrument"].map(lambda x: instrument_to_product("future", x))
        data["Product"] = data["Exchange"] + "." + data["Product_"] + "." + data["Product"].map(
            lambda x: x.split('.')[1])

        return data.loc[:, ["TradeDay", "Product", "Ticker", "Instrument"]]

    def asset_quote(self, asset: str, start: Union[datetime, str, int], end: Union[datetime, str, int],
                    interval: Union[str, int], ticker: Union[list, tuple] = None, use_schema: bool = False):
        asset = asset.lower()
        try:
            interval = Interval(interval)
        except ValueError:
            self.ERROR(f"行情周期无法识别: {interval}")
            return

        if interval.pattern == "day":
            arg = (start, end)
            func = eval(f"self.{asset}_quote_daily")
        elif interval.pattern == "minute":
            arg = (start, end, f"{interval.window}min")
            func = eval(f"self.{asset}_quote_min")
        else:
            self.ERROR(f"行情周期数据不支持: {interval}")
            return

        quote = func(*arg, ticker).dropna(subset=["DateTime"])
        self.INFO(f"{asset.capitalize()}资产{interval.value}行情数据获取完成: {int(len(quote))}条记录")

        quote = quote.drop_duplicates(subset=["DateTime", "Instrument"])
        if use_schema:
            quote = self.create_bar_schema(asset, interval.value, quote)

        return quote

    def future_quote_daily(self, start: Union[datetime, str, int], end: Union[datetime, str, int],
                           ticker: Union[list, tuple] = None):
        # 统一日期格式
        start_ = unify_time(start, fmt="str", mode=3, dot="")
        end_ = unify_time(end, fmt="str", mode=3, dot="")
        # 解析品种列表
        product, ticker_lst = parse_ticker("future", ticker)
        raws = [k for k in set(product) if not "_" in k]
        hots = [k for k in set(product) if k not in raws]

        query_all = []
        # 先取原始数据
        if ticker_lst or raws or not ticker:
            query = f"""
                    SELECT [DateTime], [TradeDay], [Ticker], %s AS [Product],
                            [Open], [High], [Low], [Close], [PrevClose], [Settle], [PrevSettle],
                            [Volume], [Amount], [OpenInterest], 1.0 AS [CoefAdj]
                    FROM [FUTURE_RESEARCH_DAILY].[dbo].[FUTUREQUOTE_DAILY] a
                    WHERE [TradeDay] >= '{start_}' AND [TradeDay] <= '{end_}'
                    """
            if not ticker:
                # 未指定ticker，取全部合约数据
                query_all.append(query % '[Product]')
                # 未指定合约，取全部主力合约数据
                query_1 = f"""
                        SELECT DISTINCT [Product] FROM [FUTURE_RESEARCH_DAILY].[dbo].[FutureCoefAdj_MainTicker] 
                                WHERE [TradeDay] >= '{start_}' AND [TradeDay] <= '{end_}'
                        UNION ALL
                        SELECT DISTINCT [Product] FROM [FUTURE_RESEARCH_DAILY].[dbo].[FutureCoefAdj_Calendar] 
                                WHERE [TradeDay] >= '{start_}' AND [TradeDay] <= '{end_}'
                        """
                product = self._api.exec_query(query_1)["Product"].unique().tolist()
                hots = [k + "_HOT" if "_" not in k else k for k in product]
            else:
                if raws:
                    query_all.append(query % "[Product]" + f" AND [Product] in ({str(raws)[1:-1]})")
                if ticker_lst:
                    query_all.append(query % "[Ticker]" + f" AND [Ticker] in ({str(ticker_lst)[1:-1]})")

        # 取主力合约数据
        if hots:
            raw_hots = [k.rstrip("_HOT") for k in hots if k.endswith("_HOT")]
            calen_hots = [k for k in hots if not k.endswith("_HOT")]
            query = f"""
                    SELECT [DateTime], b.[TradeDay], b.[Ticker], b.[Product] + %s AS [Product], [Open], [High], [Low],
                            [Close], [PrevClose], [Settle], [PrevSettle], [Volume], [Amount], [OpenInterest],
                            b.[CoefAdj]
                     FROM [FUTURE_RESEARCH_DAILY].[dbo].[FUTUREQuote_DAILY] a
                     INNER JOIN 
                        (SELECT [TradeDay],[Product],[Ticker], CAST(EXP(SUM(LOG(CoefAdj + 1)) 
                                OVER (PARTITION BY Product ORDER BY TradeDay)) AS DECIMAL(18, 4)) AS [CoefAdj]
                         FROM [FUTURE_RESEARCH_DAILY].[dbo].[%s]
                         WHERE [TradeDay] >= '{start_}' AND [TradeDay] <= '{end_}' 
                            AND [Product] IN (%s)
                         ) b
                     ON a.[TradeDay] = b.[TradeDay] AND a.[Ticker] = b.[Ticker]
                     """
            # 原始主力合约
            if raw_hots:
                query_all.append(query % ("'.HOT'", "FutureCoefAdj_MainTicker", f"{str(raw_hots)[1:-1]}"))
            # 跨期主力合约
            if calen_hots:
                query_all.append(query % ("''", "FutureCoefAdj_Calendar", f"{str(calen_hots)[1:-1]}"))

        if query_all:
            query = " UNION ALL ".join(query_all)
            query = f"""
                    SELECT [DateTime], [TradeDay], x.[Product] AS [Ticker], y.[Product], y.[Exchange], 
                            y.[Instrument], [Open], [High], [Low],
                            [Close], [PrevClose], [Settle], [PrevSettle], [Volume], [Amount], [OpenInterest], [CoefAdj]
                    FROM ({query}) x
                    LEFT JOIN (SELECT [Ticker], [Product], [Exchange], [Instrument]
                               FROM [FUTURE_RESEARCH_DAILY].[dbo].[FutureInfo_Basic]
                               ) y
                    on x.[Ticker] = y.[Ticker]
                    ORDER BY [DateTime]
                    """
            data = self._api.exec_query(query)
            # 调整数据格式
            data["Ticker"] = data["Ticker"].map(lambda x: x.replace("_", "."))
            data["Ticker"] = data["Ticker"].map(
                lambda x: x[:-4] + "." + x[-4:] if x[-4:].isdigit() and x[:-4].isalpha() else x)
            data["Ticker"] = data["Ticker"].map(lambda x: x.split(".")[-1] if "." in x else "")
            data["Ticker"] = (
                        data["Exchange"] + "." + data["Instrument"].map(lambda x: instrument_to_product("future", x))
                        + "." + data["Ticker"]).map(lambda x: x.rstrip("."))
            return data
        else:
            self.ERROR(f"期货日线数据查询失败: {ticker}")

    def future_quote_min(self, start: Union[datetime, str, int], end: Union[datetime, str, int], interval: str,
                         ticker: Union[list, tuple] = None):
        # 统一日期格式
        start_ = unify_time(start, fmt="str", mode=3, dot="")
        end_ = unify_time(end, fmt="str", mode=3, dot="")
        # 解析品种列表
        product, ticker_lst = parse_ticker("future", ticker)
        raws = [k for k in set(product) if not "_" in k]
        hots = [k for k in set(product) if "_" in k]

        if not self._check_db_exist(db=f"FUTURE_RESEARCH_{interval}"):
            self.INFO(f"[{self._name}] [INFO] 期货{interval}数据库不存在，切换为主动合成")
            return self._future_bar_generator(start_, end_, interval, ticker_lst, raws, hots)

        query_all = []
        # 先取原始数据
        if ticker_lst or raws or not ticker:
            query = f"""
                    SELECT [DateTime], [TradeDay], [Ticker], %s AS [Product], [Open], [High], [Low],
                            [Close], [PrevClose], [Settle], [PrevSettle], [Volume], [Amount], [OpenInterest], 1.0 AS [CoefAdj]
                    FROM [FUTURE_RESEARCH_{interval}].[dbo].[%s]
                    WHERE [TradeDay] >= '{start_}' AND [TradeDay] <= '{end_}'
                    """
            # 未指定合约：取所有原始合约和主力合约
            if not ticker:
                # 取原始合约
                product = self.asset_info("future")["Product"].unique().tolist()
                for product_ in product:
                    query_all.append(query % (f"'{product_.upper()}'", product_.upper()))
                # 取全部主力合约
                query_1 = f"""
                        SELECT DISTINCT [Product] FROM [FUTURE_RESEARCH_DAILY].[dbo].[FutureCoefAdj_MainTicker] 
                                WHERE [TradeDay] >= '{start_}' AND [TradeDay] <= '{end_}'
                        UNION ALL
                        SELECT DISTINCT [Product] FROM [FUTURE_RESEARCH_DAILY].[dbo].[FutureCoefAdj_Calendar] 
                                WHERE [TradeDay] >= '{start_}' AND [TradeDay] <= '{end_}'
                        """
                product = self._api.exec_query(query_1)["Product"].unique().tolist()
                hots = [k + "_HOT" if "_" not in k else k for k in product]
            # 指定合约中包含原始品种
            if raws:
                for product_ in raws:
                    query_all.append(query % (f"'{product_.upper()}'", product_.upper()))
            # 指定合约中包含原始合约
            if ticker_lst:
                product_lst = defaultdict(list)
                for ticker_ in ticker_lst:
                    product_ = instrument_to_product("future", ticker_)
                    product_lst[product_].append(ticker_)
                for product_, tickers in product_lst.items():
                    query_all.append(query % ("[Ticker]", product_) + f" AND [Ticker] in ({str(set(tickers))[1:-1]})")
        # 取主力合约数据
        if hots:
            product_dict = defaultdict(list)
            for product_ in hots:
                product_dict[product_.split('_')[0].upper()].append(product_.upper())

            for table, product_lst in dict(product_dict).items():
                query_ = f"""
                        SELECT [DateTime], b.[TradeDay], b.[Ticker], b.[Product] + '%s' AS [Product], [Open], [High], [Low], [Close],
                                [PrevClose], [Settle], [PrevSettle], [Volume], [Amount], [OpenInterest], b.[CoefAdj]
                         FROM [FUTURE_RESEARCH_{interval}].[dbo].[{table}] a
                         INNER JOIN (SELECT [TradeDay], [Product], [Ticker], CAST(EXP(SUM(LOG(CoefAdj + 1)) 
                                        OVER (PARTITION BY Product ORDER BY TradeDay)) AS DECIMAL(18, 4)) AS [CoefAdj]
                                     FROM [FUTURE_RESEARCH_DAILY].[dbo].[%s]
                                     WHERE [TradeDay] >= '{start_}' AND [TradeDay] <= '{end_}' AND [Product] IN (%s)) b
                         ON a.[TradeDay] = b.[TradeDay] AND a.[Ticker] = b.[Ticker]
                         """
                raw_hot = [k for k in product_lst if k.endswith("_HOT")]
                calen_hot = [k for k in product_lst if k not in raw_hot]
                if raw_hot:
                    query_all.append(query_ % ("_HOT", "FutureCoefAdj_MainTicker", f"'{table}'"))
                if calen_hot:
                    query_all.append(query_ % ("", "FutureCoefAdj_Calendar", f"{str(calen_hot)[1:-1]}"))

        if query_all:
            query = " UNION ALL ".join(query_all)
            query = f"""
                    SELECT [DateTime], [TradeDay], x.[Product] AS [Ticker], y.[Product], y.[Exchange], 
                            y.[Instrument], [Open], [High], [Low],
                            [Close], [PrevClose], [Settle], [PrevSettle], [Volume], [Amount], [OpenInterest], [CoefAdj]
                    FROM ({query}) x
                    LEFT JOIN (SELECT [Ticker], [Product], [Exchange], [Instrument]
                               FROM [FUTURE_RESEARCH_DAILY].[dbo].[FutureInfo_Basic]
                               ) y
                    on x.[Ticker] = y.[Ticker]
                    ORDER BY [DateTime]
                    """
            data = self._api.exec_query(query)
            # 调整数据格式
            data["Ticker"] = data["Ticker"].map(lambda x: x.replace("_", "."))
            data["Ticker"] = data["Ticker"].map(
                lambda x: x[:-4] + "." + x[-4:] if x[-4:].isdigit() and x[:-4].isalpha() else x)
            data["Ticker"] = data["Ticker"].map(lambda x: x.split(".")[-1] if "." in x else "")
            data["Ticker"] = (
                        data["Exchange"] + "." + data["Instrument"].map(lambda x: instrument_to_product("future", x))
                        + "." + data["Ticker"]).map(lambda x: x.rstrip("."))
            return data
        else:
            self.ERROR(f"期货数据库不存在: {interval} | {ticker}")

    def _future_bar_generator(self, start: str, end: str, interval: str, ticker_lst: list, raws: list, hots: list):
        """期货灵活K线生成"""
        raw_hot = [k.rstrip("_HOT") for k in hots if k.endswith("_HOT")]
        calen_hot = [k for k in hots if not k.endswith("_HOT")]

        query = f"""
                DECLARE @ProductList NVARCHAR(MAX) = N'{','.join(raw_hot)}';
                DECLARE @CalenProductList NVARCHAR(MAX) = N'{','.join(calen_hot)}';
                DECLARE @TickerList NVARCHAR(MAX) = N'{','.join(ticker_lst)}';
                DECLARE @RawProductList NVARCHAR(MAX) = N'{','.join(raws)}';
                DECLARE @StartDay DATE = '{start}';
                DECLARE @EndDay DATE = '{end}';
                DECLARE @WindowSize INT = {Interval(interval).window};

                DECLARE @PrevDay DATE = (
                    SELECT MAX([Date])
                    FROM (
                        SELECT [Date]
                        FROM [FUTURE_RESEARCH_DAILY].[dbo].[AssetTradeDate_UTCGMT8]
                        WHERE [Date] < @StartDay AND [China] = 1
                    ) x
                );
                IF @PrevDay IS NULL SET @PrevDay = @StartDay;

                DECLARE @baseDataSourceSql NVARCHAR(MAX) = N'';
                DECLARE @baseDataDelimiter NVARCHAR(100) = N' UNION ALL ';

                SELECT @baseDataSourceSql = STRING_AGG(
                    N'SELECT [DateTime], [TradeDay], [Ticker], [Open], [High], [Low], [Close], [PrevClose],
                             [Volume], [Amount], [OpenInterest]
                      FROM [FUTURE_RESEARCH_1MIN].[dbo].' + QUOTENAME(
                        CASE 
                            WHEN CHARINDEX('_', value) > 0 THEN LEFT(value, CHARINDEX('_', value) - 1)
                            ELSE value
                        END
                    ) + '
                      WHERE TradeDay BETWEEN @PrevDay AND @EndDay',
                    @baseDataDelimiter
                )
                FROM (
                    SELECT value FROM STRING_SPLIT(@ProductList, ',') WHERE LTRIM(RTRIM(value)) <> ''
                    UNION
                    SELECT value FROM STRING_SPLIT(@CalenProductList, ',') WHERE LTRIM(RTRIM(value)) <> ''
                    UNION
                    SELECT DISTINCT
                        CASE 
                            WHEN CHARINDEX('_', Product) > 0 THEN LEFT(Product, CHARINDEX('_', Product) - 1)
                            ELSE Product
                        END
                    FROM [FUTURE_RESEARCH_DAILY].[dbo].[FutureQuote_Daily]
                    WHERE ( (@TickerList IS NOT NULL AND @TickerList <> '' AND Ticker IN (SELECT value FROM STRING_SPLIT(@TickerList, ',')))
                         OR (@RawProductList IS NOT NULL AND @RawProductList <> '' AND Product IN (SELECT value FROM STRING_SPLIT(@RawProductList, ','))) )
                      AND TradeDay BETWEEN @PrevDay AND @EndDay
                ) AS AllInput;

                IF @baseDataSourceSql IS NULL OR @baseDataSourceSql = N''
                BEGIN
                    PRINT N'错误: 无可用行情表。';
                    RETURN;
                END

                DECLARE @sql NVARCHAR(MAX) = N'
                WITH MainTickers AS (
                    SELECT [TradeDay], [Ticker], (Product + ''.HOT'') AS [Product], CAST(EXP(SUM(LOG(CoefAdj + 1)) 
                             OVER (PARTITION BY Product ORDER BY TradeDay)) AS DECIMAL(18, 4)) AS [CoefAdj]
                        FROM [FUTURE_RESEARCH_DAILY].[dbo].[FutureCoefAdj_MainTicker]
                        WHERE (@ProductList IS NOT NULL AND @ProductList <> '''')
                            AND [Product] IN (SELECT value FROM STRING_SPLIT(@ProductList, '',''))
                            AND [TradeDay] BETWEEN @StartDay AND @EndDay
                    UNION ALL
                    SELECT [TradeDay], [Ticker], [Product], CAST(EXP(SUM(LOG(CoefAdj + 1)) 
                           OVER (PARTITION BY Product ORDER BY TradeDay)) AS DECIMAL(18, 4)) AS [CoefAdj]
                        FROM [FUTURE_RESEARCH_DAILY].[dbo].[FutureCoefAdj_Calendar]
                        WHERE (@CalenProductList IS NOT NULL AND @CalenProductList <> '''')
                          AND [Product] IN (SELECT value FROM STRING_SPLIT(@CalenProductList, '',''))
                          AND [TradeDay] BETWEEN @StartDay AND @EndDay
                    UNION ALL
                    SELECT [TradeDay], [Ticker], [Ticker] AS [Product], 1.0 AS [CoefAdj]
                        FROM [FUTURE_RESEARCH_DAILY].[dbo].[FutureQuote_Daily]
                        WHERE (@TickerList IS NOT NULL AND @TickerList <> '''')
                          AND [Ticker] IN (SELECT value FROM STRING_SPLIT(@TickerList, '',''))
                          AND [TradeDay] BETWEEN @StartDay AND @EndDay
                    UNION ALL
                    SELECT [TradeDay], [Ticker], [Product], 1.0 AS [CoefAdj]
                        FROM [FUTURE_RESEARCH_DAILY].[dbo].[FutureQuote_Daily]
                        WHERE (@RawProductList IS NOT NULL AND @RawProductList <> '''')
                          AND [Product] IN (SELECT value FROM STRING_SPLIT(@RawProductList, '',''))
                          AND [TradeDay] BETWEEN @StartDay AND @EndDay
                ),
                BaseData AS (
                    ' + @baseDataSourceSql + '
                ),
                FilteredBaseData AS (
                    SELECT B.*, M.Product, M.CoefAdj
                    FROM BaseData B
                    INNER JOIN MainTickers M ON B.TradeDay = M.TradeDay AND B.Ticker = M.Ticker
                ),
                WithMultiplier AS (
                    SELECT FBD.*, Basic.Multiplier, Basic.Instrument, Basic.Product AS [Product_Raw], Basic.Exchange
                    FROM FilteredBaseData FBD
                    INNER JOIN FUTURE_RESEARCH_DAILY.dbo.FutureInfo_Basic Basic
                        ON FBD.Ticker = Basic.Ticker
                    WHERE Basic.Multiplier IS NOT NULL AND Basic.Multiplier <> 0
                ),
                Ranked AS (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY TradeDay, Ticker ORDER BY DateTime) AS BarNum
                    FROM WithMultiplier
                ),
                Grouped AS (
                    SELECT *, ((BarNum - 1) / @WindowSize) + 1 AS GroupID
                    FROM Ranked
                ),
                WithOHLC AS (
                        SELECT *,
                        FIRST_VALUE([Open]) OVER (PARTITION BY TradeDay, Ticker, GroupID ORDER BY DateTime) AS Open_,
                        FIRST_VALUE([PrevClose]) OVER (PARTITION BY TradeDay, Ticker, GroupID ORDER BY DateTime) AS PrevClose_,
                        LAST_VALUE([Close]) OVER (
                            PARTITION BY TradeDay, Ticker, GroupID
                            ORDER BY DateTime 
                            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                        ) AS Close_,
                        LAST_VALUE([OpenInterest]) OVER (
                            PARTITION BY TradeDay, Ticker, GroupID
                            ORDER BY DateTime 
                            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                        ) AS OpenInterest_
                    FROM Grouped
                ),
                Aggregated AS (
                    SELECT GroupID, COUNT(*) AS BarCount, MAX([DateTime]) AS [DateTime], TradeDay, Ticker, Product_Raw,
                        Exchange,Instrument, MAX(High) AS High, MIN(Low) AS Low, SUM(Volume) AS Volume, SUM(Amount) AS Amount,
                        MIN(Open_) AS [Open], MIN(PrevClose_) AS PrevClose, MAX(Close_) AS [Close],
                        MAX(OpenInterest_) AS OpenInterest, MAX(CoefAdj) AS CoefAdj, Multiplier
                    FROM WithOHLC
                    GROUP BY TradeDay, Product_Raw, Instrument, Exchange, Ticker, GroupID, Multiplier
                ),
                WithSettle AS (
                    SELECT *,
                        ROUND(CASE WHEN Volume = 0 THEN [Close] ELSE Amount * 1.0 / NULLIF(Volume, 0) / Multiplier END, 2) AS Settle
                    FROM Aggregated
                ),
                FinalWithPrevSettle AS (
                    SELECT *,
                        ROUND(LAG(Settle) OVER (PARTITION BY Ticker ORDER BY TradeDay, DateTime), 2) AS PrevSettle
                    FROM WithSettle
                ),
                MainMerged AS (
                    SELECT K.DateTime, M.TradeDay, M.Product AS [Ticker], K.Product_Raw AS [Product], K.Exchange, K.Instrument,
                        K.[Open], K.[High], K.[Low], K.[Close], K.[PrevClose], K.Settle, ISNULL(K.PrevSettle, K.[Open]) AS PrevSettle,
                        K.Volume, K.Amount, K.OpenInterest, CAST(K.CoefAdj AS FLOAT) AS [CoefAdj], K.BarCount
                    FROM FinalWithPrevSettle K
                    INNER JOIN MainTickers M ON K.TradeDay = M.TradeDay AND K.Ticker = M.Ticker
                )
                SELECT *
                FROM MainMerged
                WHERE TradeDay >= @StartDay
                ORDER BY DateTime;
                ';
                EXEC sp_executesql
                    @sql,
                    N'@ProductList NVARCHAR(MAX), @CalenProductList NVARCHAR(MAX), @TickerList NVARCHAR(MAX), @RawProductList NVARCHAR(MAX), @StartDay DATE, @EndDay DATE, @PrevDay DATE, @WindowSize INT',
                    @ProductList = @ProductList,
                    @CalenProductList = @CalenProductList,
                    @TickerList = @TickerList,
                    @RawProductList = @RawProductList,
                    @StartDay = @StartDay,
                    @EndDay = @EndDay,
                    @PrevDay = @PrevDay,
                    @WindowSize = @WindowSize;
                """
        data = self._api.exec_query(query)

        # 调整数据格式
        data["Ticker"] = data["Ticker"].map(lambda x: x.replace("_", "."))
        data["Ticker"] = data["Ticker"].map(
            lambda x: x[:-4] + "." + x[-4:] if x[-4:].isdigit() and x[:-4].isalpha() else x)
        data["Ticker"] = data["Ticker"].map(lambda x: x.split(".")[-1] if "." in x else "")
        data["Ticker"] = (data["Exchange"] + "." + data["Instrument"].map(lambda x: instrument_to_product("future", x))
                          + "." + data["Ticker"]).map(lambda x: x.rstrip("."))
        return data

    def stock_quote_daily(self, start: Union[datetime, str, int], end: Union[datetime, str, int],
                          ticker: Union[list, tuple] = None):
        # 统一日期格式
        start_ = unify_time(start, fmt="str", mode=3, dot="")
        end_ = unify_time(end, fmt="str", mode=3, dot="")
        # 解析目标合约
        ticker_lst = parse_ticker("stock", ticker)[1]

        query = f"""
                WITH PrevDay AS (
                    SELECT MAX(TradeDay) AS PrevDay
                    FROM [STOCK_RESEARCH_DAILY].[dbo].[STOCKQUOTE_DAILY]
                    WHERE TradeDay < '{start_}'
                ),
                Base AS (
                    SELECT 
                        q.[DateTime], q.[TradeDay], q.[Ticker], 'STK' AS [Product], 
                        i.[Exchange], q.[Ticker] AS [Instrument], q.[Open], q.[High], q.[Low], q.[Close], 
                        q.[PrevClose], q.[Volume], q.[Amount], q.[DealNumber], q.[Committee], 
                        q.[QuantityRelative], q.[BuyVolume], q.[BuyAmount], q.[SaleVolume], 
                        q.[SaleAmount], q.[CommitBuy], q.[CommitSale],
                        CASE 
                            WHEN q.[Volume] = 0 THEN q.[Close]
                            ELSE ROUND(q.[Amount] * 1.0 / NULLIF(q.[Volume], 0), 2)
                        END AS [Settle]
                    FROM [STOCK_RESEARCH_DAILY].[dbo].[STOCKQUOTE_DAILY] q
                    INNER JOIN (
                        SELECT [Ticker], [Exchange]
                        FROM [STOCK_RESEARCH_DAILY].[dbo].[STOCKInfo_Basic]
                        {f"WHERE [Ticker] IN ({str(set(ticker_lst))[1:-1]})" if ticker else ''}
                    ) i ON q.[Ticker] = i.[Ticker]
                    CROSS JOIN PrevDay p
                    WHERE q.[TradeDay] >= p.PrevDay AND q.[TradeDay] <= '{end_}'
                ),
                ADDPREV AS (SELECT *, 
                       LAG(Settle) OVER (
                           PARTITION BY [Ticker]
                           ORDER BY TradeDay
                       ) AS PrevSettle
                FROM Base)
                SELECT * FROM ADDPREV
                WHERE TradeDay >= '{start_}'
                ORDER BY DateTime;
              """

        data = self._api.exec_query(query)
        data["Ticker"] = data["Exchange"] + "." + data["Product"] + "." + data["Instrument"]
        return data

    def stock_quote_min(self, start: Union[datetime, str, int], end: Union[datetime, str, int], interval: str,
                        ticker: Union[list, tuple] = None):
        # 统一日期格式
        start_ = unify_time(start, fmt="str", mode=3, dot="")
        end_ = unify_time(end, fmt="str", mode=3, dot="")
        # 解析目标合约
        ticker_lst = parse_ticker("stock", ticker)[1]
        if not ticker:
            ticker_lst = self.stock_info_basic()["Instrument"].tolist()

        data = pd.DataFrame()
        for i in range(0, int(len(ticker_lst)), 500):
            ticker_lst_ = ticker_lst[i:i + 500]

            if not self._check_db_exist(db=f"STOCK_RESEARCH_{interval}"):
                self.INFO(f"[{self._name}] [INFO] 股票{interval}数据库不存在，切换为主动合成")
                return self._general_bar_generator("Stock", start_, end_, interval, ticker_lst_)

            query = f"""
                    DECLARE @InstrumentList NVARCHAR(MAX) = '{",".join(ticker_lst_)}';
                    DECLARE @StartDay DATE = '{start_}';
                    DECLARE @EndDay DATE = '{end_}';
                    DECLARE @db NVARCHAR(100) = 'STOCK_RESEARCH_{interval}';

                    DECLARE @PrevDay DATE;
                    SELECT @PrevDay = MAX([Date])
                    FROM [FUTURE_RESEARCH_DAILY].[dbo].[AssetTradeDate_UTCGMT8]
                    WHERE China = 1 AND [Date] < @StartDay;

                    DECLARE @PrevDayStr NVARCHAR(10) = CONVERT(NVARCHAR(10), @PrevDay, 23);
                    DECLARE @StartDayStr NVARCHAR(10) = CONVERT(NVARCHAR(10), @StartDay, 23);
                    DECLARE @EndDayStr NVARCHAR(10) = CONVERT(NVARCHAR(10), @EndDay, 23);

                    DECLARE @sql NVARCHAR(MAX) = N'';
                    DECLARE @individualSQL NVARCHAR(MAX);
                    DECLARE @delimiter NVARCHAR(100) = CHAR(13) + CHAR(10) + 'UNION ALL' + CHAR(13) + CHAR(10);

                    SELECT @individualSQL = STRING_AGG(CAST(
                        '
                        SELECT *, ''' + T.Ticker + ''' AS [Ticker], ''' + T.[Ticker] + ''' AS Instrument,
                                ''' + T.Exchange + ''' AS [Exchange],
                               CASE WHEN Volume = 0 THEN [Close] ELSE ROUND(Amount * 1.0 / NULLIF(Volume, 0), 2) END AS Settle
                        FROM [' + @db + '].[dbo].[' + T.Ticker + ']
                        WHERE TradeDay >= ''' + @PrevDayStr + ''' AND TradeDay <= ''' + @EndDayStr + ''''
                    AS NVARCHAR(MAX)), @delimiter)
                    FROM [STOCK_RESEARCH_DAILY].[dbo].[StockInfo_Basic] T
                    WHERE (
                        @InstrumentList IS NULL OR @InstrumentList = ''
                        OR T.[Ticker] IN (
                            SELECT LTRIM(RTRIM(value)) FROM STRING_SPLIT(@InstrumentList, ',')
                        )
                    )
                    AND OBJECT_ID('[' + @db + '].[dbo].[' + T.Ticker + ']', 'U') IS NOT NULL;

                    SET @sql = '
                    WITH AllDataWithSettle AS (
                    ' + @individualSQL + '
                    ),

                    ADDPREV AS (
                        SELECT *,
                               LAG(Settle) OVER (
                                   PARTITION BY Instrument
                                   ORDER BY DateTime
                               ) AS PrevSettle
                        FROM AllDataWithSettle
                    ),
                    TradeCalendar AS (
                    SELECT [Date] AS TradeDay
                    FROM [FUTURE_RESEARCH_DAILY].[dbo].[AssetTradeDate_UTCGMT8]
                    WHERE [China] = 1 AND [Date] <= ''' + @EndDayStr + '''
                    ),
                    RawCoef AS (
                        SELECT Ticker, TradeDay, CAST(CoefAdj AS FLOAT) AS CoefAdj
                        FROM [STOCK_RESEARCH_DAILY].[dbo].[StockCoefAdj]
                        WHERE Ticker IN (
                            SELECT DISTINCT Ticker
                            FROM [STOCK_RESEARCH_DAILY].[dbo].[StockInfo_Basic]
                            WHERE Ticker IN (
                                SELECT LTRIM(RTRIM(value)) FROM STRING_SPLIT(''' + @InstrumentList + ''', '','')
                            )
                        )
                    ),
                    Cumulative AS (
                        SELECT Ticker, TradeDay, CoefAdj,
                               ROW_NUMBER() OVER (PARTITION BY Ticker ORDER BY TradeDay) AS rn
                        FROM RawCoef
                    ),
                    RecursiveCoef AS (
                        SELECT Ticker, TradeDay, CoefAdj AS AdjFactor, rn
                        FROM Cumulative
                        WHERE rn = 1
                        UNION ALL
                        SELECT C.Ticker, C.TradeDay,
                               R.AdjFactor * C.CoefAdj AS AdjFactor,
                               C.rn
                        FROM Cumulative C
                        JOIN RecursiveCoef R ON C.Ticker = R.Ticker AND C.rn = R.rn + 1
                    ),
                    FullCalendar AS (
                        SELECT T.TradeDay, M.Ticker
                        FROM TradeCalendar T
                        CROSS JOIN (
                            SELECT DISTINCT Ticker
                            FROM RawCoef
                        ) M
                    ),
                    CoefJoined AS (
                        SELECT F.TradeDay, F.Ticker, R.AdjFactor
                        FROM FullCalendar F
                        LEFT JOIN RecursiveCoef R
                            ON F.Ticker = R.Ticker AND F.TradeDay = R.TradeDay
                    ),
                    FilledCoef AS (
                        SELECT TradeDay, Ticker,
                            MAX(AdjFactor) OVER (
                                PARTITION BY Ticker ORDER BY TradeDay
                                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                            ) AS CoefAdj
                        FROM CoefJoined
                    )

                    SELECT W.[DateTime],W.[TradeDay], W.[Ticker], ''' + 'STK' + ''' AS [Product],[Exchange],[Instrument],
                            [Open],[High],[Low],[Close],[PrevClose],[Settle],[PrevSettle],[Volume],
                            [Amount],[DealNumber],[Committee],[QuantityRelative],[BuyVolume],[BuyAmount],[SaleVolume],
                            [SaleAmount],[CommitBuy],[CommitSale],ROUND(ISNULL(A.CoefAdj, 1.0), 4) AS [CoefAdj]
                    FROM ADDPREV W
                    LEFT JOIN FilledCoef A
                    ON W.Ticker = A.Ticker AND W.TradeDay = A.TradeDay
                    WHERE W.TradeDay >= ''' + @StartDayStr + '''
                    ORDER BY DateTime
                    OPTION (MAXRECURSION 0);
                    ';
                    EXEC sp_executesql @sql;
                    """
            data_ = self._api.exec_query(query)
            data = pd.concat([data, data_])
        data["Ticker"] = data["Exchange"] + "." + data["Product"] + "." + data["Instrument"]
        return data

    def _general_bar_generator(self, asset: str, start: str, end: str, interval: str, instrument: list):
        """期货灵活K线生成"""
        asset_tag = {"etf": "ETF",
                     "stock": "STK",
                     "index": "IDX"}
        query = f"""
                DECLARE @InstrumentList NVARCHAR(MAX) = N'{','.join(instrument)}'; 
                DECLARE @StartDay DATE = '{start}';
                DECLARE @EndDay DATE = '{end}';
                DECLARE @WindowSize INT = {Interval(interval).window};

                DECLARE @PrevDay DATE = (
                    SELECT MAX([Date])
                    FROM [FUTURE_RESEARCH_DAILY].[dbo].[AssetTradeDate_UTCGMT8]
                    WHERE [Date] < @StartDay AND [China] = 1
                );
                IF @PrevDay IS NULL SET @PrevDay = @StartDay;

                IF OBJECT_ID('tempdb..#TickerMap') IS NOT NULL DROP TABLE #TickerMap;

                SELECT DISTINCT
                    [Ticker],
                    [Ticker] AS [Instrument],
                    [Exchange]
                INTO #TickerMap
                FROM [{asset}_RESEARCH_DAILY].[dbo].[{asset}Info_Basic]
                WHERE [Ticker] IN (
                    SELECT TRIM(value)
                    FROM STRING_SPLIT(@InstrumentList, ',')
                )
                AND Ticker IS NOT NULL;

                DECLARE @baseDataSql NVARCHAR(MAX);
                DECLARE @delimiter VARCHAR(100) = CHAR(13) + CHAR(10) + 'UNION ALL' + CHAR(13) + CHAR(10);

                SELECT @baseDataSql = STRING_AGG(
                    'SELECT
                        [DateTime], [TradeDay],
                        ''' + Ticker + ''' AS [Ticker],
                        ''' + Instrument + ''' AS [Instrument],
                        ''' + Exchange + ''' AS [Exchange],
                        [Open], [High], [Low], [Close], [PrevClose],
                        [Volume], [Amount], [DealNumber], [Committee], [QuantityRelative],
                        [BuyVolume], [BuyAmount], [SaleVolume], [SaleAmount],
                        [CommitBuy], [CommitSale]
                     FROM [{asset}_RESEARCH_1MIN].[dbo].[' + Ticker + ']
                     WHERE [TradeDay] BETWEEN @PrevDay AND @EndDay',
                    @delimiter
                )
                FROM #TickerMap;

                IF @baseDataSql IS NULL OR @baseDataSql = N''
                BEGIN
                    PRINT N'无匹配股票数据表';
                    RETURN;
                END;

                DECLARE @sql NVARCHAR(MAX) = N'
                WITH BaseData AS (
                ' + @baseDataSql + N'
                ),
                Ranked AS (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY TradeDay, Ticker ORDER BY DateTime) AS BarNum
                    FROM BaseData
                ),
                Grouped AS (
                    SELECT *, ((BarNum - 1) / @WindowSize) + 1 AS GroupID
                    FROM Ranked
                ),
                WithOHLC AS (
                    SELECT *,
                        FIRST_VALUE([Open]) OVER (PARTITION BY TradeDay, Ticker, GroupID ORDER BY DateTime) AS [Open_],
                        FIRST_VALUE(PrevClose) OVER (PARTITION BY TradeDay, Ticker, GroupID ORDER BY DateTime) AS PrevClose_,
                        LAST_VALUE([Close]) OVER (PARTITION BY TradeDay, Ticker, GroupID ORDER BY DateTime
                            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS [Close_],
                        LAST_VALUE(QuantityRelative) OVER (PARTITION BY TradeDay, Ticker, GroupID ORDER BY DateTime
                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS QuantityRelative_
                    FROM Grouped
                ),
                Aggregated AS (
                    SELECT
                        Ticker, Instrument, Exchange, TradeDay, MAX(DateTime) AS DateTime, COUNT(*) AS BarCount,
                        MAX(High) AS High, MIN(Low) AS Low, SUM(Volume) AS Volume, SUM(Amount) AS Amount,
                        SUM(DealNumber) AS DealNumber,
                        SUM(BuyVolume) AS BuyVolume, SUM(BuyAmount) AS BuyAmount,
                        SUM(SaleVolume) AS SaleVolume, SUM(SaleAmount) AS SaleAmount,
                        SUM(CommitBuy) AS CommitBuy, SUM(CommitSale) AS CommitSale,
                        MIN([Open_]) AS [Open],
                        MIN(PrevClose_) AS PrevClose,
                        MAX([Close_]) AS [Close],
                        MAX(QuantityRelative_) AS QuantityRelative,
                        CASE
                            WHEN SUM(CommitSale) = 0 THEN NULL
                            ELSE ROUND(SUM(CommitBuy) * 1.0 / NULLIF(SUM(CommitSale), 0), 4)
                        END AS Committee,
                        ROUND(
                            CASE
                                WHEN SUM(Volume) = 0 THEN MAX([Close_])
                                ELSE SUM(Amount) * 1.0 / NULLIF(SUM(Volume), 0)
                            END, 2
                        ) AS Settle
                    FROM WithOHLC
                    GROUP BY Ticker, Instrument, Exchange, TradeDay, GroupID
                ),
                WithPrev AS (
                    SELECT *,
                        ROUND(LAG(Settle) OVER (PARTITION BY Ticker ORDER BY TradeDay, DateTime), 2) AS PrevSettle
                    FROM Aggregated
                ),
                TradeCalendar AS (
                    SELECT [Date] AS TradeDay
                    FROM [FUTURE_RESEARCH_DAILY].[dbo].[AssetTradeDate_UTCGMT8]
                    WHERE [China] = 1 AND [Date] <= @EndDay
                ),
                RawCoef AS (
                    SELECT Ticker, TradeDay, CAST(CoefAdj AS FLOAT) AS CoefAdj
                    FROM [{asset}_RESEARCH_DAILY].[dbo].[{asset}CoefAdj]
                    WHERE Ticker IN (SELECT DISTINCT Ticker FROM #TickerMap)
                ),
                Cumulative AS (
                    SELECT Ticker, TradeDay, CoefAdj,
                           ROW_NUMBER() OVER (PARTITION BY Ticker ORDER BY TradeDay) AS rn
                    FROM RawCoef
                ),
                RecursiveCoef AS (
                    SELECT Ticker, TradeDay, CoefAdj AS AdjFactor, rn
                    FROM Cumulative
                    WHERE rn = 1
                    UNION ALL
                    SELECT C.Ticker, C.TradeDay,
                           R.AdjFactor * C.CoefAdj AS AdjFactor,
                           C.rn
                    FROM Cumulative C
                    JOIN RecursiveCoef R ON C.Ticker = R.Ticker AND C.rn = R.rn + 1
                ),
                FullCalendar AS (
                    SELECT T.TradeDay, M.Ticker
                    FROM TradeCalendar T
                    CROSS JOIN (SELECT DISTINCT Ticker FROM RawCoef) M
                ),
                CoefJoined AS (
                    SELECT F.TradeDay, F.Ticker, R.AdjFactor
                    FROM FullCalendar F
                    LEFT JOIN RecursiveCoef R ON F.Ticker = R.Ticker AND F.TradeDay = R.TradeDay
                ),
                FilledCoef AS (
                    SELECT TradeDay, Ticker,
                        MAX(AdjFactor) OVER (
                            PARTITION BY Ticker ORDER BY TradeDay
                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                        ) AS AdjFactor
                    FROM CoefJoined
                )

                SELECT
                    W.[DateTime], W.[TradeDay], W.[Ticker], ''' + '{asset_tag[asset.lower()]}' + ''' AS [Product], W.[Exchange], W.[Instrument],
                    W.[Open], W.[High], W.[Low], W.[Close],
                    W.[PrevClose], W.[Settle], W.[PrevSettle],
                    W.[Volume], W.[Amount], W.[DealNumber],
                    W.[BuyVolume], W.[BuyAmount], W.[SaleVolume], W.[SaleAmount],
                    W.[CommitBuy], W.[CommitSale], W.[Committee],
                    W.[QuantityRelative],
                    ROUND(ISNULL(A.[AdjFactor], 1.0), 4) AS [CoefAdj],
                    W.[BarCount]
                FROM WithPrev W
                LEFT JOIN FilledCoef A ON W.Ticker = A.Ticker AND W.TradeDay = A.TradeDay
                WHERE W.TradeDay >= @StartDay
                ORDER BY W.DateTime
                OPTION (MAXRECURSION 0);
                ';

                EXEC sp_executesql
                    @sql,
                    N'@StartDay DATE, @EndDay DATE, @PrevDay DATE, @WindowSize INT',
                    @StartDay = @StartDay,
                    @EndDay = @EndDay,
                    @PrevDay = @PrevDay,
                    @WindowSize = @WindowSize;                
                """
        data = self._api.exec_query(query)

        # 调整数据格式
        data["Ticker"] = data["Exchange"] + "." + data["Product"] + "." + data["Instrument"]
        return data

    def index_quote_daily(self, start: Union[datetime, str, int], end: Union[datetime, str, int],
                          ticker: Union[list, tuple] = None):
        # 统一日期格式
        start_ = unify_time(start, fmt="str", mode=3, dot="")
        end_ = unify_time(end, fmt="str", mode=3, dot="")
        # 解析目标合约
        ticker_lst = parse_ticker("index", ticker)[1]

        query = f"""
                WITH PrevDay AS (
                    SELECT MAX(TradeDay) AS PrevDay
                    FROM [INDEX_RESEARCH_DAILY].[dbo].[IndexQuote_Daily]
                    WHERE TradeDay < '{start_}'
                ),
                Base AS (
                    SELECT 
                        q.[DateTime], q.[TradeDay], q.[Ticker], 'IDX' AS [Product], 
                        i.[Exchange], i.[Instrument], q.[Open], q.[High], q.[Low], q.[Close], 
                        q.[PrevClose], q.[Volume], q.[Amount], q.[DealNumber], q.[Committee], 
                        q.[QuantityRelative], q.[BuyVolume], q.[BuyAmount], q.[SaleVolume], 
                        q.[SaleAmount], q.[CommitBuy], q.[CommitSale],
                        CASE 
                            WHEN q.[Volume] = 0 THEN q.[Close]
                            ELSE ROUND(q.[Amount] * 1.0 / NULLIF(q.[Volume], 0), 2)
                        END AS [Settle]
                    FROM [INDEX_RESEARCH_DAILY].[dbo].[IndexQuote_Daily] q
                    INNER JOIN (
                        SELECT [Ticker], [Exchange], [Ticker] AS [Instrument]
                        FROM [INDEX_RESEARCH_DAILY].[dbo].[IndexInfo_Basic]
                        {f"WHERE [Ticker] IN ({str(set(ticker_lst))[1:-1]})" if ticker else ''}
                    ) i ON q.[Ticker] = i.[Ticker]
                    CROSS JOIN PrevDay p
                    WHERE q.[TradeDay] >= p.PrevDay AND q.[TradeDay] <= '{end_}'
                ),
                ADDPREV AS (SELECT *, 
                       LAG(Settle) OVER (
                           PARTITION BY Instrument
                           ORDER BY TradeDay
                       ) AS PrevSettle
                FROM Base)
                SELECT * FROM ADDPREV
                WHERE TradeDay >= '{start_}'
                ORDER BY DateTime;
              """

        data = self._api.exec_query(query)
        data["Ticker"] = data["Exchange"] + "." + data["Product"] + "." + data["Instrument"]
        return data

    def index_quote_min(self, start: Union[datetime, str, int], end: Union[datetime, str, int], interval: str,
                        ticker: Union[list, tuple] = None):
        # 统一日期格式
        start_ = unify_time(start, fmt="str", mode=3, dot="")
        end_ = unify_time(end, fmt="str", mode=3, dot="")
        # 解析目标合约
        ticker_lst = parse_ticker("index", ticker)[1]
        if not ticker:
            info = self.index_info_basic()
            ticker_lst = info["Ticker"].unique().tolist()

        data = pd.DataFrame()
        for i in range(0, int(len(ticker_lst)), 500):
            ticker_lst_ = ticker_lst[i:i + 500]

            if not self._check_db_exist(db=f"INDEX_RESEARCH_{interval}"):
                self.INFO(f"[{self._name}] [INFO] 指数{interval}数据库不存在，切换为主动合成")
                return self._index_bar_generator(start_, end_, interval, ticker_lst_)

            query = f"""
                    DECLARE @InstrumentList NVARCHAR(MAX) = '{",".join(ticker_lst_)}';
                    DECLARE @StartDay DATE = '{start_}';
                    DECLARE @EndDay DATE = '{end_}';
                    DECLARE @db NVARCHAR(100) = 'INDEX_RESEARCH_{interval}';

                    DECLARE @PrevDay DATE;
                    SELECT @PrevDay = MAX([Date])
                    FROM [FUTURE_RESEARCH_DAILY].[dbo].[AssetTradeDate_UTCGMT8]
                    WHERE China = 1 AND [Date] < @StartDay;

                    DECLARE @PrevDayStr NVARCHAR(10) = CONVERT(NVARCHAR(10), @PrevDay, 23);
                    DECLARE @StartDayStr NVARCHAR(10) = CONVERT(NVARCHAR(10), @StartDay, 23);
                    DECLARE @EndDayStr NVARCHAR(10) = CONVERT(NVARCHAR(10), @EndDay, 23);

                    DECLARE @sql NVARCHAR(MAX) = N'';
                    DECLARE @individualSQL NVARCHAR(MAX);
                    DECLARE @delimiter NVARCHAR(100) = CHAR(13) + CHAR(10) + 'UNION ALL' + CHAR(13) + CHAR(10);

                    SELECT @individualSQL = STRING_AGG(CAST(
                        '
                        SELECT *, ''' + T.Ticker + ''' AS [Ticker], ''' + T.Ticker + ''' AS Instrument,
                                ''' + T.Exchange + ''' AS [Exchange],
                               CASE WHEN Volume = 0 THEN [Close] ELSE ROUND(Amount * 1.0 / NULLIF(Volume, 0), 2) END AS Settle
                        FROM [' + @db + '].[dbo].[' + T.Ticker + ']
                        WHERE TradeDay >= ''' + @PrevDayStr + ''' AND TradeDay <= ''' + @EndDayStr + ''''
                    AS NVARCHAR(MAX)), @delimiter)
                    FROM [INDEX_RESEARCH_DAILY].[dbo].[INDEXInfo_Basic] T
                    WHERE (
                        @InstrumentList IS NULL OR @InstrumentList = ''
                        OR T.Ticker IN (
                            SELECT LTRIM(RTRIM(value)) FROM STRING_SPLIT(@InstrumentList, ',')
                        )
                    )
                    AND OBJECT_ID('[' + @db + '].[dbo].[' + T.Ticker + ']', 'U') IS NOT NULL;

                    SET @sql = '
                    WITH AllDataWithSettle AS (
                    ' + @individualSQL + '
                    ),
                    ADDPREV AS (
                        SELECT *,
                               LAG(Settle) OVER (
                                   PARTITION BY Instrument
                                   ORDER BY DateTime
                               ) AS PrevSettle
                        FROM AllDataWithSettle
                    )
                    SELECT [DateTime],[TradeDay], [Ticker], ''' + 'IDX' + ''' AS [Product],[Exchange],[Instrument],
                            [Open],[High],[Low],[Close],[PrevClose],[Settle],[PrevSettle],[Volume],
                            [Amount],[DealNumber],[Committee],[QuantityRelative],[BuyVolume],[BuyAmount],[SaleVolume],
                            [SaleAmount],[CommitBuy],[CommitSale] FROM ADDPREV
                    WHERE TradeDay >= ''' + @StartDayStr + '''
                    ORDER BY DateTime;
                    ';
                    EXEC sp_executesql @sql;
                    """
            data_ = self._api.exec_query(query)
            data = pd.concat([data, data_])
        data["Ticker"] = data["Exchange"] + "." + data["Product"] + "." + data["Instrument"]
        return data

    def _index_bar_generator(self, start: str, end: str, interval: str, ticker: list):

        query = f"""
                DECLARE @TickerList NVARCHAR(MAX) = N'{','.join(ticker)}';  -- 指数代码
                DECLARE @StartDay DATE = '{start}';
                DECLARE @EndDay DATE = '{end}';
                DECLARE @WindowSize INT = {Interval(interval).window};  -- 合成 X 分钟线

                DECLARE @PrevDay DATE = (
                    SELECT MAX([Date])
                    FROM [FUTURE_RESEARCH_DAILY].[dbo].[AssetTradeDate_UTCGMT8]
                    WHERE [Date] < @StartDay AND [China] = 1
                );
                IF @PrevDay IS NULL SET @PrevDay = @StartDay;

                IF OBJECT_ID('tempdb..#TickerMap') IS NOT NULL DROP TABLE #TickerMap;

                SELECT DISTINCT
                    Ticker,
                    Ticker AS [Instrument],
                    Exchange
                INTO #TickerMap
                FROM [INDEX_RESEARCH_DAILY].[dbo].[IndexInfo_Basic]
                WHERE Ticker IN (
                    SELECT TRIM(value) FROM STRING_SPLIT(@TickerList, ',')
                );

                DECLARE @baseDataSql NVARCHAR(MAX);
                DECLARE @delimiter VARCHAR(100) = CHAR(13) + CHAR(10) + 'UNION ALL' + CHAR(13) + CHAR(10);

                SELECT @baseDataSql = STRING_AGG(
                    'SELECT
                        [DateTime], [TradeDay],
                        ''' + Ticker + ''' AS [Ticker],
                        ''' + Exchange + ''' AS [Exchange],
                        ''' + Instrument + ''' AS [Instrument],
                        [Open], [High], [Low], [Close], [PrevClose],
                        [Volume], [Amount], [DealNumber], [Committee], [QuantityRelative],
                        [BuyVolume], [BuyAmount], [SaleVolume], [SaleAmount],
                        [CommitBuy], [CommitSale]
                     FROM [INDEX_RESEARCH_1MIN].[dbo].[' + Ticker + ']
                     WHERE [TradeDay] BETWEEN @PrevDay AND @EndDay',
                    @delimiter
                )
                FROM #TickerMap;

                IF @baseDataSql IS NULL OR @baseDataSql = ''
                BEGIN
                    PRINT N'⚠ 无匹配指数分钟线数据表';
                    RETURN;
                END;

                DECLARE @sql NVARCHAR(MAX) = N'
                WITH BaseData AS (
                ' + @baseDataSql + N'
                ),
                Ranked AS (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY TradeDay, Ticker ORDER BY DateTime) AS BarNum
                    FROM BaseData
                ),
                Grouped AS (
                    SELECT *, ((BarNum - 1) / @WindowSize) + 1 AS GroupID
                    FROM Ranked
                ),
                WithOHLC AS (
                    SELECT *,
                        FIRST_VALUE([Open]) OVER (PARTITION BY TradeDay, Ticker, GroupID ORDER BY DateTime) AS [Open_],
                        FIRST_VALUE(PrevClose) OVER (PARTITION BY TradeDay, Ticker, GroupID ORDER BY DateTime) AS PrevClose_,
                        LAST_VALUE([Close]) OVER (PARTITION BY TradeDay, Ticker, GroupID ORDER BY DateTime
                            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS [Close_],
                        LAST_VALUE(QuantityRelative) OVER (PARTITION BY TradeDay, Ticker, GroupID ORDER BY DateTime
                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS QuantityRelative_
                    FROM Grouped
                ),
                Aggregated AS (
                    SELECT
                        Ticker, Instrument, Exchange, TradeDay, MAX(DateTime) AS DateTime, COUNT(*) AS BarCount,
                        MAX(High) AS High, MIN(Low) AS Low, SUM(Volume) AS Volume, SUM(Amount) AS Amount,
                        SUM(DealNumber) AS DealNumber,
                        SUM(BuyVolume) AS BuyVolume, SUM(BuyAmount) AS BuyAmount,
                        SUM(SaleVolume) AS SaleVolume, SUM(SaleAmount) AS SaleAmount,
                        SUM(CommitBuy) AS CommitBuy, SUM(CommitSale) AS CommitSale,
                        MIN([Open_]) AS [Open],
                        MIN(PrevClose_) AS PrevClose,
                        MAX([Close_]) AS [Close],
                        MAX(QuantityRelative_) AS QuantityRelative,
                        CASE
                            WHEN SUM(CommitSale) = 0 THEN NULL
                            ELSE ROUND(SUM(CommitBuy) * 1.0 / NULLIF(SUM(CommitSale), 0), 4)
                        END AS Committee,
                        ROUND(
                            CASE
                                WHEN SUM(Volume) = 0 THEN MAX([Close_])
                                ELSE SUM(Amount) * 1.0 / NULLIF(SUM(Volume), 0)
                            END, 2
                        ) AS Settle
                    FROM WithOHLC
                    GROUP BY Ticker, Instrument, Exchange, TradeDay, GroupID
                ),
                WithPrev AS (
                    SELECT *,
                        ROUND(LAG(Settle) OVER (PARTITION BY Ticker ORDER BY TradeDay, DateTime), 2) AS PrevSettle
                    FROM Aggregated
                )

                SELECT
                    W.[DateTime], W.[TradeDay], W.[Ticker], W.[Instrument], ''' + 'IDX' + ''' AS [Product],
                    W.[Exchange],
                    W.[Open], W.[High], W.[Low], W.[Close],
                    W.[PrevClose], W.[Settle], W.[PrevSettle],
                    W.[Volume], W.[Amount], W.[DealNumber],
                    W.[BuyVolume], W.[BuyAmount], W.[SaleVolume], W.[SaleAmount],
                    W.[CommitBuy], W.[CommitSale], W.[Committee],
                    W.[QuantityRelative],
                    W.[BarCount]
                FROM WithPrev W
                WHERE W.TradeDay >= @StartDay
                ORDER BY W.Ticker, W.TradeDay, W.DateTime
                OPTION (MAXRECURSION 0);
                ';

                EXEC sp_executesql
                    @sql,
                    N'@StartDay DATE, @EndDay DATE, @PrevDay DATE, @WindowSize INT',
                    @StartDay = @StartDay,
                    @EndDay = @EndDay,
                    @PrevDay = @PrevDay,
                    @WindowSize = @WindowSize;
                """
        data = self._api.exec_query(query)
        # 调整数据格式
        data["Ticker"] = data["Exchange"] + "." + data["Product"] + "." + data["Ticker"]
        return data

    def etf_quote_daily(self, start: Union[datetime, str, int], end: Union[datetime, str, int],
                        ticker: Union[list, tuple] = None):
        # 统一日期格式
        start_ = unify_time(start, fmt="str", mode=3, dot="")
        end_ = unify_time(end, fmt="str", mode=3, dot="")
        # 解析目标合约
        ticker_lst = parse_ticker("stock", ticker)[1]
        query = f"""
                WITH PrevDay AS (
                    SELECT MAX(TradeDay) AS PrevDay
                    FROM [ETF_RESEARCH_DAILY].[dbo].[ETFQuote_Daily]
                    WHERE TradeDay < '{start_}'
                ),
                Base AS (
                    SELECT 
                        q.[DateTime], q.[TradeDay], q.[Ticker], 'ETF' AS [Product], 
                        i.[Exchange], q.[Ticker] AS [Instrument], q.[Open], q.[High], q.[Low], q.[Close], 
                        q.[PrevClose], q.[Volume], q.[Amount], q.[DealNumber], q.[Committee], 
                        q.[QuantityRelative], q.[BuyVolume], q.[BuyAmount], q.[SaleVolume], 
                        q.[SaleAmount], q.[CommitBuy], q.[CommitSale],
                        CASE 
                            WHEN q.[Volume] = 0 THEN q.[Close]
                            ELSE ROUND(q.[Amount] * 1.0 / NULLIF(q.[Volume], 0), 2)
                        END AS [Settle]
                    FROM [ETF_RESEARCH_DAILY].[dbo].[ETFQuote_Daily] q
                    INNER JOIN (
                        SELECT [Ticker], [Exchange]
                        FROM [ETF_RESEARCH_DAILY].[dbo].[ETFInfo_Basic]
                        {f"WHERE [Ticker] IN ({str(set(ticker_lst))[1:-1]})" if ticker else ''}
                    ) i ON q.[Ticker] = i.[Ticker]
                    CROSS JOIN PrevDay p
                    WHERE q.[TradeDay] >= p.PrevDay AND q.[TradeDay] <= '{end_}'
                ),
                ADDPREV AS (SELECT *, 
                       LAG(Settle) OVER (
                           PARTITION BY [Ticker]
                           ORDER BY TradeDay
                       ) AS PrevSettle
                FROM Base)
                SELECT * FROM ADDPREV
                WHERE TradeDay >= '{start_}'
                ORDER BY DateTime;
              """

        data = self._api.exec_query(query)
        data["Ticker"] = data["Exchange"] + "." + data["Product"] + "." + data["Instrument"]
        return data

    def etf_quote_min(self, start: Union[datetime, str, int], end: Union[datetime, str, int], interval: str,
                      ticker: Union[list, tuple] = None):
        # 统一日期格式
        start_ = unify_time(start, fmt="str", mode=3, dot="")
        end_ = unify_time(end, fmt="str", mode=3, dot="")
        # 解析目标合约
        ticker_lst = parse_ticker("etf", ticker)[1]
        if not ticker:
            ticker_lst = self.etf_info_basic()["Instrument"].tolist()

        data = pd.DataFrame()
        for i in range(0, int(len(ticker_lst)), 500):
            ticker_lst_ = ticker_lst[i:i + 500]

            if not self._check_db_exist(db=f"ETF_RESEARCH_{interval}"):
                self.INFO(f"[{self._name}] [INFO] ETF{interval}数据库不存在，切换为主动合成")
                return self._general_bar_generator("ETF", start_, end_, interval, ticker_lst)

            query = f"""
                    DECLARE @InstrumentList NVARCHAR(MAX) = '{",".join(ticker_lst_)}';
                    DECLARE @StartDay DATE = '{start_}';
                    DECLARE @EndDay DATE = '{end_}';
                    DECLARE @db NVARCHAR(100) = 'ETF_RESEARCH_{interval}';

                    DECLARE @PrevDay DATE;
                    SELECT @PrevDay = MAX([Date])
                    FROM [FUTURE_RESEARCH_DAILY].[dbo].[AssetTradeDate_UTCGMT8]
                    WHERE China = 1 AND [Date] < @StartDay;

                    DECLARE @PrevDayStr NVARCHAR(10) = CONVERT(NVARCHAR(10), @PrevDay, 23);
                    DECLARE @StartDayStr NVARCHAR(10) = CONVERT(NVARCHAR(10), @StartDay, 23);
                    DECLARE @EndDayStr NVARCHAR(10) = CONVERT(NVARCHAR(10), @EndDay, 23);

                    DECLARE @sql NVARCHAR(MAX) = N'';
                    DECLARE @individualSQL NVARCHAR(MAX);
                    DECLARE @delimiter NVARCHAR(100) = CHAR(13) + CHAR(10) + 'UNION ALL' + CHAR(13) + CHAR(10);

                    SELECT @individualSQL = STRING_AGG(CAST(
                        '
                        SELECT *, ''' + T.[Ticker] + ''' AS [Ticker], ''' + T.[Ticker] + ''' AS [Instrument],
                                ''' + T.[Exchange] + ''' AS [Exchange],
                               CASE WHEN Volume = 0 THEN [Close] ELSE ROUND(Amount * 1.0 / NULLIF(Volume, 0), 2) END AS Settle
                        FROM [' + @db + '].[dbo].[' + T.Ticker + ']
                        WHERE TradeDay >= ''' + @PrevDayStr + ''' AND TradeDay <= ''' + @EndDayStr + ''''
                    AS NVARCHAR(MAX)), @delimiter)
                    FROM [ETF_RESEARCH_DAILY].[dbo].[ETFInfo_Basic] T
                    WHERE (
                        @InstrumentList IS NULL OR @InstrumentList = ''
                        OR T.[Ticker] IN (
                            SELECT LTRIM(RTRIM(value)) FROM STRING_SPLIT(@InstrumentList, ',')
                        )
                    )
                    AND OBJECT_ID('[' + @db + '].[dbo].[' + T.Ticker + ']', 'U') IS NOT NULL;

                    SET @sql = '
                    WITH AllDataWithSettle AS (
                    ' + @individualSQL + '
                    ),
                    ADDPREV AS (
                        SELECT *,
                               LAG(Settle) OVER (
                                   PARTITION BY [Instrument]
                                   ORDER BY DateTime
                               ) AS PrevSettle
                        FROM AllDataWithSettle
                    )
                    SELECT [DateTime],[TradeDay], [Ticker], ''' + 'ETF' + ''' AS [Product],[Exchange],[Instrument],
                            [Open],[High],[Low],[Close],[PrevClose],[Settle],[PrevSettle],[Volume],
                            [Amount],[DealNumber],[Committee],[QuantityRelative],[BuyVolume],[BuyAmount],[SaleVolume],
                            [SaleAmount],[CommitBuy],[CommitSale] FROM ADDPREV
                    WHERE TradeDay >= ''' + @StartDayStr + '''
                    ORDER BY DateTime;
                    ';
                    EXEC sp_executesql @sql;
                    """
            data_ = self._api.exec_query(query)
            data = pd.concat([data, data_])
        data["Ticker"] = data["Exchange"] + "." + data["Product"] + "." + data["Instrument"]
        return data

    def option_quote_daily(self, start: Union[datetime, str, int], end: Union[datetime, str, int],
                           ticker: Union[list, tuple] = None):
        pass

    def option_quote_min(self, start: Union[datetime, str, int], end: Union[datetime, str, int],
                         ticker: Union[list, tuple] = None):
        pass

    @staticmethod
    def create_bar_schema(asset: str, interval: str, quote: pd.DataFrame):
        # 处理数据列
        quote.columns = quote.columns.str.lower()
        quote = quote.rename(columns={"datetime": "bartime"}).sort_values(by=["bartime", "ticker"])
        quote["asset"] = Asset(asset.lower())
        quote["closeret"] = (np.log(quote["close"]) - np.log(quote["prevclose"])).round(4)
        quote["settleret"] = (np.log(quote["settle"]) - np.log(quote["prevsettle"])).round(4)
        quote["interval"] = Interval(interval)
        quote["tradeday"] = quote["tradeday"].map(lambda x: x.strftime("%Y%m%d")).astype(int)
        quote = quote.loc[:, [k for k in quote.columns if k in get_type_hints(BarData)]]
        # 转为Bar schema
        quote_list = quote.to_dict("records")
        bar_list = [BarData(**k) for k in quote_list]
        return bar_list

    @staticmethod
    def merge_schema(*args, is_bar: bool = True):
        all_data = [item for sublist in args for item in sublist]
        if is_bar:
            return sorted(all_data, key=lambda bar: bar.bartime)
        else:
            return sorted(all_data, key=lambda bar: bar.ticktime)

    @staticmethod
    def parse_bar_schema(data: list) -> pd.DataFrame:
        columns = ['DateTime', 'TradeDay', 'Interval', 'Ticker', 'Product', 'Open', 'High', 'Low', 'Close', 'PrevClose',
                   'Settle', 'PrevSettle', 'Volume', 'Amount', 'OpenInterest', 'CoefAdj']
        if not data:
            return pd.DataFrame(columns=columns)
        data_df = pd.DataFrame([asdict(bar) for bar in data]).rename(
            columns={"bartime": "datetime", "ticktime": "datetime"})
        data_df = data_df.loc[:, [k.lower() for k in columns]]
        data_df.columns = columns
        return data_df

    def asset_coef_adj(self, asset: str, ticker: Union[list, tuple], start: Union[datetime, str, int],
                       end: Union[datetime, str, int]):
        asset = asset.lower()
        if asset == "future":
            return self.future_coef_adj(ticker, start, end)
        elif asset == "stock":
            return self.stock_coef_adj(ticker, start, end)
        else:
            self.ERROR(f"仅支持以下资产的复权系数查询: future / stock")

    def future_coef_adj(self, ticker: Union[list, tuple], start: Union[datetime, str, int],
                        end: Union[datetime, str, int]):
        # 统一日期格式
        start_ = unify_time(start, fmt="str", mode=3, dot="")
        end_ = unify_time(end, fmt="str", mode=3, dot="")
        # 解析品种列表
        product, ticker_lst = parse_ticker("future", ticker)
        hots = [k for k in product if "_" in k]
        raw_hot = [k.split("_")[0].upper() for k in set(hots) if k.endswith("_HOT")]
        calen_hot = [k.upper() for k in set(hots) if not k.endswith("_HOT")]
        query_raw = f"""
                     SELECT [TradeDay], [Ticker], [Product], [Instrument], [Exchange], 
                            CAST([CoefAdj] AS FLOAT) AS [CoefAdj]
                     FROM (SELECT a.[TradeDay], b.[Product] + '_HOT' AS [Ticker], b.[Product], b.[Instrument], b.[Exchange],
                                CAST(EXP(SUM(LOG(a.[CoefAdj] + 1)) 
                                OVER (PARTITION BY a.[Product] ORDER BY a.[TradeDay])) AS DECIMAL(18, 4)) AS [CoefAdj]
                           FROM [FUTURE_RESEARCH_DAILY].[dbo].[FutureCoefAdj_MainTicker] a
                           LEFT JOIN [FUTURE_RESEARCH_DAILY].[dbo].[FutureInfo_Basic] b
                           on a.Ticker = b.Ticker
                           WHERE a.[Product] in ({str(raw_hot)[1:-1]}) 
                                 AND a.[TradeDay] >= '{start_}' AND a.[TradeDay] <= '{end_}') c
                     """
        query_calendar = f"""
                         SELECT [TradeDay], [Ticker], [Product], [Instrument], [Exchange], 
                            CAST([CoefAdj] AS FLOAT) AS [CoefAdj]
                         FROM (
                              SELECT a.[TradeDay], a.[Product] AS [Ticker], b.[Product], b.[Instrument], b.[Exchange],
                                    CAST(EXP(SUM(LOG(a.[CoefAdj] + 1)) 
                                    OVER (PARTITION BY a.[Product] ORDER BY a.[TradeDay])) AS DECIMAL(18, 4)) AS [CoefAdj]
                              FROM [FUTURE_RESEARCH_DAILY].[dbo].[FutureCoefAdj_Calendar] a
                              LEFT JOIN [FUTURE_RESEARCH_DAILY].[dbo].[FutureInfo_Basic] b
                              on a.[Ticker] = b.[Ticker]
                              WHERE a.[Product] in ({str(calen_hot)[1:-1]}) 
                                      AND a.[TradeDay] >= '{start_}' AND a.[TradeDay] <= '{end_}') c
                          """

        if (raw_hot and calen_hot) or (not product):
            query = query_raw + " UNION ALL " + query_calendar + "ORDER BY [TradeDay]"
        elif raw_hot:
            query = query_raw + "ORDER BY [TradeDay]"
        elif calen_hot:
            query = query_calendar + "ORDER BY [TradeDay]"
        else:
            self.ERROR("[期货除权系数获取失败] 目标品种列表数据有误，请检查输入")
            return
        data = self._api.exec_query(query)
        data["Ticker"] = data["Ticker"].map(lambda x: x.split("_")[1])
        data["Ticker"] = (data["Exchange"] + "." + data["Instrument"].map(lambda x: instrument_to_product("future", x))
                          + "." + data["Ticker"])
        return data

    def stock_coef_adj(self, ticker: Union[list, tuple], start: Union[datetime, str, int],
                       end: Union[datetime, str, int]):
        # 统一日期格式
        start_ = unify_time(start, mode=3, dot="")
        end_ = unify_time(end, fmt="str", mode=3, dot="")
        # 解析品种列表
        product, ticker_lst = parse_ticker("stock", ticker)
        query = f"""
                 SELECT [TradeDay], [Ticker], [Product], [Instrument], [Exchange], 
                        CAST([CoefAdj] AS FLOAT) AS [CoefAdj]
                 FROM (SELECT a.[TradeDay], b.[Ticker], 'STK' AS [Product], b.[Ticker] AS [Instrument],
                            b.[Exchange],
                            CAST(EXP(SUM(LOG(a.[CoefAdj])) 
                            OVER (PARTITION BY a.[Ticker] ORDER BY a.[TradeDay])) AS DECIMAL(18, 4)) AS [CoefAdj]
                       FROM [STOCK_RESEARCH_DAILY].[dbo].[StockCoefAdj] a
                       INNER JOIN [STOCK_RESEARCH_DAILY].[dbo].[StockInfo_Basic] b
                       on a.Ticker = b.Ticker
                       WHERE b.[Ticker] in ({str(ticker_lst)[1:-1]})) c
                """
        data = self._api.exec_query(query)
        data["Ticker"] = data["Exchange"] + "." + data["Product"] + data["Ticker"]
        coef_adj = pd.DataFrame(self.all_tradeday(data["TradeDay"].min(), end_), columns=["TradeDay"])
        coef_adj = pd.merge(coef_adj, data, on="TradeDay", how="left")
        coef_adj = coef_adj.set_index(["TradeDay", "Ticker"])[['CoefAdj']].unstack().fillna(
            method="pad").stack().reset_index()
        coef_adj = pd.merge(coef_adj, data[["Ticker", "Product", "Instrument", "Exchange"]], on="Ticker", how="left")
        coef_adj = coef_adj.loc[coef_adj["TradeDay"] >= start_]
        return coef_adj

    def edb_info(self, ticker: Union[list, tuple, dict] = None, use_schema: bool = False):
        if ticker and not isinstance(ticker, (list, tuple, dict)):
            self.ERROR("经济指标输入类型错误")
            return
        ticker_dict = {k: [eval(i.lstrip(k)) for i in ticker if i.lstrip(k).isdigit()] if ticker else []
                       for k in self._edb}
        query_all = []
        for (label, ticker_lst) in ticker_dict.items():
            table = self._edb[label]
            query = f"""
                      SELECT '{label}' + CAST([ID] AS VARCHAR) AS [ID], [Industry], [Description], [Source], [Ticker],
                       [Frequency], [TimeTag], [PublishLag], [QuoteUnit], [Applicable_Pct], [Note],
                        '{f'{table}'}' AS [Table]
                      FROM [ECON_RESEARCH].[dbo].[EDBInfo_{table}]
                      {f"WHERE [ID] in ({str(ticker_lst)[1:-1]})" if ticker_lst else ''}                      
                      """
            if ticker_lst or not ticker:
                query_all.append(query)
        query_all = "Union ALL ".join(query_all) + "ORDER BY [ID]"
        data = self._api.exec_query(query_all)
        self.INFO(f"经济数据信息获取完成: {int(len(data))}条记录")
        if use_schema:
            data.columns = data.columns.str.lower()
            data_dict = data.rename(columns={"id": "index_id"}).to_dict("records")
            data = {k["index_id"]: EdbInfo(**k) for k in data_dict}
        return data

    def edb_data(self, start: Union[datetime, str, int], end: Union[datetime, str, int] = None,
                 ticker: Union[list, tuple, dict] = None, use_schema: bool = False):
        if ticker and not isinstance(ticker, (list, tuple, dict)):
            self.ERROR("经济指标输入类型错误")
            return
        start = unify_time(start, mode=3)
        end = unify_time(end, mode=3) if end else end

        info: dict = self.edb_info(ticker=ticker, use_schema=True)
        grouped = {k: [i for i in info if info[i].table == k] for k in set(d.table for d in info.values())}

        query_all = []
        for (table, ticker_lst) in grouped.items():
            id_lst = [k[1:] for k in ticker_lst]
            query = f"""
                    SELECT [Date], '{table[0].lower()}' + CAST([ID] AS VARCHAR) AS [ID], [EconData] AS [Data]
                    FROM [ECON_RESEARCH].[dbo].[EDB_{table}]
                    {f'WHERE [ID] in ({str(id_lst)[1:-1]})' if id_lst else ''}
                    """
            if id_lst or not ticker:
                query_all.append(query)
        data = self._api.exec_query("UNION ALL".join(query_all) + "ORDER BY [Date]")
        # 调整数据发布滞后
        data = data.set_index(["Date", "ID"])["Data"].unstack()

        pub_lags = [i.publishlag for i in info.values()]
        start_ = start - timedelta(min(0, min(pub_lags)))
        end_ = end + timedelta(max(0, max(pub_lags))) if end else data.index.max() + timedelta(max(0, max(pub_lags)))
        data = pd.DataFrame(index=all_calendar(start_, end_)).join(data)
        for index_id in data.columns:
            lag = info[index_id].publishlag
            data.loc[:, index_id] = data.loc[:, index_id].shift(lag)
        data.index.name = "Date"
        return data.sort_index()

    def edb_last_data(self, ticker: Union[list, tuple, dict] = None):
        if ticker and not isinstance(ticker, (list, tuple, dict)):
            self.ERROR("经济指标输入类型错误")
            return
        info: dict = self.edb_info(ticker=ticker, use_schema=True)
        grouped = {k: [i for i in info if info[i].table == k] for k in set(d.table for d in info.values())}

        query_all = []
        for (table, ticker_lst) in grouped.items():
            id_lst = [k[1:] for k in ticker_lst]
            query = f"""
                    SELECT t.[Date] as [date], '{table[0].lower()}' + CAST(t.[ID] AS VARCHAR) AS [id], t.[EconData] AS [data]
                    FROM [ECON_RESEARCH].[dbo].[EDB_{table}] t
                    JOIN (
                        SELECT ID, MAX([Date]) AS MaxDate
                        FROM [ECON_RESEARCH].[dbo].[EDB_{table}]
                        {f'WHERE [ID] in ({str(id_lst)[1:-1]})' if id_lst else ''}     
                        GROUP BY ID
                    ) latest
                    ON t.ID = latest.ID AND t.[Date] = latest.MaxDate
                    """
            if id_lst or not ticker:
                query_all.append(query)
        data = self._api.exec_query("UNION ALL".join(query_all))
        return data.set_index("id").T.to_dict()

    def treasury_ctd(self, start: Union[datetime, str, int], end: Union[datetime, str, int] = None,
                     ticker: Union[list, tuple, dict] = None):
        if ticker and not isinstance(ticker, (list, tuple, dict)):
            self.ERROR("经济指标输入类型错误")
            return
        start = unify_time(start, mode=3)
        end = unify_time(end, mode=3) if end else end
        query = f"""
                SELECT a.[Date], a.[Ticker], a.[CTD] AS [Treasury], b.[TFactor],
                       c.[Yield_CFETS], c.[Clean_CFETS], c.[Dirty_CFETS], c.[Yield_SHC], c.[Clean_SHC], 
                       c.[Dirty_SHC], c.[ModifiedDuration_SHC], c.[AccruedInterest_SHC], c.[ValueOnBP_SHC],
                       c.[Convexity_SHC]
                FROM (SELECT [Date], [Ticker], [CTD] 
                      FROM [ECON_RESEARCH].[dbo].[Treasury_CtdInfo]
                      WHERE [Date] >= '{start}' 
                      {f'AND [Ticker] IN ({str(ticker)[1:-1]})' if ticker else ''}
                      {f"AND [Date] <= '{end}'" if end else ''}
                      )a 
                LEFT JOIN (SELECT [Ticker], [Treasury], [TFactor]
                           FROM [ECON_RESEARCH].[dbo].[Treasury_TFactor]) b
                ON a.[Ticker] = b.[Ticker] AND a.[CTD] = b.[Treasury]
                LEFT JOIN [ECON_RESEARCH].[dbo].[Treasury_Valuation] c 
                ON a.[CTD] = c.[Ticker] AND a.[Date] = c.[Date]
                """

        data = self._api.exec_query(query)
        return data

