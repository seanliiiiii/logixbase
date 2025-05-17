import sys
import pandas as pd
sys.path.append(r'D:\licx\codes\logixbase')

from logixbase.feeder import TinysoftFeeder, TinysoftConfig, TQSDKConfig
from logixbase.configer import load_schema, read_config
from logixbase.utils import unify_time, DatabaseConfig
from logixbase.feeder.sqlfeeder import SqlServerFeeder
from logixbase.feeder.tqfeeder import TqsdkFeeder



def tinysoft_demo():
    conn_cfg = load_schema(r'D:\licx\codes\usercfg\odin\account.ini', TinysoftConfig)
    self = TinysoftFeeder(conn_cfg)
    self.connect()
    self.all_tradeday("2025-04-01", "2025-04-10", fmt="int", freq="W", day=0)
    # 查询基础信息
    basic_info = self.asset_info("future", "basic")
    basic_info = self.asset_info("etf", "basic")
    basic_info = self.asset_info("stock", "basic")
    basic_info = self.asset_info("index", "basic")
    # 查询期权基础信息
    basic_info = self.asset_info("option", "basic", "2025-04-28")
    #查询期货交易信息
    self.asset_info("future", "trade")

    self.asset_ticker("future", True)
    self.asset_ticker("etf", True)
    self.asset_ticker("stock", True)
    self.asset_ticker("index", True)
    self.asset_ticker("option", True)
    self.option_underlying()
    self.option_ticker_spec("20250402", "M")

    self.asset_quote("future", "2025-04-02", "2025-04-02", interval="60m", ticker=["CZCE.MA.2505"])
    self.asset_quote("stock", "2025-04-02", "2025-04-02", interval="60m", ticker=["SZSE.000001"])
    self.asset_quote("ETF", "2025-04-02", "2025-04-02", interval="60m", ticker=["SZSE.159003"])
    self.asset_quote("index", "2025-04-02", "2025-04-02", interval="60m", ticker=["SSE.SH000001"])
    self.asset_quote("option", start="2015-03-02", end="2015-03-12", interval="60m", ticker=["SSE.OP10000002"])


def sql_demo():
    conn_cfg = load_schema(r'D:\licx\codes\usercfg\odin\db_cfg.ini', DatabaseConfig)
    self = SqlServerFeeder(conn_cfg)
    self.connect()
    # self.all_trade_day("2025-04-01", "2025-04-10", fmt="int", freq="W", day=0)
    # self.trade_time(["RB", "IF", "sc"], call_auction=True)
    # self.asset_ticker("stock", True)

    # self.asset_info("future", "basic")
    # self.asset_info("future", "basic", product=["IF"])
    # self.asset_info("future", "basic", ticker=["CFFEX.IF.2505", "CZCE.MA.HOT", "SHFE.rb"])
    self.asset_info("future", "TRADE", ticker=["CFFEX.IF.2505", "CZCE.MA.HOT", "SHFE.rb"], use_schema=True)
    # info = self.asset_info("future", "basic", use_schema=True)
    # info = self.asset_info("future", "basic", product=["CFFEX.IF"], use_schema=True)
    info = self.asset_info("future", "basic", ticker=["CFFEX.IF.2505", "CZCE.MA.HOT", "SHFE.rb"], use_schema=True)
    info = self.asset_info("stock", "basic", ticker=['SSE.SH600520', 'SZSE.002520', 'SSE.STK.603009'], use_schema=True)
    info = self.asset_info("etf", "basic", ticker=['SSE.ETF.159001', 'SZSE.ETF.510050'], use_schema=True)
    info = self.asset_info("index", "basic", ticker=["SSE.IDX.SH000001", "SZSE.IDX.SZ399001"], use_schema=True)

    # self.asset_main_ticker("future", "2025-04-01", "2025-04-10")
    # self.asset_main_ticker("future", "2025-04-01", "2025-04-10", ['CFFEX.IF.2505', 'CZCE.MA.HOT', 'SHFE.rb.00', 'DCE.i'])

    # self.asset_quote("future", "20250401", "20250402", "daily")
    # self.asset_quote("future", "20250401", "20250402", "day")
    self.asset_quote("future", "20250401", "20250402", "daily", ticker=['CFFEX.IF.2505', 'CZCE.MA.HOT', 'CZCE.MA.01', 'SHFE.rb.00', 'DCE.i'])
    # self.asset_quote("future", "20250401", "20250402", "30min")
    # self.asset_quote("future", "20250401", "20250402", "30min")
    self.asset_quote("future", "20250401", "20250430", "m30", ticker=['CFFEX.IF.2505', 'CZCE.MA.HOT', 'CZCE.MA.01', 'SHFE.rb.00', 'DCE.i'])
    self.asset_quote("future", "20250401", "20250430", "m35", ticker=['CFFEX.IF.2505', 'CZCE.MA.HOT', 'CZCE.MA.01', 'SHFE.rb.00', 'DCE.i'])
    # self.asset_quote("future", "20250401", "20250402", "30m", product=["SHFE.rb", "CZZE.MA", "DCE.i.00"])
    # self.asset_quote("future", "20250401", "20250402", "30min", ticker=["SHFE.rb2505", "CZCE.MA509", "DCE.i2509"])

    # self.asset_quote("stock", "20250401", "20250402", "day")
    # self.asset_quote("stock", "20250401", "20250402", "daily", product=["SZSE.000001", "SSE.600520"])
    self.asset_quote("stock", "20250401", "20250402", "daily", ticker=["SZSE.000001", "SSE.SH600520"])
    self.asset_quote("stock", "20250401", "20250402", "60min", ticker=["SZSE.000001", "SSE.SH600520"])
    # self.asset_quote("stock", "20250401", "20250402", "m60", product=["SZSE.000001", "SSE.SH600520"])

    # self.asset_quote("index", "20250401", "20250402", "day")
    # self.asset_quote("index", "20250401", "20250402", "daily", product=["SSE.SH000001"])
    # self.asset_quote("index", "20250401", "20250402", "daily", ticker=["SSE.SH000001"])
    # self.asset_quote("index", "20250401", "20250402", "30min", ticker=["SSE.SH000001"])
    # self.asset_quote("index", "20250401", "20250402", "m30", product=["SSE.SH000001"])

    # self.asset_quote("etf", "20250401", "20250402", "day")
    # self.asset_quote("etf", "20250401", "20250402", "daily", product=["SZSE.159660", "SZSE.SZ159662"])
    # self.asset_quote("etf", "20250401", "20250402", "daily", ticker=["SZSE.159660", "SZSE.SZ159662"])
    # self.asset_quote("etf", "20250401", "20250402", "30min", ticker=["SZSE.159660", "SZSE.SZ159662"])
    # self.asset_quote("etf", "20250401", "20250402", "m30", product=["SZSE。159660", "SZSE.SZ159662"])

    # self.asset_coef_adj("future", ["RB", "I"], "20250401", "20250430")

    quote = self.asset_quote("future", "20250401", "20250430", "m35", ["SHFE.rb", "CFFEX.IF", "INE.sc", "CZCE.MA","INE.lu.00"], use_schema=True)
    quote = self.asset_quote("index", "20250401", "20250430", "m35", ["SSE.IDX.SH000001"], use_schema=True)
    quote = self.asset_quote("stock", "20250401", "20250430", "m25", ["SZSE.000001", "SSE.SH600520"], use_schema=True)
    quote = self.asset_quote("etf", "20250401", "20250430", "m25", ["SZSE.159660", "SZSE.SZ159662"], use_schema=True)
    quote1 = self.asset_quote("stock", "20250401", "20250430", "m18", ["SZSE.000001", "SZSE.SH600520"], use_schema=True)

    merged = self.merge_schema(quote, quote1, is_bar=True)
    quote_df = self.parse_bar_schema(merged)


def tqsdk_demo():
    conn_cfg = load_schema(r'D:\licx\codes\usercfg\odin\account.ini', TQSDKConfig)
    self = TqsdkFeeder(conn_cfg['tqsdk'])
    self.connect()

    instrument = self.asset_ticker("future")
    info = self.asset_info("future", "basic", ticker=["CFFEX.IF.2505", "CZCE.MA.2510", "SHFE.rb.2510"])
    quote = self.asset_quote("future", start, end, interval, ticker=["CFFEX.IF.2505", "CZCE.MA.2510"])


if __name__ == '__main__':
    # tinysoft_demo()
    # sql_demo()
    start = "20250401"
    end = "20250430"
    interval = "60min"