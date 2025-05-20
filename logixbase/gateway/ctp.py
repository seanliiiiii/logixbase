# -*- coding: utf-8 -*-
import sys
import platform
import importlib.util
from datetime import timedelta
from dataclasses import dataclass
import traceback
import inspect
import os
from multiprocessing import Queue
from time import sleep
import shutil
from pathlib import Path
from typing import Union
from datetime import datetime
import numpy as np
import pandas as pd

from ..trader import (AccountInfo, AccountPosition, TickData, adjust_price, round_to, FutureInfo, OptionInfo,
                      ticker_to_instrument)
from ..utils import unify_time, ProcessBar, all_tradeday, all_calendar

from .base import BaseGateway
from .schema import CtpConfig


LOCAL_DIR = Path(__file__).parent
INSTRUMENT_INFO: dict = {}


def get_target_folder():
    system = platform.system().lower()
    arch = platform.architecture()[0]  # e.g., '64bit'
    py_version = platform.python_version_tuple()  # e.g., ('3', '9', '13')
    py_tag = f"py{py_version[0]}{py_version[1]}"  # e.g., py39

    if system == 'windows':
        folder = f"win{arch.replace('bit', '')}_{py_tag}"  # e.g., win64_py39
    elif system == 'linux':
        folder = "linux"
    else:
        raise RuntimeError(f"当前系统不支持: {system}")

    return folder


def import_api(api: str):
    base_dir = os.path.dirname(os.path.abspath(__file__))  # gateway/
    target_folder = get_target_folder()
    module_dir = os.path.join(base_dir, "api", "ctp", target_folder)
    module_path = os.path.join(module_dir, f"{api}.py")
    module_name = f"{api}_{target_folder}"
    if not os.path.exists(module_path):
        raise FileNotFoundError(f"接口文件不存在：{module_path}")

    # 动态加载模块
    if module_dir not in sys.path:
        sys.path.insert(0, module_dir)

    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@dataclass
class DynamicData:
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)



mdapi = import_api("thostmduserapi")
tdapi = import_api("thosttraderapi")


class CtpGateway(BaseGateway):
    """
    CTP接口
    """
    def __init__(self, config: CtpConfig, logger=None, max_print: int = 0):
        super().__init__("CTP", config, logger)
        self._print_max: int = max_print                                 # 最大数据打印数量

        self.md_api: Union[CtpMdApi, None] = None                       # 行情接口
        self.td_api: Union[CtpTdApi, None] = None                       # 交易接口

    @property
    def tick_count(self):
        """合约tick统计"""
        return self.md_api.tick_count

    def connect(self) -> None:
        """连接交易接口"""
        if not isinstance(self._config, CtpConfig):
            raise ValueError("配置类型错误: 必须是CtpConfig类型，请参考schema创建配置数据类")

        if self._callback is None:
            raise RuntimeError("回调函数未设置: 请先调用register_callback方法注册回调")

        # 连接行情接口
        self.md_api = CtpMdApi(self)
        self.md_api.connect(self._config)
        # 连接交易接口
        self.td_api = CtpTdApi(self)
        self.td_api.connect(self._config)
        self.td_api.settlement_info_confirm()

    def join(self):
        """阻塞"""
        self.md_api.join()

    def disconnect(self) -> None:
        """关闭连接"""
        self.md_api.close()
        self.td_api.close()
        # 删除临时连接文件
        if os.path.exists(LOCAL_DIR.joinpath("ctp_flow")):
            shutil.rmtree(LOCAL_DIR.joinpath("ctp_flow"))

    def subscribe(self, instruments: list = None) -> dict:
        """订阅行情"""
        n = 0
        while n < 3:
            if self.td_api.is_login and self.md_api.is_login:
                break
            else:
                self.INFO("等待接口连接")
            n += 1
            sleep(5)
        INSTRUMENT_INFO.update(self.td_api.qry_instrument())
        # 未指定订阅合约，默认订阅全市场合约
        if instruments is None:
            instruments = list(INSTRUMENT_INFO)
        # 订阅合约
        instruments = [k for k in instruments if INSTRUMENT_INFO[k].asset.value in ("future", "option", "combination")]
        self.md_api.subscribe(instruments)
        return {INSTRUMENT_INFO[k].ticker: INSTRUMENT_INFO[k] for k in self.md_api.tick_count}

    def query_instrument(self):
        """查询所有合约信息"""
        return self.td_api.qry_instrument()

    def send_order(self, req) -> None:
        """委托下单"""
        pass

    def cancel_order(self, req) -> None:
        """委托撤单"""
        pass

    def query_account(self) -> None:
        """查询账户资金信息"""
        return self.td_api.query_account()

    def query_position(self) -> None:
        """查询持仓"""
        return self.td_api.query_position()


class RequestField:
    Req: str = ""


class BaseSpiTemplate:
    """

    """
    def __init__(self, gateway: CtpGateway):
        self.gateway: CtpGateway = gateway

        self._api = None

        self._is_authenticate: bool = False                         # 账户是否被授权登录
        self._is_login: bool = False                                # 账户是否处于已登录状态
        self._is_last: bool = False                                 # 是否为最后返回
        self._rsp_status: bool = False                              # 是否接受到请求对应的返回

        self._print_max: int = gateway._print_max
        self._print_count: int = 0
        self._total: int = 0
        self._req_id: int = 1

        self._wait_queue = Queue()

        self._print_max = gateway._print_max

    def INFO(self, msg: str):
        self.gateway.INFO(msg)

    def ERROR(self, msg: str):
        self.gateway.ERROR(msg)

    @property
    def is_login(self):
        return self._is_login

    def wait(self):
        # 阻塞 等待
        self._wait_queue.get()
        input("-------------------------------- 按任意键退出 trader api demo ")

        self._api.Release()

    def _wait_rsp(self, req_ret: bool):
        """处理请求对应的返回状态"""
        # 请求成功： 等待返回
        if req_ret:
            while True:
                if self._rsp_status:
                    self._rsp_status = False
                    return True
                sleep(0.5)
        # 请求发送失败
        else:
            return False

    def _check_req(self, req, ret: int):
        """检查请求"""
        # 打印请求
        if self._print_max:
            params = []
            for name, value in inspect.getmembers(req):
                if name[0].isupper():
                    params.append(f"{name}={value}")
            self.INFO(f"发送请求: {'.'.join(params)}")

        self._req_id += 1

        # 检查请求结果
        error = {0: "",
                 -1: "网络连接失败",
                 -2: "未处理请求超过许可数",
                 -3: "每秒发送请求数超过许可数"}.get(ret, "未知错误")

        if ret != 0:
            self.INFO(f"请求失败: {ret}={error}")
            return False
        else:
            return True

    def _check_rsp(self, rsp_info: tdapi.CThostFtdcRspInfoField, rsp=None, is_last: bool = True) -> bool:
        """检查响应
        True: 成功 False: 失败
        """

        if self._is_last:
            # Fail to receive data
            if rsp_info and rsp_info.ErrorID != 0:
                self.INFO(f"响应失败, ErrorID={rsp_info.ErrorID}, ErrorMsg={rsp_info.ErrorMsg}")
                return False

            # Print received data
            if self._print_max:
                if rsp:
                    params = []
                    for name, value in inspect.getmembers(rsp):
                        if name[0].isupper():
                            params.append(f"{name}={value}")
                    self.INFO(f"响应成功: {','.join(params)}")
                else:
                    self.INFO("响应为空")

            # Update count
            if not is_last:
                self._print_count += 1
                self._total = +1
            else:
                if self._is_login:
                    self._wait_queue.put_nowait(None)

        else:
            # Print received data
            if self._print_max and (self._print_count < self._print_max):
                if rsp:
                    params = []
                    for name, value in inspect.getmembers(rsp):
                        if name[0].isupper():
                            params.append(f"{name}={value}")
                    self.INFO(f"     {','.join(params)}")
                self._print_count += 1

            self._total += 1

            if is_last:
                self.INFO(f"总计数量: {self._total} 打印数量: {self._print_count}")
                self._print_count = 0

                self._total = 0

                if self._is_login:
                    self._wait_queue.put_nowait(None)

        self._is_last = is_last

        return True

    @staticmethod
    def extract_var_swigobj(obj):
        custom_vars = {}
        for key in dir(obj):
            if not key.startswith('__') and not key.startswith('_'):
                value = getattr(obj, key)
                if not callable(value):
                    custom_vars[key] = value
        return custom_vars

    def print_rsp_rtn(self, prefix, rsp_rtn):
        if rsp_rtn:
            params = []
            for name, value in inspect.getmembers(rsp_rtn):
                if name[0].isupper():
                    params.append(f"{name}={value}")
            self.INFO(f"{prefix}: {','.join(params)}")


class CtpTdApi(tdapi.CThostFtdcTraderSpi,
               BaseSpiTemplate):
    """
    交易回调实现类
    """
    def __init__(self, gateway: CtpGateway):
        BaseSpiTemplate.__init__(self, gateway)
        tdapi.CThostFtdcTraderSpi.__init__(self)

        self._front: list = []                                          # 前置柜台地址 tcp://
        self._user: str = ""                                            # 账户名
        self._password: str = ""                                        # 账户密码
        self._authcode: str = ""                                        # 程序化交易授权码
        self._appid: str = ""                                           # 交易程序名称
        self._broker_id: str = ""                                       # 经纪商编码

        self._api: tdapi.CThostFtdcTraderApi = None
        self.qry_queue: Queue = Queue()                                 # Queues to put query data in callback func

    def connect(self, config: CtpConfig):
        """
        初始化接口
        """
        # 解析变量
        self._front: list = config.tdfront
        self._user: str = config.username
        self._password: str = config.password
        self._authcode: str = config.authcode
        self._appid: str = config.appid
        self._broker_id: str = config.brokerid

        if self.gateway.logger:
            flow_dir = Path(self.gateway.logger.log_path).joinpath("ctp_flow", "tdapi", self._user)
        else:
            flow_dir = LOCAL_DIR.joinpath("ctp_flow", "tdapi", self._user)
        os.makedirs(flow_dir, exist_ok=True)
        self._api = tdapi.CThostFtdcTraderApi.CreateFtdcTraderApi(str(flow_dir) + "\\")
        self.INFO(f"CTP交易API版本号: {self._api.GetApiVersion()}")
        # 注册交易前置
        for front in self._front:
            self._api.RegisterFront(front)
        # 注册交易回调实例
        self._api.RegisterSpi(self)
        # 订阅私有流
        self._api.SubscribePrivateTopic(tdapi.THOST_TERT_QUICK)
        # 订阅公有流
        self._api.SubscribePublicTopic(tdapi.THOST_TERT_QUICK)
        # 初始化交易实例
        self._api.Init()
        # 等待登录成功
        n = 0
        while n < 60:
            if self._is_login or self._rsp_status:
                break
            n += 1
            sleep(1)
        self.INFO(f"CTP交易接口初始化完成， 用户登录{'成功' if self._is_login else '失败'}: {self._user}")

    def join(self):
        """阻塞API进程"""
        self._api.Join()

    def close(self):
        """释放API"""
        self._api.Release()

    def get(self):
        """Get data in query queue"""
        data = []
        while True:
            d, is_last = self.qry_queue.get()
            if d is not None:
                data.append(d)
            if is_last:
                break
        return data

    def OnFrontConnected(self):
        """交易前置连接成功"""
        self.INFO("交易前置连接成功")
        self.authenticate()

    def OnFrontDisconnected(self, reason: int):
        """交易前置连接断开"""
        self.INFO(f"交易前置连接断开: nReason={reason}")

    def authenticate(self):
        """认证账户程序化交易权限"""
        self.INFO("程序化交易权限认证")
        _req = tdapi.CThostFtdcReqAuthenticateField()
        _req.BrokerID = self._broker_id
        _req.UserID = self._user
        _req.AppID = self._appid
        _req.AuthCode = self._authcode
        self._check_req(_req, self._api.ReqAuthenticate(_req, self._req_id))

    def OnRspAuthenticate(self,
                          rsp_auth_field: tdapi.CThostFtdcRspAuthenticateField,
                          rsp_info: tdapi.CThostFtdcRspInfoField,
                          req_id: int,
                          is_last: bool):
        """客户端认证响应"""
        if not self._check_rsp(rsp_info, rsp_auth_field, is_last):
            self._rsp_status = True
            self.INFO("程序化账户认证失败")
            return
        self.INFO("程序化账户认证成功")
        self._is_authenticate = True
        # 登录账户
        self.login()

    def login(self):
        """登录账户"""
        self.INFO("登录交易账户")

        _req = tdapi.CThostFtdcReqUserLoginField()
        _req.BrokerID = self._broker_id
        _req.UserID = self._user
        _req.Password = self._password
        self._check_req(_req, self._api.ReqUserLogin(_req, self._req_id))

    def OnRspUserLogin(self,
                       rsp_login: tdapi.CThostFtdcRspUserLoginField,
                       rsp_info: tdapi.CThostFtdcRspInfoField,
                       req_id: int,
                       is_last: bool):
        """登录响应"""
        if not self._check_rsp(rsp_info, rsp_login, is_last):
            self._rsp_status = True
            self.ERROR(f"账户登录失败：{self._user}")
            return

        self._is_login = True

    def settlement_info_confirm(self):
        """投资者结算结果确认"""
        self.INFO("投资者结算结果确认")

        _req = tdapi.CThostFtdcSettlementInfoConfirmField()
        _req.BrokerID = self._broker_id
        _req.InvestorID = self._user
        send = self._check_req(_req, self._api.ReqSettlementInfoConfirm(_req, self._req_id))

        if send:
            res = self.qry_queue.get()
            if res:
                self.INFO("投资者结算结果确认成功")
            else:
                self.ERROR("投资者结算结果确认失败")
        else:
            self.ERROR("投资者结算结果确认请求失败")

    def OnRspSettlementInfoConfirm(self,
                                   settle_info_confirm: tdapi.CThostFtdcSettlementInfoConfirmField,
                                   rsp_info: tdapi.CThostFtdcRspInfoField,
                                   req_id: int,
                                   is_last: bool):
        """投资者结算结果确认响应"""
        if not self._check_rsp(rsp_info, settle_info_confirm, is_last):
            self.qry_queue.put((False, is_last))
            return
        self.qry_queue.put((True, is_last))

    def qry_instrument(self,
                       exchange_id: str = "",
                       product_id: str = "",
                       instrument_id: str = ""):
        """请求查询合约"""
        self.INFO("请求查询合约")
        _req = tdapi.CThostFtdcQryInstrumentField()
        # 填空可以查询到所有合约
        # 也可分别根据交易所、品种、合约 三个字段查询指定的合约
        _req.ExchangeID = exchange_id
        _req.ProductID = product_id
        _req.InstrumentID = instrument_id

        # 由于流控，单次查询可能失败，通过while循环持续尝试，直到成功发出请求
        while True:
            res = self._check_req(_req, self._api.ReqQryInstrument(_req, self._req_id))
            if res:
                break
            else:
                sleep(1)
        if res:
            data = self.get()
            data = {k.instrument: k for k in data}
            self.INFO(f"合约查询成功: 总计{int(len(data))}")
            return data
        else:
            self.ERROR("合约查询请求发送失败")

    def OnRspQryInstrument(self,
                           info: tdapi.CThostFtdcInstrumentField,
                           rsp_info: tdapi.CThostFtdcRspInfoField,
                           req_id: int,
                           is_last: bool):
        """请求查询合约响应"""
        if not self._check_rsp(rsp_info, info, is_last):
            self.qry_queue.put((None, is_last))
            return

        _map = {tdapi.THOST_FTDC_PC_Futures: "future",
                tdapi.THOST_FTDC_PC_Options: "option",
                tdapi.THOST_FTDC_PC_Combination: "combination",
                tdapi.THOST_FTDC_PC_Spot: "spot",
                tdapi.THOST_FTDC_PC_EFP: "efp",
                tdapi.THOST_FTDC_PC_SpotOption: "option",
                tdapi.THOST_FTDC_PC_TAS: "tas"}
        _op_map = {49: "call",
                   50: "put"}

        data = {"Asset": _map[info.ProductClass],
                "Instrument": info.InstrumentID,
                "ExchangeInstrument": info.ExchangeInstID,
                "InstrumentName": info.InstrumentName,
                "Exchange": info.ExchangeID,
                "Product": info.ProductID,
                "CreateDate":  unify_time(info.CreateDate, mode=3) if info.OpenDate else None,
                "ListDate": unify_time(info.OpenDate, mode=3) if info.OpenDate else None,
                "DelistDate": unify_time(info.ExpireDate, mode=3) if info.ExpireDate else None,
                "DeliverDate": unify_time(info.EndDelivDate, mode=3) if info.EndDelivDate else None,
                "MaxMarketOrderVolume": adjust_price(info.MaxMarketOrderVolume, 0),
                "MinMarketOrderVolume": adjust_price(info.MinMarketOrderVolume, 0),
                "MaxLimitOrderVolume": adjust_price(info.MaxLimitOrderVolume, 0),
                "MinLimitOrderVolume": adjust_price(info.MinLimitOrderVolume, 0),
                "MinVolume": adjust_price(info.MinMarketOrderVolume, 0),
                "Multiplier": float(info.VolumeMultiple),
                "PriceTick": float(info.PriceTick),
                "LifePhase": info.InstLifePhase,
                "IsTrading": info.IsTrading,
                "PositionType": info.PositionType,
                "PositionDateType": info.PositionDateType,
                "Margin": adjust_price(info.LongMarginRatio, 0),
                "LongMarginRatio": adjust_price(info.LongMarginRatio, 0),
                "ShortMarginRatio": adjust_price(info.ShortMarginRatio, 0),
                "MaxMarginSideAlgorithm": info.MaxMarginSideAlgorithm,
                "Strike": adjust_price(info.StrikePrice, 0),
                "OptionType": ord(info.OptionsType),
                "Underlying": info.UnderlyingInstrID,
                "UnderlyingMultiplier": adjust_price(info.UnderlyingMultiple),
                "CombinationType": ord(info.CombinationType),
                }

        data = {k.lower(): v for k, v in data.items()}
        try:
            if data["asset"] == "option":
                data["optiontype"] = _op_map[data["optiontype"]]
                data = OptionInfo(**data)
            else:
                data = FutureInfo(**data)
        except Exception as e:
            self.ERROR(f"{data['instrument']}数据封装失败，已使用通用动态类封装: {e}")
            data = DynamicData(**data)
        self.qry_queue.put((data, is_last))

    def qry_commission_rate(self,
                            instrument_id: str = ""):
        """请求查询合约手续费率"""
        self.INFO("请求查询合约手续费率")
        _req = tdapi.CThostFtdcQryInstrumentCommissionRateField()
        _req.BrokerID = self._broker_id
        _req.InvestorID = self._user
        # 若不指定合约ID, 则返回当前持仓对应合约的手续费率
        _req.InstrumentID = instrument_id
        res = self._check_req(_req, self._api.ReqQryInstrumentCommissionRate(_req, self._req_id))
        if res:
            data = self.get()
            return data

    def OnRspQryInstrumentCommissionRate(self,
                                         commission_rate: tdapi.CThostFtdcInstrumentCommissionRateField,
                                         rsp_info: tdapi.CThostFtdcRspInfoField,
                                         req_id: int,
                                         is_last: bool):
        """请求查询合约手续费率响应"""
        if not self._check_rsp(rsp_info, commission_rate, is_last):
            self.qry_queue.put((None, is_last))
            return

        data = {"Instrument": commission_rate.InstrumentID,
                "Exchange": commission_rate.ExchangeID,
                "BrokerID": commission_rate.BrokerID,
                "InvestorRange": commission_rate.InvestorRange,
                "InvestorID": commission_rate.InvestorRange,
                "InvestUnitID": commission_rate.InvestUnitID,
                "OpenRatioByMoney": commission_rate.OpenRatioByMoney,
                "OpenRatioByVolume": commission_rate.OpenRatioByVolume,
                "CloseRatioByMoney": commission_rate.CloseRatioByMoney,
                "CloseRatioByVolume": commission_rate.CloseRatioByVolume,
                "CloseTodayRatioByMoney": commission_rate.CloseTodayRatioByMoney,
                "CloseTodayRatioByVolume": commission_rate.CloseTodayRatioByVolume,
                "BizType": ord(commission_rate.BizType)}
        self.qry_queue.put((data, is_last))

    def qry_margin_rate(self,
                        instrument_id: str = ""):
        """请求查询合约保证金率, 如果不指定instrument，则返回当前持仓的保证金率"""
        self.INFO("请求查询合约保证金率")
        _req = tdapi.CThostFtdcQryInstrumentMarginRateField()
        _req.BrokerID = self._broker_id
        _req.InvestorID = self._user
        _req.HedgeFlag = tdapi.THOST_FTDC_HF_Speculation
        # 若不指定合约ID, 则返回当前持仓对应合约的保证金率
        _req.InstrumentID = instrument_id
        res = self._check_req(_req, self._api.ReqQryInstrumentMarginRate(_req, self._req_id))

        if res:
            data = self.get()
            return data

    def OnRspQryInstrumentMarginRate(self,
                                     margin_rate: tdapi.CThostFtdcInstrumentMarginRateField,
                                     rsp_info: tdapi.CThostFtdcRspInfoField,
                                     req_id: int,
                                     is_last: bool):
        """请求查询合约保证金率响应"""
        if not self._check_rsp(rsp_info, margin_rate, is_last):
            self.qry_queue.put((None, is_last))
            return
        data = {"Instrument": margin_rate.InstrumentID,
                "Exchange": margin_rate.ExchangeID,
                "BrokerID": margin_rate.BrokerID,
                "InvestorRange": margin_rate.InvestorRange,
                "InvestorID": margin_rate.InvestorRange,
                "InvestUnitID": margin_rate.InvestUnitID,
                "HedgeFlag": margin_rate.HedgeFlag,
                "LongMarginRatioByMoney": margin_rate.LongMarginRatioByMoney,
                "LongMarginRatioByVolume": margin_rate.LongMarginRatioByVolume,
                "ShortMarginRatioByMoney": margin_rate.ShortMarginRatioByMoney,
                "ShortMarginRatioByVolume": margin_rate.ShortMarginRatioByVolume,
                "IsRelative": margin_rate.IsRelative}
        self.qry_queue.put((data, is_last))

    def qry_depth_market_data(self, instrument_id: str = ""):
        """请求查询行情，只能查询当前快照，不能查询历史行情"""
        self.INFO("请求查询行情")
        _req = tdapi.CThostFtdcQryDepthMarketDataField()
        # 若不指定合约ID, 则返回所有合约的行情
        _req.InstrumentID = instrument_id
        res = self._check_req(_req, self._api.ReqQryDepthMarketData(_req, self._req_id))

        if res:
            data = self.get()
            return data

    def OnRspQryDepthMarketData(self,
                                md: tdapi.CThostFtdcDepthMarketDataField,
                                rsp_info: tdapi.CThostFtdcRspInfoField,
                                req_id: int,
                                is_last: bool):
        """请求查询行情响应"""
        if not self._check_rsp(rsp_info, md, is_last):
            self.ERROR(f"行情数据相应失败: {req_id}")
            self.qry_queue.put((None, is_last))
            return

        tick = md_to_tick(md)
        if tick:
            self.qry_queue.put((tick, is_last))

    def qry_trading_code(self, exchange_id: str):
        """请求查询交易编码"""
        self.INFO("请求查询交易编码")
        _req = tdapi.CThostFtdcQryTradingCodeField()
        _req.BrokerID = self._broker_id
        _req.InvestorID = self._user
        _req.ExchangeID = exchange_id
        res = self._check_req(_req, self._api.ReqQryTradingCode(_req, self._req_id))
        if res:
            data = self.get()
            return data

    def OnRspQryTradingCode(self,
                            trade_code: tdapi.CThostFtdcTradingCodeField,
                            rsp_info: tdapi.CThostFtdcRspInfoField,
                            req_id: int,
                            is_last: bool):
        """请求查询交易编码响应"""
        if not self._check_rsp(rsp_info, trade_code, is_last):
            self.qry_queue.put((None, is_last))
            return

        d = {"InvestorID": trade_code.InvestorID,
             "BrokerID": trade_code.BrokerID,
             "ClientID": trade_code.ClientID,
             "IsActive": trade_code.IsActive,
             "ClientIDType": trade_code.ClientIDType,
             "BranchID": trade_code.BranchID,
             "BizType": ord(trade_code.BizType),
             "InvestUnitID": trade_code.InvestUnitID}

        self.qry_queue.put((d, is_last))

    def qry_exchange(self, exchange_id: str):
        """查询交易所"""
        self.INFO("查询交易所")
        req = tdapi.CThostFtdcQryExchangeField()
        req.ExchangeID = exchange_id
        res = self._check_req(req, self._api.ReqQryExchange(req, self._req_id))
        if res:
            data = self.get()
            return data

    def OnRspQryExchange(self,
                         exchange: tdapi.CThostFtdcExchangeField,
                         rsp_info: tdapi.CThostFtdcRspInfoField,
                         req_id: int,
                         is_last: bool):
        """查询交易所应答"""
        if not self._check_rsp(rsp_info, exchange, is_last):
            self.qry_queue.put((None, is_last))
            return

        d = {"Exchange": exchange.ExchangeID,
             "ExchangeName": exchange.ExchangeName,
             "ExchangeProperty": exchange.ExchangeProperty}
        self.qry_queue.put((d, is_last))

    def update_password(self, new_password: str, old_password: str):
        """用户口令变更"""
        self.INFO("用户口令变更请求")

        req = tdapi.CThostFtdcUserPasswordUpdateField()
        req.BrokerID = self._broker_id
        req.UserID = self._user
        req.OldPassword = old_password
        req.NewPassword = new_password
        res = self._check_req(req, self._api.ReqUserPasswordUpdate(req, self._req_id))
        if res:
            return self.get()[0]

    def OnRspUserPasswordUpdate(self,
                                password_update: tdapi.CThostFtdcUserPasswordUpdateField,
                                rsp_info: tdapi.CThostFtdcRspInfoField,
                                req_id: int,
                                is_last: bool):
        """用户口令变更响应"""
        d = self._check_rsp(rsp_info, password_update, is_last)
        self.qry_queue.put((d, is_last))

    def qry_order_comm_rate(self, instrument_id: str):
        """查询申报费率"""
        self.INFO("请求查询申报费率")
        req = tdapi.CThostFtdcQryInstrumentOrderCommRateField()
        req.BrokerID = self._broker_id
        req.InvestorID = self._user
        req.InstrumentID = instrument_id
        res = self._check_req(req, self._api.ReqQryInstrumentOrderCommRate(req, self._req_id))

        if res:
            return self.get()

    def OnRspQryInstrumentOrderCommRate(self,
                                        order_commission_rate: tdapi.CThostFtdcInstrumentOrderCommRateField,
                                        rsp_info: tdapi.CThostFtdcRspInfoField,
                                        req_id: int,
                                        is_last: bool):
        """查询申报费率应答"""
        if not self._check_rsp(rsp_info, order_commission_rate, is_last):
            self.qry_queue.put((None, is_last))
            return

        d = {"Instrument": order_commission_rate.InstrumentID,
             "Exchange": order_commission_rate.ExchangeID,
             "BrokerID": order_commission_rate.BrokerID,
             "InvestorID": order_commission_rate.InvestorID,
             "InvestorRange": order_commission_rate.InvestorRange,
             "InvestUnitID": order_commission_rate.InvestUnitID,
             "HedgeFlag": order_commission_rate.HedgeFlag,
             "OrderCommByVolume": order_commission_rate.OrderCommByVolume,
             "OrderActionCommByVolume": order_commission_rate.OrderActionCommByVolume,
             "OrderCommByTrade": order_commission_rate.OrderCommByTrade,
             "OrderActionCommByTrade": order_commission_rate.OrderActionCommByTrade}
        self.qry_queue.put((d, is_last))

    def query_account(self):
        """查询账户资金"""
        req = tdapi.CThostFtdcQryTradingAccountField()
        req.BrokerID = self._broker_id
        req.InvestorID = self._user
        res = self._check_req(req, self._api.ReqQryTradingAccount(req, self._req_id))

        if res:
            return self.get()

    def OnRspQryTradingAccount(self,
                               account: tdapi.CThostFtdcTradingAccountField,
                               rsp_info: tdapi.CThostFtdcRspInfoField,
                               req_id: int,
                               is_last: bool):
        """查询账户资金应答"""
        if not self._check_rsp(rsp_info, account, is_last):
            self.qry_queue.put((None, is_last))
            return

        d = AccountInfo(broker_id=account.BrokerID,
                        account_id=account.AccountID,
                        prev_mortgage=account.PreMortgage,
                        prev_credit=account.PreCredit,
                        prev_deposit=account.PreDeposit,
                        prev_balance=account.PreBalance,
                        prev_margin=account.PreMargin,
                        interestbase=account.InterestBase,
                        interest=account.Interest,
                        deposit=account.Deposit,
                        withdraw=account.Withdraw,
                        frozen_margin=account.FrozenMargin,
                        frozen_cash=account.FrozenCash,
                        frozen_commission=account.FrozenCommission,
                        current_margin=account.CurrMargin,
                        cashin=account.CashIn,
                        commission=account.Commission,
                        close_profit=account.CloseProfit,
                        position_profit=account.PositionProfit,
                        balance=account.Balance,
                        available=account.Available,
                        withdrawquota=account.WithdrawQuota,
                        reserve=account.Reserve,
                        tradeday=unify_time(account.TradingDay, "odin"),
                        settlement_id=account.SettlementID,
                        credit=account.Credit,
                        mortgage=account.Mortgage,
                        exchange_margin=account.ExchangeMargin,
                        delivery_margin=account.DeliveryMargin,
                        exchange_delivery_margin=account.ExchangeDeliveryMargin,
                        reserve_balance=account.ReserveBalance,
                        currency_id=account.CurrencyID,
                        prefund_mortgagein=account.PreFundMortgageIn,
                        prefund_mortgageout=account.PreFundMortgageOut,
                        fund_mortgage_in=account.FundMortgageIn,
                        fund_mortgage_out=account.FundMortgageOut,
                        fund_mortgage_available=account.FundMortgageAvailable,
                        mortgageable_fund=account.MortgageableFund,
                        specproduct_margin=account.SpecProductMargin,
                        specproduct_frozenmargin=account.SpecProductFrozenMargin,
                        specproduct_commission=account.SpecProductCommission,
                        specproduct_frozencommission=account.SpecProductFrozenCommission,
                        specproduct_positionprofit=account.SpecProductPositionProfit,
                        specproduct_closeprofit=account.SpecProductCloseProfit,
                        specproduct_positionprofitbyalg=account.SpecProductPositionProfitByAlg,
                        specproduct_exchangemargin=account.SpecProductExchangeMargin,
                        biztype=ord(account.BizType),
                        frozen_swap=account.FrozenSwap,
                        remain_swap=account.RemainSwap
                        )
        self.qry_queue.put((d, is_last))

    def query_position(self):
        """查询账户当前持仓"""
        req = tdapi.CThostFtdcQryInvestorPositionField()
        res = self._check_req(req, self._api.ReqQryInvestorPosition(req, self._req_id))

        if res:
            return self.get()

    def OnRspQryInvestorPosition(self,
                                 position: tdapi.CThostFtdcInvestorPositionField,
                                 rsp_info: tdapi.CThostFtdcRspInfoField,
                                 req_id: int,
                                 is_last: bool):
        """查询账户持仓应答"""
        if not self._check_rsp(rsp_info, position, is_last):
            self.qry_queue.put((None, is_last))
            return
        if position:
            d = AccountPosition(broker_id=position.BrokerID,
                                account_id=position.InvestorID,
                                hold_direction=position.PosiDirection,
                                hedge_flag=position.HedgeFlag,
                                hold_date=position.PositionDate,
                                prev_volume=position.YdPosition,
                                volume=position.Position,
                                long_frozen=position.LongFrozen,
                                short_frozen=position.ShortFrozen,
                                long_frozen_amount=position.LongFrozenAmount,
                                short_frozen_amount=position.ShortFrozenAmount,
                                open_volume=position.OpenVolume,
                                close_volume=position.CloseVolume,
                                open_amount=position.OpenAmount,
                                close_amount=position.CloseAmount,
                                hold_cost=position.PositionCost,
                                prev_margin=position.PreMargin,
                                use_margin=position.UseMargin,
                                frozen_margin=position.FrozenMargin,
                                frozen_cash=position.FrozenCash,
                                frozen_commission=position.FrozenCommission,
                                cashin=position.CashIn,
                                commission=position.Commission,
                                close_profit=position.CloseProfit,
                                hold_profit=position.PositionProfit,
                                prev_settle_price=position.PreSettlementPrice,
                                settle_price=position.SettlementPrice,
                                tradeday=unify_time(position.TradingDay, "odin"),
                                settlement_id=position.SettlementID,
                                open_cost=position.OpenCost,
                                exchange_margin=position.ExchangeMargin,
                                comb_position=position.CombPosition,
                                comb_long_frozen=position.CombLongFrozen,
                                comb_short_frozen=position.CombShortFrozen,
                                close_profit_bydate=position.CloseProfitByDate,
                                close_profit_bytrade=position.CloseProfitByTrade,
                                today_hold=position.TodayPosition,
                                marginrate_bymoney=position.MarginRateByMoney,
                                marginrate_byvolume=position.MarginRateByVolume,
                                strike_frozen=position.StrikeFrozen,
                                strike_frozen_amount=position.StrikeFrozenAmount,
                                abandon_frozen=position.AbandonFrozen,
                                exchange_id=position.ExchangeID,
                                prev_strike_frozen=position.YdStrikeFrozen,
                                invest_unit_id=position.InvestUnitID,
                                position_cost_offset=position.PositionCostOffset,
                                tas_position=position.TasPosition,
                                tas_position_cost=position.TasPositionCost,
                                instrument_id=position.InstrumentID)
        else:
            d = None
        self.qry_queue.put((d, is_last))

    def send_market_order(self,
                          exchange_id: str,
                          instrument_id: str,
                          volume: int = 1):
        """报单录入请求(市价单)

        - 录入错误时对应响应OnRspOrderInsert、OnErrRtnOrderInsert，
        - 正确时对应回报OnRtnOrder、OnRtnTrade。
        """
        self.INFO("报单录入请求(市价单)")

        # 市价单, 注意选择一个相对活跃的合约
        # simnow 目前貌似不支持市价单，所以会被自动撤销！！！
        _req = tdapi.CThostFtdcInputOrderField()
        _req.BrokerID = self._broker_id
        _req.InvestorID = self._user
        _req.ExchangeID = exchange_id
        _req.InstrumentID = instrument_id
        _req.LimitPrice = 0
        _req.OrderPriceType = tdapi.THOST_FTDC_OPT_AnyPrice  # 价格类型市价单
        _req.Direction = tdapi.THOST_FTDC_D_Buy  # 买
        _req.CombOffsetFlag = tdapi.THOST_FTDC_OF_Open  # 开仓
        _req.CombHedgeFlag = tdapi.THOST_FTDC_HF_Speculation
        _req.VolumeTotalOriginal = volume
        _req.IsAutoSuspend = 0
        _req.IsSwapOrder = 0
        _req.TimeCondition = tdapi.THOST_FTDC_TC_GFD
        _req.VolumeCondition = tdapi.THOST_FTDC_VC_AV
        _req.ContingentCondition = tdapi.THOST_FTDC_CC_Immediately
        _req.ForceCloseReason = tdapi.THOST_FTDC_FCC_NotForceClose
        self._check_req(_req, self._api.ReqOrderInsert(_req, self._req_id))

    def limit_order_insert(self,
                           exchange_id: str,
                           instrument_id: str,
                           price: float,
                           volume: int = 1,
                           ):
        """报单录入请求(限价单)

        - 录入错误时对应响应OnRspOrderInsert、OnErrRtnOrderInsert，
        - 正确时对应回报OnRtnOrder、OnRtnTrade。
        """
        self.INFO("报单录入请求(限价单)")

        # 限价单 注意选择一个相对活跃的合约及合适的价格
        _req = tdapi.CThostFtdcInputOrderField()
        _req.BrokerID = self._broker_id
        _req.InvestorID = self._user
        _req.ExchangeID = exchange_id
        _req.InstrumentID = instrument_id  # 合约ID
        _req.LimitPrice = price  # 价格
        _req.OrderPriceType = tdapi.THOST_FTDC_OPT_LimitPrice  # 价格类型限价单
        _req.Direction = tdapi.THOST_FTDC_D_Buy  # 买
        _req.CombOffsetFlag = tdapi.THOST_FTDC_OF_Open  # 开仓
        _req.CombHedgeFlag = tdapi.THOST_FTDC_HF_Speculation
        _req.VolumeTotalOriginal = volume
        _req.IsAutoSuspend = 0
        _req.IsSwapOrder = 0
        _req.TimeCondition = tdapi.THOST_FTDC_TC_GFD
        _req.VolumeCondition = tdapi.THOST_FTDC_VC_AV
        _req.ContingentCondition = tdapi.THOST_FTDC_CC_Immediately
        _req.ForceCloseReason = tdapi.THOST_FTDC_FCC_NotForceClose
        self._check_req(_req, self._api.ReqOrderInsert(_req, self._req_id))

    def OnRspOrderInsert(self,
                         order: tdapi.CThostFtdcInputOrderField,
                         rsp_info: tdapi.CThostFtdcRspInfoField,
                         req_id: int,
                         is_last: bool):
        """报单录入请求响应"""
        self._check_rsp(rsp_info, order, is_last)

    def order_cancel_1(self, exchange_id: str, instrument_id: str, order_sys_id: str):
        """报单撤销请求 方式一

        - 错误响应: OnRspOrderAction，OnErrRtnOrderAction
        - 正确响应：OnRtnOrder
        """
        self.INFO("报单撤销请求 方式一")

        # 撤单请求，首先需要有一笔未成交的订单，可以使用限价单，按照未成交订单信息填写撤单请求
        _req = tdapi.CThostFtdcInputOrderActionField()
        _req.BrokerID = self._broker_id
        _req.InvestorID = self._user
        _req.UserID = self._user
        _req.ExchangeID = exchange_id
        _req.InstrumentID = instrument_id
        _req.ActionFlag = tdapi.THOST_FTDC_AF_Delete

        _req.OrderSysID = order_sys_id  # OrderSysId 中空格也要带着

        # 若成功，会通过 报单回报 返回新的订单状态, 若失败则会响应失败
        self._check_req(_req, self._api.ReqOrderAction(_req, self._req_id))

    def order_cancel_2(self,
                       exchange_id: str,
                       instrument_id: str,
                       front_id: int,
                       session_id: int,
                       order_ref: str):
        """报单撤销请求 方式二

        - 错误响应: OnRspOrderAction，OnErrRtnOrderAction
        - 正确响应：OnRtnOrder
        """
        self.INFO("报单撤销请求 方式二")

        # 撤单请求，首先需要有一笔未成交的订单，可以使用限价单，按照未成交订单信息填写撤单请求
        _req = tdapi.CThostFtdcInputOrderActionField()
        _req.BrokerID = self._broker_id
        _req.InvestorID = self._user
        _req.UserID = self._user
        _req.ExchangeID = exchange_id
        _req.InstrumentID = instrument_id
        _req.ActionFlag = tdapi.THOST_FTDC_AF_Delete

        _req.OrderRef = order_ref
        _req.FrontID = front_id
        _req.SessionID = session_id

        # 若成功，会通过 报单回报 返回新的订单状态, 若失败则会响应失败
        self._check_req(_req, self._api.ReqOrderAction(_req, self._req_id))

    def OnRspOrderAction(self,
                         order_action: tdapi.CThostFtdcInputOrderActionField,
                         rsp_info: tdapi.CThostFtdcRspInfoField,
                         req_id: int,
                         is_last: bool):
        """报单操作请求响应"""
        self._check_rsp(rsp_info, order_action, is_last)

    def OnRtnOrder(self, order: tdapi.CThostFtdcOrderField):
        """报单通知，当执行ReqOrderInsert后并且报出后，收到返回则调用此接口，私有流回报。"""
        self.print_rsp_rtn("报单通知", order)

    def OnRtnTrade(self, trade: tdapi.CThostFtdcTradeField):
        """成交通知，报单发出后有成交则通过此接口返回。私有流"""
        self.print_rsp_rtn("成交通知", trade)

    def OnErrRtnOrderInsert(self,
                            order: tdapi.CThostFtdcInputOrderField,
                            rsp_info: tdapi.CThostFtdcRspInfoField):
        """"""
        self._check_rsp(rsp_info, order)


class CtpMdApi(BaseSpiTemplate,
               mdapi.CThostFtdcMdSpi):
    """
    Spi for CTP API
    """
    def __init__(self,
                 gateway: CtpGateway):
        BaseSpiTemplate.__init__(self, gateway)
        mdapi.CThostFtdcMdSpi.__init__(self)

        self.gateway: CtpGateway = gateway

        self._api: mdapi.CThostFtdcMdApi.CreateFtdcMdApi = None
        self._subscribed: dict = {}                       # Count of ticks for each symbol
        self._sub_n: int = 0                              # Count of completed subscription attempts
        self._sub_ttl: int = 0                            # Total subbscription attempts

    @property
    def tick_count(self) -> dict:
        """合约快照数量统计"""
        return self._subscribed

    def connect(self, config: CtpConfig):
        """连接行情服务器"""
        # 解析变量
        if self.gateway.logger:
            flow_dir = Path(self.gateway.logger.log_path).joinpath("ctp_flow", "mdapi", config.username)
        else:
            flow_dir = LOCAL_DIR.joinpath("ctp_flow", "mdapi", config.username)
        os.makedirs(flow_dir, exist_ok=True)
        self._api = mdapi.CThostFtdcMdApi.CreateFtdcMdApi(str(flow_dir) + "\\")
        self.INFO(f"CTP行情API版本号: {self._api.GetApiVersion()}")
        # 注册行情前置
        for front_ in config.mdfront:
            self._api.RegisterFront(front_)
        # 注册行情回调实例
        self._api.RegisterSpi(self)
        # 初始化行情实例
        self._api.Init()
        # 等待登录成功
        n = 0
        while n < 60:
            if self._is_login or self._rsp_status:
                break
            n += 1
            sleep(1)
        self.INFO(f"CTP行情接口初始化完成: {'成功' if self._is_login else '失败'}")

    def join(self):
        """阻塞API进程"""
        self._api.Join()

    def close(self):
        """释放API"""
        self._api.Release()

    def OnFrontConnected(self):
        """Rewrite functionn upon front address connected"""
        self.INFO("行情前置连接成功")
        # 登录行情账户
        self.login()

    def OnFrontDisconnected(self, n_reason):
        """Action when front address disconnected"""
        self.INFO(f"行情前置连接断开：nReason={n_reason}")

    def login(self):
        """登录行情服务器"""
        self.INFO("登录行情服务器")
        _req = mdapi.CThostFtdcReqUserLoginField()
        self._check_req(_req, self._api.ReqUserLogin(_req, self._req_id))

    def OnRspUserLogin(self,
                       rsp_login: mdapi.CThostFtdcRspUserLoginField,
                       rsp_info: mdapi.CThostFtdcRspInfoField,
                       req_id: int,
                       is_last: bool):
        """登录响应"""
        if not self._check_rsp(rsp_info, rsp_login, is_last):
            self._rsp_status = True
            self.INFO(f"行情账户登录失败")
            return
        self._is_login = True
        self._rsp_status = True

    def subscribe(self, instruments: list):
        """订阅合约行情"""
        self.INFO("订阅合约行情")
        slice_instruments = [k.encode("utf-8") for k in sorted(instruments)]
        slice_instruments = [slice_instruments[i:(i + 30)] for i in range(0, int(len(slice_instruments)), 30)]

        process = ProcessBar(int(len(slice_instruments)))
        for (i, instrument) in enumerate(slice_instruments):
            _req = RequestField()
            _req.Req = f"{instrument}"
            self._sub_ttl += 1
            while True:
                res = self._check_req(_req, self._api.SubscribeMarketData(instrument, int(len(instrument))))
                if res:
                    break
                else:
                    sleep(1)
            process.show(i + 1)
        while True:
            if self._sub_n >= self._sub_ttl:
                self.INFO(f"所有行情订阅完成：成功{int(len(self._subscribed))}， 总计{int(len(instruments))}")
                break
            sleep(0.5)

    def OnRspSubMarketData(self,
                           symbol_field: mdapi.CThostFtdcSpecificInstrumentField,
                           rsp_info: mdapi.CThostFtdcRspInfoField,
                           rq_id: int,
                           is_last: bool):
        """合约订阅响应"""
        self._subscribed[symbol_field.InstrumentID] = 0
        if not self._check_rsp(rsp_info, symbol_field, is_last):
            self.ERROR(f"行情订阅失败：{symbol_field.InstrumentID}，尝试重新订阅")
            _req = RequestField()
            _req.Req = f"{[symbol_field.InstrumentID]}"
            self._check_req(_req, self._api.SubscribeMarketData([symbol_field.InstrumentID], 1))
            self._sub_ttl += 1
            return

        if is_last:
            self._sub_n += 1

    def OnRtnDepthMarketData(self, md: mdapi.CThostFtdcDepthMarketDataField):
        """行情数据推送响应"""
        # 过滤异常行情数据：没有时间戳 / 价格异常
        if not md.UpdateTime:
            return

        try:
            self.on_rtn_depth_marketdata(md)
        except Exception as e:
            tb = traceback.format_exc()
            self.ERROR(f"Ctp Tick行情回调执行失败: {e} /n{tb}")

    def on_rtn_depth_marketdata(self, md: mdapi.CThostFtdcDepthMarketDataField):
        """Tick回调后执行函数"""
        # 解析时间
        if not md.ActionDay:
            return

        tick = md_to_tick(md)
        if tick:
            self.gateway.on_tick(tick)
            # Count tick
            if md.InstrumentID in self._subscribed:
                self._subscribed[md.InstrumentID] += 1
            else:
                self._subscribed[md.InstrumentID] = 1


class CtpTickClean:
    """
    Clean Future tick data from CTP API

    Note:
        - Amount / Volume / Open / High / Low / Close / AveragePrice refer to cumulative value within the trade day
        - Data frequency:
            - usually 2 tick slice / second for CFFEX/DCE/SHFE/INE
            - CZCE: multiple uncertain tick slice / second
    """
    COMMODITY_TIME = [(0, 9000), (32400, 36900), (37800, 41400), (48600, 54000), (75600, 86400)]
    FIN_TIME = [(34200, 41400), (46800, 54000)]

    def __init__(self, logger=None, call_auction: bool = True):
        self._logger = logger

        self._use_callauc: bool = call_auction

        self._day_map: dict = {}                                # 自然日对应的交易日时间
        self._time_map: dict = {}                               # 合约交易时间

        self._ticktime_count: dict = {}                        # Count on the same ticktime for CZCE symbols
        self._last_ticktime: dict = {}                         # Latest tick time for CZCE symbols

        self.on_init()

    def INFO(self, msg: str):
        if self._logger is not None:
            self._logger.INFO(msg)
        else:
            print(f"[CtpGateway | Cleaner] [INFO] {msg}")

    def ERROR(self, msg: str):
        if self._logger is not None:
            self._logger.ERROR(msg)
        else:
            print(f"[CtpGateway | Cleaner] [ERROR] {msg}")

    def on_init(self):
        self.init_tradeday()

    def init_tradeday(self):
        """Generate daytime-related variables"""
        start = "2011-01-01"
        end = datetime.now() + timedelta(days=30)
        day_map = pd.DataFrame(index=all_calendar(start, end, fmt="int"))
        trade_days = all_tradeday(start, end, fmt="int")
        day_map = day_map.join(pd.DataFrame(trade_days, trade_days, columns=["DayTD"]))
        day_map["PrevTD"] = day_map["DayTD"].shift(1).ffill()
        day_map["NightTD"] = day_map["DayTD"].shift(-1).bfill()
        day_map["DayTD"] = day_map["DayTD"].bfill()
        self._day_map = day_map.dropna().astype(int).T.to_dict()

    def clean_tick(self, tick: TickData) -> Union[bool, TickData]:
        """Main API"""
        if tick.instrument not in self._time_map:
            self.ERROR(f"合约未注册：{tick.instrument}，请先调用register方法注册合约")
            return False

        valid_t = self.filter_time_invalid(tick)
        valid_p = self.filter_price_invalid(tick)
        if (not valid_t) or (not valid_p):
            return False
        tick = self.adj_time(tick=tick)
        tick = self.adj_amount(tick=tick)
        tick = self.adj_average_price(tick=tick)

        return tick

    def register(self, ticker: str, info: Union[FutureInfo, OptionInfo] = None, trade_time: Union[dict, list] = None):
        """注册待清洗合约。交易时间可选，未指定则使用默认时间"""
        instrument = ticker_to_instrument(ticker)
        if instrument not in INSTRUMENT_INFO:
            if info:
                INSTRUMENT_INFO[instrument] = info
            else:
                self.ERROR("合约信息缺失，必须提供标准信息类才能订阅合约清洗")

        if instrument not in self._time_map:
            info = INSTRUMENT_INFO[instrument]
            if trade_time is None:
                if info.exchange.value in ("CFFEX", "SSE", "SZSE", "BSE"):
                    trade_time = self.FIN_TIME
                elif info.exchange.value in ("SHFE", "DCE", "CZCE", "GFEX", "INE"):
                    trade_time = self.COMMODITY_TIME
                else:
                    trade_time = self.FIN_TIME + self.COMMODITY_TIME
                    self.ERROR(f"交易所无法识别: {info.exchange.value}，已默认使用全交易时间")
            else:
                if isinstance(trade_time, list) and isinstance(trade_time[0], tuple):
                    trade_time = trade_time[0]
                elif isinstance(trade_time, dict):
                    trade_time = [i for k in trade_time.values() for i in k if isinstance(i, tuple)]
                if not trade_time:
                    self.ERROR(f"交易时间参数格式有误: {instrument}")
                    return False
            self._time_map[instrument] = trade_time
        return True

    def filter_time_invalid(self, tick: TickData):
        """Filter time invalid ticks"""
        valid = False
        ticktime = tick.ticktime
        trade_time = self._time_map[tick.instrument]
        # 计算时间戳秒数
        time_sec = ticktime.hour * 3600 + ticktime.minute * 60 + ticktime.second + ticktime.microsecond / 1e6
        # 循环交易时间区间，确保该tick处在交易时间区间内
        for t_rng in trade_time:
            if t_rng[0] <= time_sec <= t_rng[1]:
                return True
        return valid

    @staticmethod
    def filter_price_invalid(tick: TickData):
        """过滤无效价格"""
        if tick.limitdown <= tick.lastprice <= tick.limitup:
            return True
        else:
            return False

    def adj_time(self, tick: TickData) -> TickData:
        """
        调整时间戳: DateTime / TradeDay / ActionDay
            - DCE: [ActionDay]字段调整为真实日历日期
            - CZCE: [TradeDay]字段调整为交易日日期

        Day map:
            - SHFE / INE / CFFEX / GFEX: [TradeDay] = Real TradeDay, [ActionDay] = Calendar Day
            - DCE: [TradeDay] = [ActionDay] = Real TradeDay
            - CZCE: [TradeDay] = [ActionDay] = Calendar Day

        Millisecond adjust:
            - CZCE: raw tick has no millisecond, add 0/1/2/3 by sequence on the same second

        """
        # Parse information
        exchange = tick.exchange.value.upper()
        if exchange.upper() not in ["CZCE", "DCE"]:
            return tick
        ticktime = tick.ticktime
        tradeday = tick.tradeday
        actionday = tick.actionday
        # 郑商所：调整交易日字段、添加毫秒
        if exchange == "CZCE":
            hour = ticktime.hour
            # 夜盘处理：将真实时间替换为所属交易日
            if hour >= 20:
                tradeday = self._day_map[actionday]["NightTD"]
            elif hour <= 3:
                tradeday = self._day_map[actionday]["DayTD"]
            # Get last ticktime
            instrument = tick.instrument
            if instrument in self._last_ticktime:
                last_ticktime = self._last_ticktime[instrument]
                if last_ticktime == ticktime:
                    ticktime = ticktime.replace(microsecond=ticktime.microsecond + 1000 * self._ticktime_count[instrument])
                    self._ticktime_count[instrument] += 1
                else:
                    self._last_ticktime[instrument] = ticktime
                    self._ticktime_count[instrument] = 1
            else:
                self._last_ticktime[instrument] = ticktime
                self._ticktime_count[instrument] = 1
        # 大商所：调整actionday字段（原为tradeday）
        elif exchange.upper() in ["DCE"]:
            hour = ticktime.hour
            # For night market 21:00~24:00 / 00:00~3:00
            if hour >= 20 or hour <= 3:
                actionday = datetime.strptime(str(self._day_map[tradeday]["PrevTD"]), "%Y%m%d")
                if hour <= 3:
                    actionday += timedelta(days=1)
                ticktime = actionday.replace(hour=ticktime.hour, minute=ticktime.minute,
                                             second=ticktime.second, microsecond=ticktime.microsecond)
                actionday = int(actionday.strftime("%Y%m%d"))
        # Update tick data
        tick.ticktime = ticktime
        tick.tradeday = tradeday
        tick.actionday = actionday
        return tick

    @staticmethod
    def adj_amount(tick: TickData) -> TickData:
        """
        Adjust amount value

        - CZCE: multiply by multiplier
        """
        instrument = tick.instrument
        exchange = tick.exchange.value

        if exchange.upper() in ["CZCE"]:
            amount = tick.amount
            amount *= INSTRUMENT_INFO[instrument].multiplier
            tick.amount = amount

        return tick

    @staticmethod
    def adj_average_price(tick: TickData) -> TickData:
        """
        Adjust average price value

        - CZCE: No adjust
        - SHFE / CFFEX / DCE / INE: divide by multiplier
        """
        instrument = tick.instrument
        exchange = tick.exchange.value
        info = INSTRUMENT_INFO[instrument]
        tick.ticker = info.ticker
        if exchange not in ["CZCE"]:
            multiplier = info.multiplier
            avg_price = tick.settle / multiplier
            avg_price = round_to(avg_price, info.pricetick)
            tick.settle = avg_price
        return tick


def combine_datetime(d: str, t: str = "", s: str = ""):
    """将交易所字符串格式的日期合成为datetime/int"""
    if not t:
        return int(d[:8])
    else:
        # 解析年月日
        year = int(d[:4])
        month = int(d[4:6])
        day = int(d[6:8])
        # 解析 b (时分秒)
        if t:
            hour = int(t[0:2])
            minute = int(t[3:5])
            second = int(t[6:8])
        else:
            hour = minute = second = 0
        # 解析 c (毫秒)
        microsecond = int(s) * 1000 if s else 0
        # 创建 datetime 对象
        dt = datetime(year, month, day, hour, minute, second, microsecond)
        return int(d[:8]), dt


def md_to_tick(md: Union[mdapi.CThostFtdcDepthMarketDataField, tdapi.CThostFtdcDepthMarketDataField, dict]) -> Union[None, TickData]:
    """Transform md list to TickData"""
    if isinstance(md, dict):
        md["ticktime"] = unify_time(md["ticktime"], mode=7)
        md["tradeday"] = int(md["tradeday"])
        md["actionday"] = int(md["actionday"])

        tick = TickData(**md)
        # Clear data
        for (k, v) in tick.__dict__.items():
            # Invalid data is assigned 1.7976931348623157e+308
            if isinstance(v, (float, int)) and v >= 1e308:
                setattr(tick, k, np.nan)
    elif isinstance(md, (mdapi.CThostFtdcDepthMarketDataField, tdapi.CThostFtdcDepthMarketDataField)):
        # 获取变量
        product = INSTRUMENT_INFO[md.InstrumentID].product
        exchange = INSTRUMENT_INFO[md.InstrumentID].exchange.value
        # 解析时间
        action_day, action_datetime = combine_datetime(md.ActionDay, md.UpdateTime, md.UpdateMillisec)
        if not md.TradingDay and exchange == "SHFE":
            md.TradingDay = datetime.now().strftime("%Y%m%d")
        trade_day = combine_datetime(md.TradingDay)
        # 生成tick数据
        tick = TickData(ticktime=action_datetime,
                        tradeday=trade_day,
                        actionday=action_day,
                        instrument=md.InstrumentID,
                        product=product,
                        exchange=exchange,
                        lastprice=adjust_price(md.LastPrice),
                        bid1=adjust_price(md.BidPrice1),
                        ask1=adjust_price(md.AskPrice1),
                        bidvolume1=md.BidVolume1,
                        askvolume1=md.AskVolume1,
                        open=adjust_price(md.OpenPrice),
                        high=adjust_price(md.HighestPrice),
                        low=adjust_price(md.LowestPrice),
                        settle=adjust_price(md.AveragePrice),
                        limitup=md.UpperLimitPrice,
                        limitdown=md.LowerLimitPrice,
                        volume=md.Volume,
                        amount=md.Turnover,
                        openinterest=md.OpenInterest,
                        prevclose=adjust_price(md.PreClosePrice),
                        prevsettle=adjust_price(md.PreSettlementPrice),
                        prevopeninterest=md.PreOpenInterest
                        )
        if md.BidVolume2 or md.AskVolume2:
            tick.bid2 = adjust_price(md.BidPrice2)
            tick.bid3 = adjust_price(md.BidPrice3)
            tick.bid4 = adjust_price(md.BidPrice4)
            tick.bid5 = adjust_price(md.BidPrice5)

            tick.ask2 = adjust_price(md.AskPrice2)
            tick.ask3 = adjust_price(md.AskPrice3)
            tick.ask4 = adjust_price(md.AskPrice4)
            tick.ask5 = adjust_price(md.AskPrice5)

            tick.bidvolume2 = adjust_price(md.BidVolume2)
            tick.bidvolume3 = adjust_price(md.BidVolume3)
            tick.bidvolume4 = adjust_price(md.BidVolume4)
            tick.bidvolume5 = adjust_price(md.BidVolume5)

            tick.askvolume2 = adjust_price(md.AskVolume2)
            tick.askvolume3 = adjust_price(md.AskVolume3)
            tick.askvolume4 = adjust_price(md.AskVolume4)
            tick.askvolume5 = adjust_price(md.AskVolume5)
    else:
        return None
    return tick


def tick_to_md(tick: TickData) -> dict:
    """将TickData解析为csv存储格式"""
    d = tick.__dict__
    d["ticktime"] = tick.ticktime.strftime("%Y%m%d %H:%M:%S") + f":{int(tick.ticktime.microsecond / 1000)}"
    return d
