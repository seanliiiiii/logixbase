import re
import sys
import numpy as np
from datetime import datetime
from typing import Union
from decimal import Decimal

from ..utils import unify_time
from .config import PRODUCT_NAME_MAP, INSTRUMENT_FORMAT


MAX_FLOAT = sys.float_info.max


def parse_exchange(asset: str, ticker: str):
    instrument = ''.join(filter(str.isdigit, ticker))
    if asset.lower() == "stock":
        if instrument.startswith("0") or instrument.startswith("3"):
            return "SZSE"
        elif instrument.startswith("6"):
            return "SSE"
        else:
            return "BSE"
    elif asset.lower() == "etf":
        if instrument.startswith("5"):
            return "SSE"
        elif instrument.startswith("1"):
            return "SZSE"


def parse_ticker(asset: str, ticker: list):
    """解析传入标准的ticker列表，拆分为具体合约ticker和product列表"""
    if ticker is None:
        return [], []

    asset = asset.lower()
    product_lst = []
    ticker_lst = []
    for _ticker in ticker:
        parts = _ticker.upper().split(".")
        if asset in ("stock", "etf"):
            ticker_lst.append(''.join(re.findall(r'\d+', parts[-1])))
        elif asset == "index":
            ticker_lst.append(parts[-1])
        elif asset == "future":
            if int(len(parts)) == 1:
                ticker_lst.append(parts[0])
            elif int(len(parts)) == 2:
                product_lst.append(parts[1])
            elif parts[-1].upper() == "HOT" or int(len(parts[2])) == 2:
                product_lst.append(parts[1] + "_" + parts[2])
            elif int(len(parts[2])) == 4:
                ticker_lst.append(parts[1] + parts[2])
        elif asset == "option":
            ticker_lst.append("".join(parts[1:]))
        else:
            print(f"资产类型未定义，无法解析标准合约：{asset}")

    return product_lst, ticker_lst


def ticker_formatter(ticker: str):
    """
    调整标准合约代码格式
    """
    parts = ticker.split(".")
    # 交易所代码必须为大写
    parts[0] = parts[0].upper()
    # 股票/指数/ETF的中间标识符必须为大写
    if parts[1].upper() in ("STOCK", "STK", "INDEX", "IDX", "ETF"):
        parts[1] = parts[1].upper()
    elif parts[1].isalpha():
        formatter = INSTRUMENT_FORMAT.get(parts[0], None)
        if not formatter:
            raise ValueError(f"交易所合约格式为定义: {ticker}")
        parts[1] = eval(f"parts[1].{formatter[0]}()")
    return ".".join(parts)


def ticker_to_instrument(ticker: str):
    """
    将标准合约代码转为交易所代码
    :param ticker: 标准代码， exchange.product.tag， e.g CFFEX.IF / CFFEX.IF.2505 / CFFEX.IF.HOT / CFFEX.IF.00
                                                        SSE.600520 / SSE.STK.600520 / SSE.IDX.600520 / SSE.ETF.600520
    :return:
    """
    parts = ticker.upper().split(".")
    tk_len = int(len(parts))

    if tk_len == 1:
        raise ValueError(f"合约代码格式错误：{ticker}")

    exchange = parts[0]
    # Ticker仅2位的情况：只支持股票
    if parts[1] in ("STK", "STOCK") or (parts[-1].isdigit() and int(len(parts[-1])) == 6):
        if int(len(parts[-1])) != 6:
            raise ValueError(f"股票代码必须为6位: {ticker}")
        return parts[-1]
    # 非股票的标准合约代码必须为3位，否则无法转为交易所代码
    elif tk_len == 3:
        # 指数合约：必须具备指定标识
        if parts[1] in ("INDEX", "IDX"):
            return parts[-1]
        # ETF合约：必须具备指定标识
        elif parts[1] == "ETF":
            return ''.join(re.findall(r'\d+', parts[1]))
        # 期货合约格式：exchange.product.4-digit-yearmonth
        elif parts[2].isdigit() and int(len(parts[2])) == 4:
            formater = INSTRUMENT_FORMAT.get(exchange, None)
            if not formater:
                raise ValueError(f"期货交易所合约格式未定义")
            calendar = parts[2][-formater[1]:]
            product = eval(f"parts[1].{formater[0]}()")
            return product + calendar
        # Spread合约格式：exchange.product.calendar1&calendar2(跨期）exchange.product1&product2.calendar1&calendar2
        elif "&" in parts[2] and int(len(parts[2].split("&")[1])) == 4:
            formater = INSTRUMENT_FORMAT.get(exchange, None)
            if not formater:
                raise ValueError(f"期货交易所合约格式未定义")
            products = parts[1].split("&")
            calendars = parts[2].split("&")
            instruments = []
            for i, calendar_ in enumerate(calendars):
                product_ = products[min(i, int(len(products)) - 1)]
                instruments.append(product_ + calendar_[-formater[1]:])
            instrument = "&".join(instruments)
            # 添加套利合约标识
            if exchange == "CZCE":
                return "SPD " + instrument if int(len(products)) == 1 else "IPS " + instrument
            elif exchange in ("DCE", "GFEX"):
                return "SP " + instrument if int(len(products)) == 1 else "SPC " + instrument
            else:
                raise ValueError(f"未定义当前交易所 {exchange} 套利合约规则")
        else:
            raise ValueError(f"标准合约代码有误，无法转为交易所代码：{ticker}")
    else:
        raise ValueError(f"标准合约代码长度有误，无法转为交易所代码：{ticker}")


def ticker_to_product(ticker: str):
    """
    将标准合约代码转为品种代码
    :param ticker: 标准代码， exchange.product.tag， e.g CFFEX.IF / CFFEX.IF.2505 / CFFEX.IF.HOT / CFFEX.IF.00
                                                        SSE.600520 / SSE.STK.600520 / SSE.IDX.600520 / SSE.ETF.600520
    :return:
    """
    parts = ticker.upper().split(".")
    tk_len = int(len(parts))

    if tk_len == 1:
        raise ValueError(f"合约代码格式错误：{ticker}")
    elif tk_len == 2:
        return parts[-1]
    # 非股票的标准合约代码必须为3位，否则无法转为交易所代码
    elif tk_len == 3:
        # 指数合约：必须具备指定标识
        if parts[1] in ("INDEX", "IDX"):
            return parts[2]
        # ETF合约：必须具备指定标识
        elif parts[1] == "ETF":
            return ''.join(re.findall(r'\d+', parts[2]))
        elif parts[1] in ("STK", "STOCK"):
            code = ''.join(re.findall(r'\d+', parts[2]))
            if int(len(code)) != 6:
                raise ValueError(f"股票代码必须为6位：{ticker}")
            return code
        else:
            return parts[1]
    else:
        raise ValueError(f"标准合约代码长度有误，无法转为交易所代码：{ticker}")


def instrument_to_ticker(asset: str, exchange: str, instrument: str, deliver_year: Union[int, str, list] = None):
    """将交易所代码转为完整合约代码"""
    asset = asset.lower()
    exchange = exchange.upper()

    if asset == "future":
        product = instrument_to_product(asset, instrument)
        formater = INSTRUMENT_FORMAT.get(exchange, None)
        if not formater:
            raise ValueError(f"交易所合约代码规则未定义：{exchange}")
        product = eval(f"product.{formater[0]}()")
        calendar = ''.join(re.findall(r'\d+', instrument))
        # 日历补全至4位年月
        if formater[1] != 4 and not deliver_year:
            raise ValueError("合约日历不为4位，需传参合约交割年")
        calendar = str(deliver_year)[:(4 - int(len(calendar)))] + calendar
    elif asset == "option":
        return f"{exchange}.{instrument}"
    elif asset in ("stock", "etf"):
        calendar = ''.join(re.findall(r'\d+', instrument))
        product = "STK" if asset == "stock" else "ETF"
    elif asset == "spread":
        spread = instrument.split(" ")[1]
        spread_instru = spread.replace(" ", "").split("&")
        products = instrument_to_product(asset, instrument).split("&")
        formater = INSTRUMENT_FORMAT.get(exchange, None)
        if not formater:
            raise ValueError(f"交易所合约代码规则未定义：{exchange}")
        calendar = []
        for (i, instru_) in enumerate(spread_instru):
            product_ = products[min(i, int(len(products)) - 1)]
            calendar_ = instru_.lstrip(product_)
            # 补全日历至4位
            if formater[1] != 4:
                if deliver_year:
                    year = deliver_year if isinstance(deliver_year, (int, str)) else deliver_year[min(i, int(len(products)) - 1)]
                    decade = str(year)[2]
                else:
                    decade = str(datetime.now().year)[2]
                calendar_ = decade + calendar_[-3:]
            calendar.append(calendar_)
        calendar = "&".join(calendar)
        product = "&".join(products)
    elif asset == "index":
        product = "IDX"
        calendar = instrument
    else:
        return f"{exchange}.{instrument}"

    return f"{exchange}.{product}.{calendar}"


def instrument_to_product(asset: str, instrument: str):
    """根据交易所代码获取品种"""
    # 去掉字符串前面的空格
    instrument = instrument.strip()
    asset = asset.lower()

    if asset == "future":
        match = re.findall(r"[a-zA-Z]", instrument)
        if match:
            return ''.join(match)
        else:
            return instrument
    elif asset == "spread":
        # 价差合约
        tag, symbol = instrument.split(" ", 1)
        symbols = symbol.split("&")
        matches = [re.search(r"[a-zA-Z]+", k) for k in symbols]
        products = set([k.group(0) for k in matches])
        return "&".join(products)
    elif asset in ("stock", "etf"):
        return ''.join(re.findall(r'\d+', instrument))
    elif asset == "option":
        raise ValueError("期权代码转换未定义")
    else:
        return instrument


def round_to(value: float, target: float) -> float:
    """Round price to price tick value."""
    if np.isnan(value):
        return value
    value = Decimal(str(value))
    target = Decimal(str(target))
    rounded = float(int(round(value / target)) * target)
    return rounded


def floor_to(value: float, target: float) -> float:
    """Similar to math.floor function, but to target float number."""
    if np.isnan(value):
        return value
    value = Decimal(str(value))
    target = Decimal(str(target))
    result = float(int(np.floor(value / target)) * target)
    return result


def ceil_to(value: float, target: float) -> float:
    """Similar to math.ceil function, but to target float number."""
    if np.isnan(value):
        return value
    value = Decimal(str(value))
    target = Decimal(str(target))
    result = float(int(np.ceil(value / target)) * target)
    return result


def calc_daily_bars(dts: Union[list, tuple]):
    """
    Calculate number of bars within a trading day
    :param dts: DataFrame of data with datetime as index
    :return: Int bar numbers
    """
    dts = [unify_time(k) for k in dts]
    bar_num = np.zeros(5)
    n = 1
    day = min(dts[-1].weekday(), 4)
    for i in range(2, int(len(dts))):
        day_ = dts[-i].weekday()
        if day_ >= 5:
            continue
        elif day_ == day:
            n += 1
        else:
            bar_num[day] = n
            day = day_
            n = 1
        if min(bar_num) != 0:
            break
    return int(max(bar_num))


def update_product(instrument: str):
    """Update prior-adjust future symbol to new symbol"""
    label =  "".join(re.findall("[A-Za-z]", instrument))
    adj = PRODUCT_NAME_MAP.get(label.upper(), label)

    if label.islower():
        adj = adj.lower()

    new_label = instrument.replace(label, adj)

    return new_label


def adjust_price(price: float, use: float = np.nan) -> float:
    """将异常的浮点数价格调整为nan"""
    if price == MAX_FLOAT:
        price = use
    return price


def get_calendar_contract(delist_date: dict):
    """Calendar ticker"""
    calen_tickers_list = [k[0] for k in sorted(delist_date.items(), key=lambda k: k[1], reverse=False)]
    nb_ticker = calen_tickers_list[0]
    nb_tk_delist = delist_date[nb_ticker]
    calen_tickers = {"01": nb_ticker}
    for tk in calen_tickers_list[1:]:
        tk_delist = delist_date[tk]
        month_diff = (tk_delist.year - nb_tk_delist.year) * 12 + tk_delist.month - nb_tk_delist.month
        calen_tickers["%.2d" % int(month_diff + 1)] = tk
    return calen_tickers
