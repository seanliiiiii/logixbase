from enum import Enum
import re


class Direction(Enum):
    """Direction of order/trade/position"""
    LONG = "long"
    SHORT = "short"
    NET = "net"


class Offset(Enum):
    """Offset of order/trade."""
    NONE = ""
    OPEN = "open"
    CLOSE = "close"
    CLOSETODAY = "closetoday"
    CLOSEYESTERDAY = "closeyesterday"


class Status(Enum):
    """Order status"""
    SUBMITTING = "submitting"
    NOTTRADED = "nottraded"
    PARTTRADED = "parttraded"
    ALLTRADED = "alltraded"
    CANCELLED = "cancelled"
    REJECTED = "rejected"


class Asset(Enum):
    """Product class."""
    EQUITY = "stock"
    FUTURE = "future"
    OPTION = "option"
    INDEX = "index"
    FOREX = "forex"
    SPOT = "spot"
    ETF = "etf"
    BOND = "bond"
    SPREAD = "spread"
    FUND = "fund"
    COMBINATION = "combination"
    EFP = "efp"
    TAS = "tas"


class OrderType(Enum):
    """Order type."""
    LIMIT = "limit"
    MARKET = "market"
    STOP = "stop"
    FAK = "fak"
    FOK = "fok"


class OptionType(Enum):
    """Option type."""
    CALL = "call"
    PUT = "put"


class OptionExecType(Enum):
    """Option execution type."""
    EUROPEAN = "european"
    AMERICAN = "american"


class StopOrderStatus(Enum):
    WAITING = "waiting"
    CANCELLED = "cancelled"
    TRIGGERED = "triggered"


class Exchange(Enum):
    """Exchange."""
    # Chinese
    CFFEX = "CFFEX"         # China Financial Futures Exchange
    SHFE = "SHFE"           # Shanghai Futures Exchange
    CZCE = "CZCE"           # Zhengzhou Commodity Exchange
    DCE = "DCE"             # Dalian Commodity Exchange
    INE = "INE"             # Shanghai International Energy Exchange
    GFEX = "GFEX"           # Guangzhou Futures Exchange
    SSE = "SSE"             # Shanghai Stock Exchange
    SZSE = "SZSE"           # Shenzhen Stock Exchange
    BSE = "BSE"             # Beijing Stock Exchange
    SGE = "SGE"             # Shanghai Gold Exchange
    WXE = "WXE"             # Wuxi Steel Exchange
    CSI = "CSI"             # 中国指数公司
    SW = "SW"               # 申万研究所

    # Global
    SMART = "SMART"         # Smart Router for US stocks
    NYMEX = "NYMEX"         # New York Mercantile Exchange
    COMEX = "COMEX"         # a division of theNew York Mercantile Exchange
    GLOBEX = "GLOBEX"       # Globex of CME
    IDEALPRO = "IDEALPRO"   # Forex ECN of Interactive Brokers
    CME = "CME"             # Chicago Mercantile Exchange
    ICE = "ICE"             # Intercontinental Exchange
    SEHK = "SEHK"           # Stock Exchange of Hong Kong
    HKFE = "HKFE"           # Hong Kong Futures Exchange
    HKSE = "HKSE"           # Hong Kong Stock Exchange
    SGX = "SGX"             # Singapore Global Exchange
    CBOT = "CBT"            # Chicago Board of Trade
    CBOE = "CBOE"           # Chicago Board Options Exchange
    CFE = "CFE"             # CBOE Futures Exchange
    DME = "DME"             # Dubai Mercantile Exchange
    EUREX = "EUX"           # Eurex Exchange
    APEX = "APEX"           # Asia Pacific Exchange
    LME = "LME"             # London Metal Exchange
    BMD = "BMD"             # Bursa Malaysia Derivatives
    TOCOM = "TOCOM"         # Tokyo Commodity Exchange
    EUNX = "EUNX"           # Euronext Exchange
    KRX = "KRX"             # Korean Exchange

    OANDA = "OANDA"         # oanda.com

    # CryptoCurrency
    BITMEX = "BITMEX"
    OKEX = "OKEX"
    HUOBI = "HUOBI"
    BITFINEX = "BITFINEX"
    BINANCE = "BINANCE"
    BYBIT = "BYBIT"         # bybit.com
    COINBASE = "COINBASE"
    DERIBIT = "DERIBIT"
    GATEIO = "GATEIO"
    BITSTAMP = "BITSTAMP"
    # Special Function
    LOCAL = "LOCAL"         # For local generated data


class Currency(Enum):
    """Currency."""
    USD = "USD"
    HKD = "HKD"
    CNY = "CNY"


class ModelMode(Enum):
    """Model type definition"""
    PATTERN = "pattern_oriented"
    CONTRACT = "contract_oriented"


class Interval:
    """
    Standardized interval representation for bar data.
    Supports:
        - tick
        - sX: second-based bars
        - mX: minute-based bars
        - dX: day-based bars
    """

    _TICK_ALIASES = {"tick"}
    _SECOND_PATTERNS = [r"^s$", r"^sec$", r"^second$", r"^(\d+)s$", r"^s(\d+)$", r"^(\d+)sec$", r"^sec(\d+)$",
                        r"^(\d+)second$", r"^second(\d+)$", r"^(\d+)seconds$",r"^seconds(\d+)$"]
    _MINUTE_PATTERNS = [r"^m$", r"^min$", r"^minute$", r"^(\d+)m$", r"^m(\d+)$", r"^(\d+)min$", r"^min(\d+)$",
                        r"^(\d+)mins$", r"^mins(\d+)$", r"^(\d+)minute$", r"^minute(\d+)$",
                        r"^(\d+)minutes$", r"^minutes(\d+)$"]
    _DAY_PATTERNS = [r"^d$", r"^daily$", r"^day$", r"^(\d+)d$", r"^d(\d+)$", r"^(\d+)day$", r"^day(\d+)$",
                     r"^(\d+)daily$", r"^daily(\d+)$", r"^(\d+)days$", r"^days(\d+)$"]

    def __init__(self, value: str):
        self._pattern: str = ""
        self.original = value.lower().strip()
        self.value, self.window = self._parse(self.original)

    def _parse(self, val: str):
        # Tick
        if val in self._TICK_ALIASES:
            self._pattern = "tick"
            return "tick", 1

        # Seconds
        for pattern in self._SECOND_PATTERNS:
            match = re.fullmatch(pattern, val)
            if match:
                window = 1 if not match.groups() else int(match.group(1))
                self._pattern = "second"
                return f"s{window}", window

        # Minutes
        for pattern in self._MINUTE_PATTERNS:
            match = re.fullmatch(pattern, val)
            if match:
                window = 1 if not match.groups() else int(match.group(1))
                self._pattern = "minute"
                return f"m{window}", window

        # Days
        for pattern in self._DAY_PATTERNS:
            match = re.fullmatch(pattern, val)
            if match:
                window = 1 if not match.groups() else int(match.group(1))
                self._pattern = "day"
                return f"d{window}", window

        raise ValueError(f"Invalid interval format: '{val}'")

    @property
    def pattern(self):
        return self._pattern

    def __str__(self):
        return self.value

    def __repr__(self):
        return f"Interval('{self.value}')"

    def __eq__(self, other):
        if isinstance(other, Interval):
            return self.value == other.value
        elif isinstance(other, str):
            try:
                return self.value == Interval(other).value
            except ValueError:
                return False
        return False

