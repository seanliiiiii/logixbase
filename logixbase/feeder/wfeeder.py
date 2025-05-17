import re
from datetime import datetime, timedelta
import pandas as pd
from pathlib import Path
from typing import Union

from .base import BaseFeeder
from ..utils import unify_time, DealWithOracle, DatabaseConfig
from ..configer import read_config
from ..trader import PRODUCT_NAME_MAP

LOCAL_DIR = Path(__file__).parent


def map_exchange(symbol: str,
                 wind_exchange: dict):
    """Map normal exchange to wind exchange tag"""
    wind_code = symbol
    for exchange, v in wind_exchange.items():
        if symbol.endswith(exchange):
            wind_code = wind_code.replace(exchange, v)
            break
    return wind_code


class WindOracleFeeder(BaseFeeder):
    """
    Extract data from wind oracle database
    """

    def __init__(self, conn_cfg: DatabaseConfig, logger = None):
        super().__init__("WindOracle", logger)

        self._conn = DealWithOracle(conn_cfg)

        self._table: dict = {}
        self._exchange: dict = {}

        self.on_init()


    def on_init(self):
        """"""
        cfg = read_config(LOCAL_DIR / "config/wind.yaml")
        self._table = cfg["table"]
        self._exchange = cfg["exchange"]

    @property
    def login_status(self):
        return True

    def connect(self):
        """Establish connection to WindOracle"""
        self._conn.connect()

    def disconnect(self):
        """Close wind oracle connection"""
        self._conn.disconnect()

    def check_login_status(self):
        """Check connect status to ensure connection"""
        self._conn.connect()

    def split_info_byasset(self, info: pd.DataFrame):
        """Split given ticker info table by asset: commodity, equity, treasury"""
        com_exg = self._exchange["class"]["COM_EXCHANGE"]
        fin_exg = self._exchange["class"]["FIN_EXCHANGE"]

        commodities = info[info['Exchange'].isin(com_exg)]
        equities = info[(info['Exchange'].isin(fin_exg)) & (info['Code'].str.startswith('I'))]
        treasury = info[(info['Exchange'].isin(fin_exg)) & (info['Code'].str.startswith('T'))]

        asset_info = {"Commodity": commodities,
                      "Equity": equities,
                      "Treasury": treasury}

        return asset_info

    def trade_day(self, day: datetime, n: int = 1, exchange: str = "SHFE"):
        """Return specific N-th datetime format trade date prior/after given date"""
        self.check_login_status()
        # Format input date
        day_ = unify_time(day, fmt="str", mode=3, dot="")
        # Query data from wind oracle
        if n > 0:
            symbol = f"TRADE_DAYS > {day_} order by TRADE_DAYS asc"
        else:
            symbol = f"TRADE_DAYS < {day_} order by TRADE_DAYS desc"

        query = f"""
                SELECT TRADE_DAYS FROM
                (
                 FROM CFUTURESCALENDAR
                 WHERE S_INFO_EXCHMARKET = '{exchange}' AND {symbol})
                 WHERE ROWNUM <= '{abs(n)}' ORDER BY ROWNUM
                 """
        trade_dates = self._conn.exec_query(query)

        if trade_dates is None or len(trade_dates) == 0:
            return False

        # Get target trading date
        trade_day = str(trade_dates.loc[:, 'TRADE_DAYS'].values[-1])
        trade_day = unify_time(trade_day, mode=3)

        return trade_day

    def all_trade_day(self, start: Union[str, datetime, int], end: Union[str, datetime, int], fmt: str = "datetime",
                      freq: str = "D", day: int = 1, exchange: str = "SHFE"):
        """Get trading date series spanning begin to end"""
        self.check_login_status()
        # Format input dates
        start_ = unify_time(start, fmt="str", mode=3, dot="")
        end_ = unify_time(end, fmt="str", mode=3, dot="")

        # Query data from wind oracle
        query = f"""
                SELECT TRADE_DAYS
                FROM CFUTURESCALENDAR
                WHERE S_INFO_EXCHMARKET = '{exchange}' AND TRADE_DAYS >= '{start_}'
                               AND TRADE_DAYS <= '{end_}'
                ORDER BY TRADE_DAYS
                """
        trade_dates = self._conn.exec_query(query)
        trade_dates.loc[:, 'TRADE_DAYS'] = trade_dates.loc[:, 'TRADE_DAYS'].map(unify_time)

        if freq == 'D':
            trade_dates = trade_dates.loc[:, 'TRADE_DAYS'].tolist()
            return trade_dates
        elif freq == 'W':
            trade_dates.loc[:, 'Year'] = trade_dates['TRADE_DAYS'].map(lambda x: x.year)
            trade_dates.loc[:, 'Week'] = trade_dates['TRADE_DAYS'].map(lambda x: x.week)
            trade_dates = trade_dates.groupby(['Year', 'Week'])['TRADE_DAYS']
        elif freq == 'M':
            trade_dates.loc[:, 'Year'] = trade_dates['TRADE_DAYS'].map(lambda x: x.year)
            trade_dates.loc[:, 'Month'] = trade_dates['TRADE_DAYS'].map(lambda x: x.month)
            trade_dates = trade_dates.groupby(['Year', 'Month'])['TRADE_DAYS']

        if day == 1:
            trade_dates = sorted(trade_dates.max().tolist())
        elif day == 0:
            trade_dates = sorted(trade_dates.min().tolist())
        else:
            raise ValueError("Only surrport Frequency's first/last data! Please enter correct day parameter: 1 or 0")

        return trade_dates

    def stock_management_board(self, ticker: str, start: datetime, condition: str = ""):
        """Get management board names and info"""
        self.check_login_status()
        # Format time
        start_ = unify_time(start, fmt="str", mode=3, dot="")

        # Query data from wind oracle
        query = f"""
                SELECT S_INFO_WINDCODE AS Ticker, ANN_DATE AS AnnDate, S_INFO_MANAGER_NAME AS Name,
                 S_INFO_MANAGER_GENDER AS Gender, S_INFO_MANAGER_BIRTHYEAR AS Birthday,
                 S_INFO_MANAGER_STARTDATE AS StartDate, S_INFO_MANAGER_LEAVEDATE AS QuitDate,
                 S_INFO_MANAGER_POST AS Title
                 FROM AShareManagement
                 WHERE S_INFO_MANAGER_STARTDATE >= '{start_}' AND S_INFO_WINDCODE = '{ticker}'
                 {'AND ' + condition if condition else ""}
                """
        info = self._conn.exec_query(query)

        return info

    def asset_info(self, asset: str, info: str = "basic"):
        self.ERROR("WindOracle的资产信息获取暂不支持")
    def asset_quote(self, asset: str, start: Union[datetime, str, int], end: Union[datetime, str, int],
                    interval: Union[str, int], product: Union[list, tuple] = None, ticker: Union[list, tuple] = None,
                    underlying: Union[list, tuple] = None, main: bool = True):
        self.ERROR("WindOracle的行情数据获取暂不支持")

    def asset_ticker(self, asset: str, active: bool):
        self.ERROR("WindOracle的合约信息获取暂不支持")

    def future_info_basic(self, codes: list = None, contract: list = None):
        """
        Get futures basic info of given codes
        input: list of codes(short string ticker without exchange info)
        """
        self.check_login_status()

        if codes:
            codes_ = ','.join([x.upper() for x in codes])
            condition = f"AND a.S_INFO_CODE in ({codes_})"
        elif contract:
            contract_ = [k for (k, v) in PRODUCT_NAME_MAP.items() if v in contract] + contract
            contract_ = ",".join(["'" + str(x) + "'" for x in contract_])
            condition = f" AND a.FS_INFO_SCCODE in ({contract_})"
        else:
            condition = ""

        # Query data from wind oracle
        query = f"""
                SELECT a.S_INFO_CODE AS Code, a.S_INFO_WINDCODE AS WindCode,
                       a.FS_INFO_SCCODE AS Contract, a.S_INFO_LISTDATE AS ListDate,
                       a.S_INFO_DELISTDATE AS DelistDate, a.FS_INFO_LTDLDATE AS DeliverDate,
                       a.FS_INFO_DLMONTH AS DeliverMonth, b.S_INFO_PUNIT, AS Multiplier,
                       b.S_INFO_MFPRICE AS TickSize, a.S_INFO_EXCHMARKET AS Exchange
                 FROM CFUTURESDESCRIPTION a LEFT JOIN CFUTURESCONTPRO b
                 ON a.S_Info_Windcode = b.S_Info_Windcode
                 WHERE a.FS_INFO_TYPE = 1
                 {condition}
                """
        basic_info = self._conn.exec_query(query)

        if basic_info.shape[0] != 0:
            basic_info['Ticker'] = basic_info['Contract'] + basic_info['DeliverMonth'].map(lambda x: x[-4:])
            basic_info = basic_info[~(basic_info['DelistDate'].isnull() & basic_info['ListDate'].isnull())]
            # Deal with missing datetime data
            for tag in ['ListDate', 'DelistDate']:
                mask = basic_info[tag] == 0
                info_ = basic_info[mask]
                dt_ = ["20" + re.findall(r"\d+", x)[0] + "15" for x in info_['Ticker']]

                if tag == 'ListDate':
                    dt_ = [(unify_time(x) + timedelta(days=-365)).strftime("%Y%m%d") for x in dt_]

                dt_ = [int(x) for x in dt_]

                if dt_:
                    basic_info.loc[mask, tag] = dt_

            cols = ['ListDate', 'DelistDate', 'DeliverDate']
            basic_info.loc[:, cols] = basic_info.loc[:, cols].applymap(lambda x: unify_time(x, fmt="str", mode=3))
            basic_info['TickSize'] = basic_info['TickSize'].map(lambda x: re.findall(r"\d+\.?\d*", x)[0])

        else:
            basic_info = pd.DataFrame(columns=['Code', 'WindCode', 'Contract', 'ListDate', 'DelistDate',
                                               'DeliverDate', 'Multiplier', 'TickSize', 'Exchange'])
        return basic_info

    def future_info_poslimit(self, codes: list = None, contracts: list = None):
        """Get position_limit of futures contracts"""
        self.check_login_status()

        if codes:
            codes_ = ','.join([x.upper() for x in codes])
            condition = f"AND a.S_INFO_CODE in ({codes_})"
        elif contracts:
            contract_ = [k for (k, v) in PRODUCT_NAME_MAP.items() if v in contracts] + contracts
            contract_ = ",".join(["'" + str(x) + "'" for x in contract_])
            condition = f" AND a.FS_INFO_SCCODE in ({contract_})"
        else:
            condition = ""

        # Query data from wind oracle
        query = f"""
                SELECT b.FS_INFO_SCCODE AS Contract, b.S_INFO_CODE AS Code, a.S_INFO_WINDCODE AS WindCode,
                a.POSITION_LIMIT_TYPE AS PosLimitType, a.CHANGE_DT AS ExecDate,
                a.POSITION_LIMIT AS PosLimit_Fix, a.POSITION_LIMIT_RATIO AS PosLimit_Ratio,
                a.CHANGE_REASON AS Note, b.FS_INFO_DLMONTH AS DeliverMonth
                FROM CFuturesPositionLimit a LEFT JOIN CFUTURESDESCRIPTION b
                ON a.S_INFO_WINDCODE = b.S_INFO_WINDCODE
                WHERE b.FS_INFO_TYPE = 1 {condition}
                 """

        pos_limit = self._conn.exec_query(query).fillna(0)

        pos_limit['Ticker'] = pos_limit['Contract'] + pos_limit['DeliverMonth'].map(lambda x: x[-4:])
        pos_limit['ExecDate'] = pos_limit['ExecDate'].map(lambda x: unify_time(x, fmt="str", mode=3))

        return pos_limit

    def future_ticker_main(self, contract: str, today: datetime = datetime.today()):
        """Get future contract's last main ticker"""
        self.check_login_status()
        # Format date
        today_ = unify_time(today, fmt="str", mode=3, dot="")
        # Query data from Oracle
        query = f"""
                 SELECT S_INFO_WINDCODE AS WindCode
                 FROM CFUTURESDESCRIPTION WHERE FS_INFO_SCCODE = '{contract}'
                 """
        last_main = self._conn.exec_query(query)
        contract_ = contract + '.' + last_main.iloc[0, 0].split('.')[1]

        query = f"""
                 SELECT FS_MAPPING_WINDCODE
                 FROM CFUTURESCONTRACTMAPPING T
                 WHERE S_INFO_WINDCODE = '{contract_}' AND ENDDATE >= '{today_}'
                 """

        code = self._conn.exec_query(query).iat[0, 0].split('.')[0]
        return code

    def future_ticker_continuous(self, wind_contracts: list, start: datetime, end: datetime):
        """Query continuous calendar tickers from wind db"""
        self.check_login_status()
        # Union date format
        start_ = unify_time(start, fmt="str", mode=3, dot="")
        end_ = unify_time(end, fmt="str", mode=3, dot="")

        calendar_tickers = pd.DataFrame(index=self.all_trade_day(start_, end_))

        for contract in wind_contracts:
            # Query raw data table from Oracle database
            query = f"""
                     SELECT S_INFO_WINDCODE, FS_MAPPING_WINDCODE, STARTDATE, ENDDATE
                     FROM CfuturesContractMapping
                     WHERE S_INFO_WINDCODE = '{contract}' AND ENDDATE >= '{start_}'
                     AND STARTDATE <= '{end_}'
                     """
            contract = map_exchange(contract, self._exchange["map"])
            data = self._conn.exec_query(query)
            calendar_tickers[contract] = contract

            for i in range(data.shape[0]):
                contract, ticker, ticker_start, ticker_end = data.iloc[i]
                ticker_start = unify_time(ticker_start, mode=3)
                ticker_end = unify_time(ticker_end, mode=3)
                calendar_tickers.loc[ticker_start:ticker_end, contract] = ticker
        calendar_tickers.columns = [x.split('.')[0] for x in calendar_tickers.columns]
        calendar_tickers = calendar_tickers.applymap(lambda x: x.split('.')[0])

        return calendar_tickers

    def futurequote_daily_spec(self, wind_codes: list, start: datetime, end: datetime, table: str):
        """Get daily quote of specific exchange"""
        self.check_login_status()
        # Unify date format
        start_ = unify_time(start, fmt="str", mode=3, dot="")
        end_ = unify_time(end, fmt="str", mode=3, dot="")

        future_quote = pd.DataFrame()
        for i in range(0, len(wind_codes), 1000):
            code_temp = wind_codes[i:i + 1000]
            code_temp = ','.join(["'" + x.upper() + "'" for x in code_temp])

            # Query data from Oracle database
            query = f"""
                     SELECT TRADE_DT AS Date, S_INFO_WINDCODE AS WindCode, S_DQ_OPEN AS Open,
                     S_DQ_HIGH AS High, S_DQ_LOW AS Low, S_DQ_CLOSE AS Close, S_DQ_SETTLE AS Settle,
                     S_DQ_PRESETTLE AS PrevSettle, S_DQ_VOLUME AS Volume, S_DQ_AMOUNT AS Amount,
                     S_DQ_OI AS OpenInterest
                     FROM {table}
                     WHERE TRADE_DT >= '{start_}' AND TRADE_DT <='{end_}' AND S_INFO_WINDCODE IN ({code_temp})
                     """

            data = self._conn.exec_query(query)

            future_quote = pd.concat([future_quote, data], ignore_index=True)

        return future_quote

    def future_quote_daily(self, start: datetime, end: datetime, codes: list = None):
        """Get futures daily quote data spanning start to end of given tickers"""
        self.check_login_status()

        # Generate ticker info table
        ticker_info = self.future_info_basic()
        if codes is not None:
            ticker_info = ticker_info[ticker_info['Ticker'].isin(codes)]

        # Split target tickers to specific tables
        asset_info = self.split_info_byasset(ticker_info)

        # Generate trade date series spanning start to end
        trade_dates = self.all_trade_day(start, end)

        daily_quote = pd.DataFrame()
        for today in trade_dates:
            for asset, info in asset_info.items():
                mask = (info['ListDate'] <= today) & (info['DelistDate'] >= today)
                today_tickers = info.loc[mask, 'WindCode'].tolist()
                if today_tickers:
                    quote_temp = self.futurequote_daily_spec(today_tickers, today, today, table=self._table[asset])
                    daily_quote = pd.concat([daily_quote, quote_temp], ignore_index=True)

        daily_quote = pd.merge(daily_quote, ticker_info[['WindCode', 'Code', 'Contract']], on='WindCode', how='left')

        return daily_quote.sort_values(['Date', 'WindCode'], inplace=True)

    def future_memberhold(self, codes: list = None, contracts: list = None, start: datetime = None,
                          end: datetime = None, info_types=None, rank=None):
        """Return daily holding pos of futures company"""
        if info_types is None:
            info_types = [2, 3]
        self.check_login_status()
        group_by = "Code"
        # Unify date format
        start_ = unify_time(start, fmt="str", mode=3, dot="")
        end_ = unify_time(end, fmt="str", mode=3, dot="")
        info_type = ','.join(["'" + str(x).upper() + "'" for x in info_types])
        # Get ticker info
        ticker_info = self.future_info_basic(codes=codes, contract=contracts)
        # Split target tickers to specific tables
        asset_info = self.split_info_byasset(ticker_info)

        data = pd.DataFrame(columns=['Code', 'Contract', 'Date', 'Member', 'Type', 'Num', 'Change'])
        for asset, target_temp in asset_info.items():
            table = self._table[asset]

            if target_temp.shape[0] == 0:
                continue

            # Prepare data
            if codes is not None:
                call_data = target_temp['WindCode'].unique().tolist()
                condition = "a.S_INFO_WINDCODE AS Code"
                group_by = 'Code'
            elif contracts is not None:
                call_data = target_temp['Contract'].unique().tolist()
                condition = "b.FS_INFO_SCCODE AS Code"
                group_by = 'Contract'
            else:
                raise NotImplementedError("Input target missing!")
            targets = ','.join(["'" + x.upper() + "'" for x in call_data])
            # Query data from Wind oracle
            query = f"""
                     SELECT {condition}, b.FS_INFO_SCCODE AS Contract, a.TRADE_DT AS Date,
                     a.FS_INFO_MEMBERNAME AS Member, a.FS_INFO_TYPE AS Type,
                     sum(a.FS_INFO_POSITIONSNUM) AS Num, sum(a.S_OI_POSITIONSNUMC) AS Change
                     FROM ({table} a LEFT JOIN CFUTURESDESCRIPTION b
                           ON a.S_INFO_WINDCODE=b.S_INFO_WINDCODE)
                     WHERE a.TRADE_DT >= '{start_}' and a.TRADE_DT <= '{end_}' and {condition} in ({targets})
                     and a.FS_INFO_TYPE in ({info_type})
                     GROUP BY {condition}, b.FS_INFO_SCCODE, a.TRADE_DT, a.FS_INFO_MEMBERNAME, a.FS_INFO_TYPE
                     """
            data_temp = self._conn.exec_query(query)
            data = pd.concat([data, data_temp])
        # Adjust data format
        data['Date'] = data['Date'].map(unify_time)

        # Update contract label to latest
        for k in PRODUCT_NAME_MAP:
            data['Code'] = [i.replace(k, PRODUCT_NAME_MAP[k]) for i in data['Code']]
            data['Contract'] = [i.replace(k, PRODUCT_NAME_MAP[k]) for i in data['Contract']]
        if codes is not None:
            wind_to_ticker = ticker_info.set_index('Windcode')['Code'].to_dict()
            data['Code'] = data['Code'].map(lambda x: wind_to_ticker[x])

        # Append rank label
        data['Num'] = data['Num'].astype(float)
        data['Type'] = data['Type'].astype(int)
        data['Rank'] = data.groupby(['Date', group_by, 'Type'])['Num'].rank(method='min', ascending=False)
        if rank is not None:
            data = data.query("rank <= %s" % rank)

        return data

    def cheapest_bond(self, codes: list = None, contracts: list = None, start: str = None, end: str = None):
        """Return info of cheapest deliverable bond for treasury bonds futures"""
        self.check_login_status()

        # Union date format
        start_ = unify_time(start, fmt="str", mode=3, dot="")
        end_ = unify_time(end, fmt="str", mode=3, dot="")
        # Get ticker info
        ticker_info = self.future_info_basic(codes=codes, contract=contracts)
        target = ticker_info['WindCode'].unique().tolist()
        targets = ','.join(["'" + x.upper() + "'" for x in target])
        # Query data from oracle wind
        query = f"""
                 SELECT S_INFO_WINDCODE AS WindCode, TRADE_DT AS Date, CTD_WINDCODE, CTD_IRR,
                        IB_CTD_WINDCODE, IB_CTD_IRR, SH_CTD_WINDCODE, SH_CTD_IRR, SZ_CTD_WINDCODE,
                        SZ_CTD_IRR
                 FROM CbondFCTD
                 WHERE S_INFO_WINDCODE in ({targets}) and TRADE_DT >= {start_} and TRADE_DT <= {end_}
                """
        data = self._conn.exec_query(query)
        # Prepare data
        irr_list = data.loc[:, ['CTD_IRR', 'IB_CTD_IRR', 'SH_CTD_IRR', 'SZ_CTD_IRR']].values.tolist()
        bond_names = data.loc[:, ['CTD_WINDCODE', 'IB_CTD_WINDCODE', 'SH_CTD_WINDCODE',
                                  'SZ_CTD_WINDCODE']].values.tolist()
        data['Bond'] = [bond_names[i][x.index(max(x))] for i, x in enumerate(irr_list)]
        data['IRR'] = [max(x) for x in irr_list]
        data = data.loc[:, ['WindCode', 'Date', 'Bond', 'IRR']]
        wind_to_contract = ticker_info.set_index('WindCode')['Contract'].to_dict()
        data.loc[:, 'Contract'] = data.loc[:, 'WindCode'].map(lambda x: wind_to_contract[x])
        wind_to_ticker = ticker_info.set_index('WindCode')['Code'].to_dict()
        data.loc[:, 'Code'] = data.loc[:, 'WindCode'].map(lambda x: wind_to_ticker[x])

        return data

    def treasury_basis(self, contracts: list = None, codes: list = None, start: datetime = None,
                       end: datetime = None):
        """Get basis of cheapest treasury bond's basis"""
        self.check_login_status()
        # Query cheapest deliverable treasury
        target_bonds = self.cheapest_bond(contracts=contracts, codes=codes, start=start, end=end)
        target_bonds['Condition'] = target_bonds['WindCode'] + '-' + target_bonds['Date'] + '-' + target_bonds['Bond']
        condition = target_bonds.loc[:, 'Condition'].unique().tolist()

        data = pd.DataFrame()
        for i in range(0, len(condition), 1000):
            condition_temp = condition[i:i + 1000]
            condition_temp = ','.join(["'" + x.upper() + "'" for x in condition_temp])

            # Query market data
            query = f"""
                    SELECT S_INFO_WINDCODE AS WindCode, DLS_WINDCODE AS BondCode, TRADE_DT AS Date,
                            DL_INTEREST as DlInterest, INTERVALINTEREST AS IntervalInterest,
                            DL_COST AS DlCost, FS_SPREAD AS FsSpread, IRR, RT_SPREAD AS Basis
                    FROM CbondFValuation
                    WHERE S_INFO_WINDCODE||'-'||TRADE_DT||'-'||DLS_WINDCODE in ({condition_temp})
                    """
            data_temp = self._conn.exec_query(query)
            data = pd.concat([data, data_temp])

        data['Date'] = data['Date'].map(unify_time)

        # Update contract label to latest
        wind_to_ticker = target_bonds.set_index('WindCode')['Code'].to_dict()
        data['Code'] = data.loc[:, 'WindCode'].map(lambda x: wind_to_ticker[x])
        wind_to_contract = target_bonds.set_index('WindCode')['Contract'].to_dict()
        data['Contract'] = data.loc[:, 'WindCode'].map(lambda x: wind_to_contract[x])

        for k in PRODUCT_NAME_MAP:
            data['Code'] = [i.replace(k, PRODUCT_NAME_MAP[k]) for i in data['Code']]
            data['Contract'] = [i.replace(k, PRODUCT_NAME_MAP[k]) for i in data['Contract']]

        data = data.loc[:, ['Date', 'Contract', 'Code', 'WindCode', 'BondCode', 'DlInterest',
                            'IntervalInterest', 'DlCost', 'FsSpread', 'IRR', 'Basis']]
        return data


class DataSeedWindAPI(BaseFeeder):
    """
    Fetch data from wind via api.
    """

    def __init__(self, logger = None):
        super().__init__("WindApi", None, logger)
        self._api = None

        self._table: dict = {}
        self._exchange: dict = {}

        self.on_init()

    def on_init(self):
        """Initialize wind api configs"""
        # Parse configs
        cfg = read_config(LOCAL_DIR / "config/wind.yaml")
        self._table = cfg["table"]
        self._exchange = cfg["exchange"]

    def connect(self):
        """Start wind api"""
        self.INFO("初始化万得API连接")
        # Load official wind api module
        exec("from WindPy import w")
        exec("self._api = w")
        self._api.start()
        if self.login_status:
            self.INFO("万得API连接成功")
        else:
            self.ERROR("万得API连接失败")

    def disconnect(self):
        """Close wind api"""
        if self._api.isconnected():
            self._api.stop()
        self.INFO("万得连接已断开")

    @property
    def login_status(self):
        return self._api.isconnected()


    def check_login_status(self):
        """Check login status of wind api and ensure connection"""
        if not self.login_status:
            self._api.start()

    def all_trade_day(self, start: datetime, end: datetime, frequency: str = 'D', fmt: str = "datetime",
                      freq: str = "D", day: int = 1, market: str = "China"):
        """Get trading dates (W:Weekly last/ M: Monthly last/ D: Daily)"""
        self.check_login_status()
        # Format datetime
        start_ = unify_time(start, fmt="str", mode=3, dot="")
        end_ = unify_time(end, fmt="str", mode=3, dot="")
        # Query trade dates from wind api
        dl = self._api.tdays(start_, end_, f"Period = {frequency}")
        if dl.Data:
            trade_dates = dl.Data[0]
            return trade_dates
        else:
            self.ERROR("交易日期获取失败")

    def trade_day(self, day: datetime, n: int = -1):
        """Get specified previous/afterwards N-th trade date on specific date"""
        self.check_login_status()
        # Format union date
        day_ = unify_time(day, fmt="str", mode=3, dot="")
        # Query data from wind
        dl = self._api.tdaysoffset(n, day_, "")

        if dl.ErrorCode == 0:
            return dl.Data[0][0]
        else:
            self.ERROR(f"指定交易日期获取失败: {day}")

    def asset_ticker(self, asset: str, active: bool):
        asset = asset.lower()
        if asset not in ["stock"]:
            self.ERROR("资产类型无法识别：仅支持stock（股票）")
        tickers = eval(f"self.{asset}_ticker")(active)
        self.INFO(f"{asset.capitalize()}资产{'活跃' if active else '全部'}合约获取完成: {int(len(tickers))}条记录")
        return tickers

    def stock_ticker(self, day: datetime = datetime.now()):
        """Get the tickers of all A-share equities today  """
        self.check_login_status()
        # Format union time
        day_ = unify_time(day, fmt="str", mode=3, dot="")
        # Query data from wind
        dl = self._api.wset("sectorconstituent", f"date = {day_}; sectorid = a001010100000000")
        if dl.ErrorCode == 0:
            tickers = pd.DataFrame(dl.Data, index=['Date', 'Ticker', 'Contract']).T
            tickers.loc[:, 'Date'] = tickers.loc[:, 'Date'].map(unify_time)
            return tickers
        else:
            self.ERROR("A股指定交易日所有合约代码获取失败")

    def asset_quote(self, asset: str, start: Union[datetime, str, int], end: Union[datetime, str, int],
                    interval: Union[str, int], product: Union[list, tuple] = None, ticker: Union[list, tuple] = None,
                    underlying: Union[list, tuple] = None, main: bool = True):
        self.ERROR("暂不支持万得API获取行情数据")

    def asset_info(self, asset: str, info: str = "basic"):
        self.ERROR("暂不支持万得API获取基础信息")

    def edb_data(self, start: datetime, end: datetime, tickers: list):
        """Get fundamental data from Wind EDB"""
        self.check_login_status()
        # Format time
        start_ = unify_time(start, fmt="str", mode=3, dot="")
        end_ = unify_time(end, fmt="str", mode=3, dot="")
        # Query data from wind
        tickers_ = ','.join(tickers)
        windata = self._api.edb(tickers_, start_, end_)
        if windata.ErrorCode == 0:
            data = pd.DataFrame(windata.Data, index=tickers, columns=pd.to_datetime(windata.Times)).T
            data.index.name = 'Date'
            self.INFO(f"EDB数据获取成功: {int(len(data))}条记录")
            return data
        else:
            self.ERROR(f"EDB数据获取失败")
            return False

    def treasury_ctd(self, ticker: str, start: datetime, end: datetime):
        """Query cheapest treasury data"""
        self.check_login_status()
        # Format time
        start_ = unify_time(start, fmt="str", mode=3, dot="")
        end_ = unify_time(end, fmt="str", mode=3, dot="")
        # Query data from wind
        ticker_ = ticker.split(".")[0] + ".CFE"
        dt_con = f"startdate={start_};enddate={end_};"
        ticker_con = f"windcode={ticker_};"
        windata = self._api.wset("ctd", dt_con + ticker_con)
        data = pd.DataFrame(columns=["Date", "Ticker", "CTD", "IRR", "CTD_IB", "IRR_IB",
                                     "CTD_SH", "IRR_SH", "CTD_SZ", "IRR_SZ"])

        if windata.ErrorCode == 0:
            data_ = pd.DataFrame(windata.Data, index=[k for k in data.columns if k != "Ticker"]).T
            data_["Ticker"] = ticker
            data = pd.concat([data, data_])
            self.INFO(f"{ticker}最廉交割券数据获取成功: {int(len(data))}条记录")
        else:
            self.ERROR(f"{ticker}最廉交割券数据获取失败")

        return data

    def treasury_price(self,
                       tickers: list,
                       day: datetime):
        """Query treasury price data: Net Price / Total Price"""
        self.check_login_status()
        # Format time
        day_ = unify_time(day, fmt="str", mode=3, dot="")
        # Query data from wind
        tickers_ = ",".join(tickers).replace(" ", "")
        cols = "yield_cfets,net_cfets,dirty_cfets," \
               "yield_shc,net_shc,dirty_shc,modidura_shc,accruedinterest_shc,vobp_shc,cnvxty_shc"
        dts = f"tradeDate={day_};date={day_}"

        windata = self._api.wss(tickers_, cols, dts)

        data = pd.DataFrame(columns=["Date", "Ticker", "Yield_CFETS", "Clean_CFETS", "Dirty_CFETS",
                                     "Yield_SHC", "Clean_SHC", "Dirty_SHC", "ModifiedDuration_SHC",
                                     "AccruedInterest_SHC", "ValueOnBP_SHC", "Convexity_SHC"])

        if windata.ErrorCode == 0:
            data_ = pd.DataFrame(windata.Data, columns=windata.Codes).T.reset_index()
            data_.columns = data.columns[1:]
            data = pd.concat([data, data_])
            data["Date"] = day
            self.INFO(f"国债现券{day_}数据获取成功: {int(len(data))}条记录")
        else:
            self.ERROR(f"国债现券{day_}数据获取失败")

        return data

    def treasury_tfactor(self, tickers: list):
        """Query treasury transform factor on future tickers"""
        self.check_login_status()

        data = pd.DataFrame(columns=["Ticker", "Treasury", "TFactor"])
        for ticker in tickers:
            ticker_ = ticker.replace(" ", "").split(".")[0] + ".CFE"
            windata = self._api.wset("conversionfactor", f"windcode={ticker_}")

            if windata.ErrorCode == 0:
                data_ = pd.DataFrame(windata.Data, index=["Treasury", "TFactor"]).T
                data_["Ticker"] = ticker.split(".")[0]
                data = pd.concat([data, data_])
                self.INFO(f"国债现券转换因子数据获取成功: {int(len(data))}条记录")
            else:
                self.ERROR(f"国债现券转换因子数据获取失败")

        return data





