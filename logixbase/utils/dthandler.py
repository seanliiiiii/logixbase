import re
from datetime import datetime, timedelta
from typing import Union
import pandas as pd
import pandas_market_calendars as mcal


EXCHANGE_MAP = {"China": "SSE"}


def parse_date_str(date_str: str) -> tuple:
    """
    解析日期字符串，并返回年、月、日的元组。

    Args:
        date_str (str): 日期字符串，支持多种格式，如 '2023', '202301', '2023-01', '01-2023', '20230101', '01-01-2023', '01-2023-01'。

    Returns:
        tuple: 包含年、月、日的元组。

    Raises:
        ValueError: 如果日期字符串长度不合法或无法确定具体的日期，则抛出异常。
    """
    month = day = 1
    d_token = date_str.strip()
    non_digit = re.findall(r'\D', d_token)
    # 存在间隔符，则补足月/日至两位
    if non_digit:
        sep = non_digit[0]
        for sep_ in non_digit[1:]:
            d_token = d_token.replace(sep_, sep)
        parts = d_token.split(sep)
        parts = [p.zfill(2) for p in parts]
        d_token = "".join(parts)
    # 纯数日日期
    elif int(len(d_token)) % 2:
        raise ValueError("纯数字日期必须为4/6/8位")
    date_len = int(len(d_token))
    # 仅有年份时
    if date_len == 4:
        year = int(d_token[:4])
    # 有年月时：考虑年-月、月-年两种情况
    elif date_len == 6:
        ym = (int(d_token[:4]), int(d_token[4:6]))  # 年-月
        my = (int(d_token[2:6]), int(d_token[:2]))  # 月-年
        candidate = []
        for (y, m) in (ym, my):
            if m <= 12:
                candidate.append((y, m))
        if int(len(candidate)) > 1:
            raise ValueError(f"无法确定年月：{candidate}")
        year, month = candidate[0]
    # 有年月日时: 年-月-日/月-日-年/日-月-年
    elif date_len == 8:
        # 三种可能组合
        ymd = (d_token[0:4], d_token[4:6], d_token[6:8])
        mdy = (d_token[4:8], d_token[0:2], d_token[2:4])
        dmy = (d_token[4:8], d_token[2:4], d_token[0:2])
        candidate = [dt for dt in (ymd, mdy, dmy) if int(len(dt[0])) == int(len(str(int(dt[0])))) and
                     1 <= int(dt[1]) <= 12 and 1 <= int(dt[2]) <= 31]
        if int(len(candidate)) > 1:
            candidate = [k for k in candidate if int(k[0]) > 1300]
            if int(len(candidate)) > 1:
                raise ValueError(f"无法确定日期：{candidate}")
        year, month, day = candidate[0]
    else:
        raise ValueError("日期长度不合法")
    return int(year), int(month), int(day)


def parse_time_str(time_str: str) -> tuple:
    """
    将时间字符串解析为元组格式的时间表示

    Args:
        time_str (str): 时间字符串，可以包含分隔符，但必须为顺序：时分秒微秒，且自左向右存在，若不包含分隔符，时分秒必须为2位，微秒可选。

    Returns:
        tuple: 包含小时、分钟、秒和微秒的元组

    """
    t_token = time_str.strip()
    non_digit = set(re.findall(r'\D', t_token))
    # 存在间隔符，则补足时分秒至两位
    if non_digit:
        for sep_ in non_digit:
            t_token = t_token.replace(sep_, ":")
        parts = [int(k) for k in t_token.split(":")]
    else:
        size = int(len(t_token))
        parts = [int(t_token[i:(i + 2)]) for i in range(0, min(size, 6), 2)]
        if size > 6:
            parts.append(int(t_token[6:]))
    parts += [0] * (4 - int(len(parts)))
    for k in parts[:3]:
        if k < 0 or k >= 60:
            raise ValueError(f"非法时间值: {k}")
    if parts[-1] < 0 or parts[-1] > 1e6:
        raise ValueError(f"非法时间值: {parts[-1]}")
    return tuple(parts)


def unify_time(daytime, fmt: str = "datetime",  mode: int = 7, pattern: str = None, dot: str = "-"):
    """
    统一时间格式函数。

    Args:
        daytime (datetime.datetime, pd.Timestamp, int, str): 待转换的时间戳。
        fmt (str, optional): 目标格式，可选值为 "datetime"（默认）、"int"、"str"。默认为 "datetime"。
        mode (int, optional): 时间戳长度，可选值为 1 到 7，分别表示 Y、YM、YMD、YMDH、YMDHM、YMDHMS、YMDHMSμ。默认为 7。
        pattern (str, optional): 自定义字符串格式，仅在 daytime 为字符串时有效。默认为 None。
        dot (str, optional): 日期部分连接符，仅在 fmt 为 "str" 时有效。默认为 "-"。

    Returns:
        datetime.datetime 或 int 或 str: 根据 fmt 参数返回相应类型的时间戳。

    Raises:
        ValueError: 如果 mode 不是 1 到 7 之间的整数。
        TypeError: 如果 daytime 不是 datetime.datetime、pd.Timestamp、int 或 str 类型。

    """

    if not (1 <= mode <= 7):
        raise ValueError("mode 必须是 1 到 7 之间的整数")

    if isinstance(daytime, datetime) or isinstance(daytime, pd.Timestamp):
        dt_parts = (daytime.year, daytime.month, daytime.day, daytime.hour, daytime.minute,
                    daytime.second, daytime.microsecond)
    elif isinstance(daytime, int):
        dt_str = str(daytime)
        dt_len = int(len(dt_str))
        year, month, day = parse_date_str(dt_str[:8])
        hour = int(dt_str[8:10]) if dt_len > 10 else 0
        minute = int(dt_str[10:12]) if dt_len > 12 else 0
        second = int(dt_str[12:14]) if dt_len > 14 else 0
        microsecond = int(dt_str[14:]) if dt_len > 16 else 0
        dt_parts = (year, month, day, hour, minute, second, microsecond)
    elif isinstance(daytime, str):
        # 已指定字符串模式
        if pattern:
            dt = datetime.strptime(daytime, pattern)
            dt_parts = (dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second, dt.microsecond)
        # 未指定字符串模式：尝试解析
        else:
            # 拆分日期与时间部分（以空格或 T 分隔）
            s = daytime.strip()
            dt_parts = re.split(r'[T\s]', s, maxsplit=1)
            d_token = dt_parts[0]
            t_token = dt_parts[1] if int(len(dt_parts) == 2) else ""
            # 处理日期部分
            year, month, day = parse_date_str(d_token)
            # 处理时间部分
            hour, minute, second, microsecond = parse_time_str(t_token)
            dt_parts = (year, month, day, hour, minute, second, microsecond)
    else:
        raise TypeError("不支持输入的时间戳类型")
    # 根据mode截取目标数据
    full = list(dt_parts[:mode]) + [1, 1, 1, 0, 0, 0, 0][mode:]
    year, month, day, hour, minute, second, micro = full

    labels = ["%04d", "%02d", "%02d", "%02d", "%02d", "%02d", "%06d"]
    if fmt.upper() == "DATETIME":
        return datetime(year, month, day, hour, minute, second, micro)
    elif fmt.upper() == "INT":
        return int("".join(labels[i] % full[i] for i in range(mode)))
    elif fmt.upper() == "STR":
        date_vals = [year, month, day]
        date_str = dot.join(labels[i] % date_vals[i] for i in range(min(mode, 3)))
        if mode <= 3:
            return date_str
        # 时间部分（始终补齐到 H:M:S），微秒仅 mode == 7 显示
        time_vals = [hour, minute, second]
        time_str = ":".join(f"{time_vals[i]:02d}" for i in range(3))
        if mode == 7:
            time_str += f".{micro:06d}"
        return f"{date_str} {time_str}"


def select_date(dt: list, freq: str = "W", day: int = 1):
    """
    根据指定的频率和时间选择日期。

    Args:
        dt (list): 包含日期信息的list
        freq (str, optional): 频率参数，默认值为"W"。支持"W"（周）和"M"（月）两种频率。
        day (int, optional): 选择日期的参数，默认值为1。如果设置为1，则选择频率周期内的最后一个日期；如果设置为0，则选择频率周期内的第一个日期。

    Returns:
        list: 选择的日期列表。

    Raises:
        ValueError: 如果输入的frequency参数不是"W"或"M"，或者day参数不是0或1，则抛出ValueError异常。

    """
    dt_df = pd.DataFrame(sorted([unify_time(k, mode=3) for k in dt]), columns=["Date"])
    dt_df = dt_df.copy()
    dt_df.loc[:, "Year"] = dt_df["Date"].map(lambda x: x.year)

    if freq.upper() == "W":
        dt_df.loc[:, "Freq"] = dt_df["Date"].map(lambda x: x.isocalendar().week)
    elif freq.upper() == "M":
        dt_df.loc[:, "Freq"] = dt_df["Date"].map(lambda x: x.month)
    else:
        raise ValueError("频率参数仅支持 W / M")

    dt_df = dt_df.groupby(["Year", "Freq"])["Date"]
    if day == 1:
        td_df = sorted(dt_df.max().tolist())
    elif day == 0:
        td_df = sorted(dt_df.min().tolist())
    else:
        raise ValueError("仅支持新频率的第一个或最后一天：1 / 0")
    return td_df


def all_tradeday(start: Union[datetime, str, int],
                    end: Union[datetime, str, int],
                    fmt: str = "int",
                    frequency: str = "D",
                    day: int = 1,
                    market: str = "China"):
    # 统一日期格式
    st = unify_time(start, fmt="datetime", mode=3)
    ed = unify_time(end, fmt="datetime", mode=3)
    # 检查开始日期是否大于结束日期
    if st > ed:
        raise ValueError("开始日期不能大于结束日期！")
    # 通过公开接口查询
    data = mcal.get_calendar(EXCHANGE_MAP[market]).schedule(start_date=st, end_date=ed)
    data = sorted(data.index.tolist())

    # 根据频率调整返回的日期列表
    if frequency.upper() == "D":
        return [unify_time(k, fmt=fmt, mode=3) for k in data]
    else:
        return [unify_time(k, fmt=fmt, mode=3) for k in select_date(data, freq=frequency, day=day)]


def get_tradeday(today: Union[datetime, str, int] = datetime.now(), n: int = 0, market: str = "China"):
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
    day = unify_time(today, fmt="datetime", mode=3)
    # 从公开接口获取
    start = unify_time(day - timedelta(days=abs(n) + 30), fmt="str", mode=3, dot="")
    end = unify_time(day + timedelta(days=abs(n) + 30), fmt="str", mode=3, dot="")
    data = mcal.get_calendar(EXCHANGE_MAP[market]).schedule(start_date=start, end_date=end)
    data = pd.DataFrame(data.index, columns=["Date"])
    if n >= 0:
        data = data.loc[data["Date"] >= day].sort_values(by="Date").iloc[:abs(n) + 1]
    else:
        data = data.loc[data["Date"] < day].sort_values(by="Date", ascending=False).iloc[:abs(n)]

    if data.any:
        trade_day = data["Date"].tolist()[-1].to_pydatetime()
        return trade_day.replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        raise NotImplementedError(f"未能找到{n}个交易日的数据")


def is_tradeday(day: Union[str, int, datetime] = datetime.now(), market: str = "China"):
    """
    判断指定日期是否为交易日。

    Args:
        day (datetime, optional): 指定日期，默认为当前日期。默认为 datetime.now()。
        market (str, optional): 市场代码，默认为 "China"。
    Returns:
        bool: 指定日期是否为交易日。

    """
    day_ = unify_time(day, fmt="datetime", mode=3)
    # 从公开接口获取
    data = mcal.get_calendar(EXCHANGE_MAP[market]).schedule(start_date=day_ - timedelta(days=30), end_date=day_)
    return bool(day_ in data.index)


def all_calendar(start: Union[str, int, datetime], end: Union[str, int, datetime] = datetime.now(),
                 fmt: str = "datetime", frequency: str = "D", day: int = 1, dot: str = "-"):
    """
    获取指定时间段内的日期列表，根据频率和日期选择返回特定日期。

    Args:
        start (datetime): 开始日期，datetime类型。
        end (datetime, optional): 结束日期，默认为当前日期，datetime类型。
        fmt (str): 日期格式，默认为datetime
        frequency (str, optional): 日期频率，默认为"D"（每天）。可选值包括"W"（每周）、"M"（每月）、"Q"（每季度）等。
        day (int, optional): 当频率为每周或每月时，用于指定周几或几号，默认为1。
        dot (str, optional): 日期部分连接符，仅在 fmt 为 "str" 时有效。默认为 "-"。
    Returns:
        list : 返回指定时间段内的日期列表
    """
    # 统一时间格式
    start = unify_time(start, mode=3)
    end = unify_time(end, mode=3)
    # 初始化日期列表
    date_list = []
    # 循环添加日期到列表中，直到起始日期大于结束日期
    while start <= end:
        date_list.append(start)
        start += timedelta(days=1)
    # 如果频率为每天，直接返回日期列表
    if frequency.upper() != "D":
        # 根据频率和指定的日期选择日期
        date_list = select_date(date_list, frequency, day)

    if fmt.lower() != "datetime":
        date_list = [unify_time(k, fmt=fmt, mode=3, dot=dot) for k in date_list]

    return date_list


def transform_time_range(t_rng: list, call_auction: bool = True):
    """
    将时间范围转换为秒数表示的时间范围。

    Args:
        t_rng (list): 包含时间范围的列表，每个时间范围是一个包含两个元素的元组，分别表示起始时间和结束时间。
        call_auction (bool, optional): 是否处理市场集合竞价时间。默认为True。

    Returns:
        list: 转换后的时间范围列表，每个时间范围是一个包含两个元素的元组，分别表示起始时间和结束时间的秒数。

    """
    def get_time_second(str_t):
        """
        将字符串形式的时间转换为秒数。
        Args:
            str_t (str): 包含时间信息的字符串，格式为 "YYYY-MM-DD HH:MM:SS" 或 "HH:MM:SS"。
        Returns:
            int: 字符串形式的时间转换为秒数后的结果。
        Raises:
            ValueError: 如果输入的字符串格式不正确，或者时间部分不是三个元素（小时、分钟、秒）。
        Examples:
            >>> get_time_second("2023-10-01 12:34:56")
            45296
            >>> get_time_second("12:34:56")
            45296
            >>> get_time_second("12:34")
            44640
        """
        # 将字符串按空格分割，取最后一个元素（时间部分），再按冒号分割取前三个元素（小时、分钟、秒）
        str_t_tag = str_t.split(" ")[-1].split(":")[:3]
        # 如果时间部分元素数量小于3，则补全为3个元素，秒数补为00
        if float(len(str_t_tag)) < 3:
            str_t_tag.append("00")
        # 提取小时、分钟、秒
        h, m, s = str_t_tag
        # 将小时、分钟、秒转换为秒数，并累加得到总秒数
        t_second = int(h) * 3600 + int(m) * 60 + int(s)
        return t_second
    sec_rng = []
    for rng_ in t_rng:
        start, end = rng_
        # 获取起始时间的秒数
        st_second = get_time_second(start)
        # 获取结束时间的秒数
        ed_second = get_time_second(end)
        # Generate time range
        if ed_second < 24 * 3600:
            sec_rng.append([st_second, ed_second])
        else:
            sec_rng.append([st_second, 24 * 3600])
            sec_rng.append([0, ed_second - 24 * 3600])
    # 市场集合竞价时间处理
    if call_auction:
        sec_rng[0][0] -= 60
    sec_rng = [tuple(k) for k in sec_rng]
    return sec_rng


def bartime_to_tradeday(bartimes):
    """
    将给定的交易时间列表转换为交易日映射字典。

    Args:
        bartimes (list of datetime.datetime): 给定的交易时间列表。

    Returns:
        dict: 交易日映射字典，其中键为给定的交易时间，值为对应的交易日（datetime.datetime对象）。

    """
    all_bartime = sorted(set(bartimes))
    td_map = {}
    td = None
    change = True
    for k in all_bartime:
        if 8 <= k.hour < 16:
            td = k.replace(hour=0, minute=0, second=0, microsecond=0)
            change = True
        elif (k.hour < 8 or k.hour >= 16) and change:
            td = get_tradeday(k, n=0)
            change = False
        td_map[k] = td
    return td_map
