"""
LogixBase 工具模块

这是一个全面的工具模块，提供了丰富的实用函数和类，涵盖了数据处理、时间处理、装饰器、字符串操作、进度条、JSON编码、网络通信等多个领域。

主要功能模块：
1. 数据库处理（database）：提供数据库连接和操作的基础工具类
2. 装饰器工具（decorator）：提供函数和方法的装饰器，如计时器、进度条等
3. 日期时间处理（dthandler）：提供日期和时间的高级处理函数
4. 工具类（tool）：提供进度条、JSON编码等实用工具
5. 网络通信（web）：提供SSH连接、邮件发送等网络相关功能
6. 字符串操作（strmanip）：提供字符串处理的实用函数

模块特点：
- 高度模块化和可复用
- 提供丰富的类型转换和处理函数
- 支持多种数据库操作
- 提供网络通信和文件传输工具
"""

# 导入所有模块的函数和类
from .database import (
    DealWithSql,
    DealWithOracle
)

from .decorator import (
    virtual,
    timer,
    progressor,
    silence_asyncio_warning
)

from .dthandler import (
    unify_time,
    select_date,
    all_tradeday,
    get_tradeday,
    is_tradeday,
    all_calendar,
    transform_time_range,
    bartime_to_tradeday,
)

from .strmanip import (
    split_camel_case_to_snake_case
)

from .tool import (
    ProcessBar,
    JsonEncoder,
    contains_callable_values,
    has_own_method,
    load_module_from_file
)

from .web import (
    SSHConnection,
    send_mail_file,
    send_email_text
)

from .schema import DatabaseConfig, MailConfig

# 定义可以被 from logixbase.utils import * 导入的内容
__all__ = [
    # 数据库相关类
    'DealWithSql',
    'DealWithOracle',

    # 装饰器相关函数
    'virtual',
    'timer',
    'progressor',
    'silence_asyncio_warning',

    # 日期时间处理函数
    'unify_time',
    'select_date',
    'all_tradeday',
    'get_tradeday',
    'is_tradeday',
    'all_calendar',
    'transform_time_range',

    # 字符串操作函数
    'split_camel_case_to_snake_case',

    # 工具类和函数
    'ProcessBar',
    'JsonEncoder',
    'contains_callable_values',
    'has_own_method',
    'load_module_from_file',

    # 网络通信相关类和函数
    'SSHConnection',
    'send_mail_file',
    'send_email_text',

    # 标准配置类
    "MailConfig",
    "DatabaseConfig",

]