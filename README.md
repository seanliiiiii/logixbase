# LogixBase

LogixQuant是一个高度模块化的量化交易策略开发框架，为金融量化研究和自动化交易提供一站式解决方案。

*让数据驱动决策，让策略创造价值*

Logixbase为LogixQuant的底层通用库项目

## 功能特性

- **模块化插件架构**：基于插件的灵活扩展系统，轻松添加新功能
- **完整策略生命周期管理**：从数据采集、信号生成到策略回测、实盘交易的全流程支持
- **高性能回测引擎**：支持多进程/多线程并行计算，提供精确的绩效分析
- **多数据源集成**：兼容主流金融数据提供商，支持实时和历史数据
- **丰富的交易接口**：对接多种交易网关，支持股票、期货、期权等多品种交易
- **完善的日志和监控系统**：实时监控策略运行状态，详细记录交易执行过程

## 系统架构

LogixBase采用分层设计，主要包含以下核心层次：

1. **核心层**：提供基础引擎、组件系统和插件架构
2. **协议层**：定义各模块间的通信接口和数据结构
3. **功能层**：实现数据处理、信号生成、策略执行等核心功能
4. **应用层**：面向最终用户的策略开发接口和工具

## 安装方式

### 环境要求

- Python 3.9+
- Windows/Linux/MacOS

### 安装命令

```bash
# 从源码安装
git clone https://github.com/yourusername/logixbase.git
cd logixbase
pip install -e .

# 或直接通过pip安装
pip install logixbase
```

## 开发指南

### 自定义引擎开发

```python
from typing import Dict, Any, Optional
from logixbase.engine import BaseEngine
from logixbase.logger import LoggerManager
from logixbase.configer import ConfigLoader

class QuantEngine(BaseEngine):
    """量化交易专用引擎，扩展基础引擎功能"""
    
    def __init__(self, config_path: str, extra_config: Optional[Dict[str, Any]] = None):
        super().__init__(config_path, extra_config)
        self.strategy_config = self.config.get("strategy", {})
        self.market_data = None
        
    def initialize(self):
        """引擎初始化，加载核心组件"""
        self.logger.INFO("量化引擎初始化中...")
        
        # 注册内置插件
        self._register_builtin_plugins()
        
        # 初始化数据源
        self._init_data_source()
        
        # 初始化交易网关
        self._init_trading_gateway()
        
        self.logger.INFO("量化引擎初始化完成")
        
    def _register_builtin_plugins(self):
        """注册内置插件"""
        from logixbase.plugin.log_monitor import LogMonitorPlugin
        from logixbase.plugin.progress import ProgressPlugin
        
        self.register_plugin("log_monitor", LogMonitorPlugin())
        self.register_plugin("progress", ProgressPlugin())
    
    def _init_data_source(self):
        """初始化数据源"""
        from my_project.data import MarketDataFeeder
        
        data_config = self.config.get("data_source", {})
        self.market_data = MarketDataFeeder(data_config)
        self.register_component("market_data", self.market_data)
    
    def _init_trading_gateway(self):
        """初始化交易网关"""
        from my_project.gateway import SimulatedTradeGateway
        
        gateway_config = self.config.get("gateway", {})
        gateway = SimulatedTradeGateway(gateway_config)
        self.register_component("trade_gateway", gateway)
    
    def run_backtest(self, start_date, end_date):
        """运行回测"""
        self.logger.INFO(f"开始回测: {start_date} 至 {end_date}")
        
        # 配置回测参数
        self.set_global_context("backtest_start", start_date)
        self.set_global_context("backtest_end", end_date)
        
        # 启动核心组件
        self.start_component("market_data")
        self.start_component("trade_gateway")
        
        # 启动策略
        for name, plugin in self.plugins.items():
            if plugin.plugin_type == "strategy":
                self.logger.INFO(f"启动策略: {name}")
                plugin.on_start()
        
        # 运行主循环
        self.run()
        
        # 生成回测报告
        self._generate_report()
    
    def _generate_report(self):
        """生成回测报告"""
        self.logger.INFO("生成回测报告")
        # 回测报告生成逻辑
```

### 组件开发示例

#### 数据组件开发

```python
from typing import Dict, Any, List
from logixbase.engine import BaseComponent
from logixbase.protocol.mod import DataProtocol

class MarketDataFeeder(BaseComponent):
    """市场数据提供组件"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__("market_data")
        self.config = config
        self.api_key = config.get("api_key", "")
        self.data_source = config.get("source", "local")
        self.symbols = config.get("symbols", [])
        self.data_cache = {}
        
    def _execute(self):
        """组件主执行逻辑"""
        self.logger.INFO(f"开始获取市场数据，来源: {self.data_source}")
        
        if self.data_source == "local":
            self._load_local_data()
        elif self.data_source == "api":
            self._fetch_api_data()
        else:
            self.logger.ERROR(f"不支持的数据源: {self.data_source}")
            
        # 发布数据就绪事件
        self.publish_event("MARKET_DATA_READY", self.data_cache)
        
    def _load_local_data(self):
        """从本地加载数据"""
        import pandas as pd
        
        for symbol in self.symbols:
            try:
                file_path = f"data/{symbol}.csv"
                self.logger.INFO(f"从本地加载数据: {file_path}")
                
                data = pd.read_csv(file_path, index_col="date", parse_dates=True)
                self.data_cache[symbol] = data
                
                self.logger.INFO(f"成功加载 {symbol} 数据，共 {len(data)} 条记录")
            except Exception as e:
                self.logger.ERROR(f"加载 {symbol} 数据失败: {str(e)}")
    
    def _fetch_api_data(self):
        """从API获取数据"""
        # API数据获取逻辑
        
    def get_history_bars(self, symbol: str, start_date: str, end_date: str) -> DataProtocol:
        """获取历史K线数据"""
        if symbol not in self.data_cache:
            self.logger.WARNING(f"未找到 {symbol} 的数据")
            return None
            
        # 截取指定日期范围的数据
        data = self.data_cache[symbol]
        return data.loc[start_date:end_date]
    
    def _stop_execution(self):
        """停止组件执行"""
        self.logger.INFO("停止市场数据组件")
        self.data_cache.clear()
```

#### 信号组件开发

```python
from typing import Dict, Any, List
from logixbase.engine import BaseComponent
import pandas as pd
import numpy as np

class SignalGenerator(BaseComponent):
    """信号生成组件"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__("signal_generator")
        self.config = config
        self.indicators = config.get("indicators", [])
        self.lookback_period = config.get("lookback_period", 20)
        self.signal_threshold = config.get("signal_threshold", 0.5)
        
    def _execute(self):
        """组件主执行逻辑"""
        self.logger.INFO("开始生成交易信号")
        
        # 订阅市场数据事件
        self.subscribe_event("MARKET_DATA_READY", self._on_market_data_ready)
        
    def _on_market_data_ready(self, event_data):
        """处理市场数据就绪事件"""
        market_data = event_data
        
        for symbol, data in market_data.items():
            try:
                signals = self._calculate_signals(symbol, data)
                self.logger.INFO(f"生成 {symbol} 信号，共 {len(signals)} 个信号")
                
                # 发布信号就绪事件
                self.publish_event("SIGNAL_READY", {
                    "symbol": symbol,
                    "signals": signals
                })
            except Exception as e:
                self.logger.ERROR(f"生成 {symbol} 信号失败: {str(e)}")
    
    def _calculate_signals(self, symbol: str, data: pd.DataFrame) -> pd.DataFrame:
        """计算交易信号"""
        signals = pd.DataFrame(index=data.index)
        signals["symbol"] = symbol
        
        # 计算技术指标
        for indicator in self.indicators:
            if indicator == "ma_cross":
                signals = self._calculate_ma_cross(data, signals)
            elif indicator == "rsi":
                signals = self._calculate_rsi(data, signals)
                
        return signals
        
    def _calculate_ma_cross(self, data: pd.DataFrame, signals: pd.DataFrame) -> pd.DataFrame:
        """计算均线交叉信号"""
        # 计算短期和长期均线
        data["ma_short"] = data["close"].rolling(window=5).mean()
        data["ma_long"] = data["close"].rolling(window=20).mean()
        
        # 计算金叉死叉信号
        signals["ma_cross"] = 0
        signals.loc[data["ma_short"] > data["ma_long"], "ma_cross"] = 1
        signals.loc[data["ma_short"] < data["ma_long"], "ma_cross"] = -1
        
        return signals
        
    def _calculate_rsi(self, data: pd.DataFrame, signals: pd.DataFrame) -> pd.DataFrame:
        """计算RSI指标信号"""
        # RSI指标计算和信号生成
        return signals
    
    def _stop_execution(self):
        """停止组件执行"""
        self.logger.INFO("停止信号生成组件")
```

#### 策略组件开发

```python
from typing import Dict, Any
from logixbase.plugin.base import BasePlugin

class MACrossStrategy(BasePlugin):
    """均线交叉策略插件"""
    
    plugin_type = "strategy"
    
    def __init__(self, config: Dict[str, Any] = None):
        super().__init__()
        self.config = config or {}
        self.symbols = self.config.get("symbols", [])
        self.position = {}
        
    def on_init(self):
        """策略初始化"""
        self.logger.INFO(f"均线交叉策略初始化，交易品种: {', '.join(self.symbols)}")
        
        # 初始化持仓
        for symbol in self.symbols:
            self.position[symbol] = 0
            
    def on_start(self):
        """策略启动"""
        self.logger.INFO("均线交叉策略启动")
        
        # 订阅信号事件
        self.subscribe_event("SIGNAL_READY", self._on_signal_ready)
        
    def _on_signal_ready(self, event_data):
        """处理信号就绪事件"""
        symbol = event_data["symbol"]
        signals = event_data["signals"]
        
        if symbol not in self.symbols:
            return
            
        self.logger.INFO(f"处理 {symbol} 信号数据")
        
        # 获取最新信号
        latest_signal = signals.iloc[-1]
        
        # 根据信号生成交易决策
        current_position = self.position[symbol]
        
        if latest_signal["ma_cross"] == 1 and current_position <= 0:
            # 买入信号
            self._generate_order(symbol, "buy", 1)
            self.position[symbol] = 1
            
        elif latest_signal["ma_cross"] == -1 and current_position >= 0:
            # 卖出信号
            self._generate_order(symbol, "sell", 1)
            self.position[symbol] = -1
    
    def _generate_order(self, symbol: str, direction: str, volume: float):
        """生成交易订单"""
        order = {
            "symbol": symbol,
            "direction": direction,
            "volume": volume,
            "order_type": "market",
            "strategy_name": "ma_cross"
        }
        
        self.logger.INFO(f"生成订单: {symbol} {direction} {volume}手")
        
        # 发布订单事件
        self.publish_event("ORDER_GENERATED", order)
        
    def on_stop(self):
        """策略停止"""
        self.logger.INFO("均线交叉策略停止")
        
        # 平仓所有持仓
        for symbol, pos in self.position.items():
            if pos != 0:
                direction = "sell" if pos > 0 else "buy"
                self._generate_order(symbol, direction, abs(pos))
                self.position[symbol] = 0
```

### 完整应用示例

```python
from logixbase.logger import init_logger
from my_project.engine import QuantEngine
from my_project.components import SignalGenerator
from my_project.strategies import MACrossStrategy

def main():
    # 初始化日志
    init_logger("./logs", log_level="INFO")
    
    # 创建量化引擎
    engine = QuantEngine(config_path="./config.yaml")
    
    # 初始化引擎
    engine.initialize()
    
    # 注册信号生成组件
    signal_config = {
        "indicators": ["ma_cross", "rsi"],
        "lookback_period": 20
    }
    signal_generator = SignalGenerator(signal_config)
    engine.register_component("signal_generator", signal_generator)
    
    # 注册策略插件
    strategy_config = {
        "symbols": ["AAPL", "MSFT", "GOOG"]
    }
    engine.register_plugin("ma_cross_strategy", MACrossStrategy(strategy_config))
    
    # 运行回测
    engine.run_backtest(start_date="2023-01-01", end_date="2023-12-31")
    
if __name__ == "__main__":
    main()
```

## 配置说明

LogixBase使用YAML格式的配置文件，主要配置项包括：

```yaml
# config.yaml
logger:
  log_path: "./logs"  # 日志保存路径
  log_level: "INFO"   # 日志级别：DEBUG, INFO, WARNING, ERROR, CRITICAL
  log_format: "TEXT"  # 日志格式：TEXT, JSON
  to_console: true    # 是否输出到控制台
  use_multiprocess: false  # 是否使用多进程日志

plugin:
  # 插件特定配置
  log_monitor:
    enabled: true
    log_dir: "./logs"
  
  progress:
    enabled: true
    update_interval: 1

component:
  enabled: true
  
data_source:
  source: "local"  # 数据源类型：local, api
  api_key: ""      # API密钥
  symbols: ["AAPL", "MSFT", "GOOG"]  # 交易品种

strategy:
  ma_cross:
    enabled: true
    fast_period: 5
    slow_period: 20
    
gateway:
  simulation:
    enabled: true
    initial_capital: 1000000
    commission_rate: 0.0003
```

## 项目结构

```
logixbase/
├── engine/            # 核心引擎实现
│   ├── base.py        # 引擎基类和组件基类
│   ├── manager.py     # 进程管理器
│   └── __init__.py    # 模块初始化文件
│
├── plugin/            # 插件系统
│   ├── base.py        # 插件基类
│   ├── manager.py     # 插件管理器
│   ├── log_monitor.py # 日志监控插件
│   ├── progress.py    # 进度监控插件
│   └── __init__.py    # 模块初始化文件
│
├── protocol/          # 接口协议定义
│   ├── mod.py         # 基础模块协议
│   ├── plugin.py      # 插件协议
│   ├── database.py    # 数据库访问协议
│   ├── feeder.py      # 数据源协议
│   ├── gateway.py     # 交易网关协议
│   ├── task.py        # 任务协议
│   └── __init__.py    # 模块初始化文件
│
├── logger/            # 日志系统
│   ├── core.py        # 日志管理器
│   ├── writer.py      # 日志写入器
│   ├── mpwriter.py    # 多进程日志写入器
│   ├── config.py      # 日志配置
│   ├── parser.py      # 日志解析器
│   ├── decorator.py   # 日志装饰器
│   ├── utils.py       # 日志工具函数
│   ├── constant.py    # 日志常量
│   ├── formatter.py   # 日志格式化器
│   ├── dashboard/     # 日志仪表盘
│   └── __init__.py    # 模块初始化文件
│
├── configer/          # 配置系统
│   ├── loader.py      # 配置加载器
│   ├── schema.py      # 配置模式定义
│   ├── hotreloader.py # 配置热加载器
│   ├── proxy.py       # 配置代理
│   └── __init__.py    # 模块初始化文件
│
├── executor/          # 执行器
│   ├── base.py        # 执行器基类
│   ├── thread.py      # 线程执行器
│   ├── process.py     # 进程执行器
│   ├── coordinator.py # 执行协调器
│   ├── main.py        # 主执行入口
│   ├── payload.py     # 执行负载
│   ├── task.py        # 任务定义
│   ├── context.py     # 执行上下文
│   └── __init__.py    # 模块初始化文件
│
├── gateway/           # 交易网关
│   ├── base.py        # 网关基类
│   ├── ctp.py         # CTP交易接口
│   ├── schema.py      # 网关数据结构
│   ├── api/           # API接口
│   └── __init__.py    # 模块初始化文件
│
├── feeder/            # 数据源
│   ├── base.py        # 数据源基类
│   ├── schema.py      # 数据源结构定义
│   ├── tsfeeder.py    # 时间序列数据源
│   ├── tqfeeder.py    # TQData数据源
│   ├── sqlfeeder.py   # SQL数据源
│   ├── wfeeder.py     # Wind数据源
│   ├── config/        # 数据源配置
│   └── __init__.py    # 模块初始化文件
│
├── trader/            # 交易系统
│   ├── schema.py      # 交易数据结构定义
│   ├── utils.py       # 交易工具函数
│   ├── constant.py    # 常量定义
│   ├── config.py      # 交易配置
│   ├── tool.py        # 交易工具集
│   └── __init__.py    # 模块初始化文件
│
├── compiler/          # 策略编译器
│   ├── build.py       # 构建工具
│   ├── schema.py      # 编译器模式定义
│   └── __init__.py    # 模块初始化文件
│
├── algolib/           # 算法库
│   ├── timeseries.py  # 时间序列分析
│   ├── optimization.py # 优化算法
│   ├── matops.py      # 矩阵操作
│   ├── regression.py  # 回归分析
│   ├── fin.py         # 金融计算
│   ├── chart.py       # 图表工具
│   ├── basestat.py    # 基础统计分析
│   ├── stattest.py    # 统计检验
│   ├── utils.py       # 工具函数
│   ├── stochastic.py  # 随机过程
│   └── __init__.py    # 模块初始化文件
│
├── utils/             # 工具函数
│   ├── dthandler.py   # 日期时间处理
│   ├── database.py    # 数据库工具
│   ├── decorator.py   # 装饰器工具
│   ├── schema.py      # 模式定义工具
│   ├── tool.py        # 通用工具
│   ├── web.py         # Web相关工具
│   ├── strmanip.py    # 字符串处理
│   └── __init__.py    # 模块初始化文件
│
└── demos/             # 示例代码
    ├── simple_engine.py    # 简单引擎示例
    └── process_manager.py  # 进程管理器示例
```

## 开发者说明

### 本地开发环境搭建

```bash
# 克隆仓库
git clone https://github.com/339640170/logixbase.git
cd logixbase

# 创建虚拟环境
python -m venv venv
source venv/bin/activate  # Linux/MacOS
venv\Scripts\activate     # Windows

# 安装开发依赖
pip install -e ".[dev]"
```

### 编码规范

- 遵循PEP 8风格指南
- 使用Type Hints进行类型注解
- 使用Docstrings记录函数和类的用途
- 保持日志调用的一致性，使用大写方法(如 logger.INFO)

### 模块开发规范

1. **插件开发**：继承BasePlugin实现自定义插件
2. **组件开发**：继承BaseComponent实现数据处理组件
3. **引擎扩展**：继承BaseEngine实现专用引擎

## 测试说明

```bash
# 运行所有测试
pytest tests/

# 运行特定模块测试
pytest tests/test_engine.py

# 运行示例程序
cd demos
python simple_engine.py
```

## 子项目

LogixBase是一个框架生态，包含以下子项目：

1. **logixbase**：核心基础库，提供引擎、插件和组件系统
2. **logixdata**：数据管理模块，负责数据获取、清洗和存储
3. **logixsignal**：信号分析模块，提供技术指标和信号生成
4. **logixbt**：回测模块，用于策略性能评估
5. **logixtrader**：交易系统，用于实盘交易执行

## License

MIT License

Copyright (c) 2024 LogixBase开发团队

## 联系方式

项目维护者：trader@logixquant.com
