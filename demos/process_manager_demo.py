import time
import threading
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))
from logixbase.engine import BaseEngine, ProcessManager, BaseComponent
from logixbase.plugin.log_monitor import LogMonitorPlugin
from logixbase.plugin.progress import ProgressPlugin
from logixbase.logger import LogManager


class DataCollectionComponent(BaseComponent):
    """数据采集组件"""
    
    def __init__(self, component_id, data_source, interval=1, **kwargs):
        super().__init__(component_id, **kwargs)
        self.data_source = data_source
        self.interval = interval
        self.data = []
        self._running = False
        self._thread = None
        
    def _execute(self):
        """执行数据采集"""
        self._running = True
        self._thread = threading.Thread(target=self._collection_task)
        self._thread.daemon = True
        self._thread.start()
        
    def _collection_task(self):
        """数据采集任务"""
        self.logger.INFO(f"Starting data collection from {self.data_source}")
        
        counter = 0
        while self._running:
            # 模拟数据采集
            counter += 1
            data_item = f"Data {counter} from {self.data_source} at {time.time()}"
            self.data.append(data_item)
            
            if self.logger and counter % 10 == 0:
                self.logger.INFO(f"Collected {len(self.data)} items from {self.data_source}")
                
            time.sleep(self.interval)
            
    def _stop_execution(self):
        """停止数据采集"""
        self._running = False
        if self._thread:
            self._thread.join(timeout=2)
        self.logger.INFO(f"Stopped data collection from {self.data_source}")
        
    def get_status(self):
        """获取组件状态"""
        status = super().get_status()
        status.update({
            "data_source": self.data_source,
            "data_count": len(self.data),
            "running": self._running
        })
        return status


class DataEngine(BaseEngine):
    """数据引擎"""
    
    def __init__(self):
        """初始化DataEngine，指定配置文件路径"""
        config_path = Path(__file__).parent / "configs" / "logixbase.yaml"
        super().__init__(config_path=str(config_path))
    
    def on_init(self):
        """初始化引擎"""
        super().on_init()
        
        # 注册插件
        self.register_plugin("log_monitor", LogMonitorPlugin(log_dir="./logs/data"))
        
        # 注册组件
        self.register_component("market_data", DataCollectionComponent("market_data", "market", interval=0.5))
        self.register_component("news_data", DataCollectionComponent("news_data", "news", interval=2))
        
    def on_start(self):
        """启动引擎"""
        self.logger.INFO("DataEngine starting...")
        
        # 启动所有组件
        self.start_component("market_data")
        self.start_component("news_data")
        
        # 运行一段时间后返回，让组件在后台继续运行
        time.sleep(5)
        self.logger.INFO("DataEngine started successfully")


class AnalysisEngine(BaseEngine):
    """分析引擎"""
    
    def __init__(self):
        """初始化AnalysisEngine，指定配置文件路径"""
        config_path = Path(__file__).parent / "configs" / "logixbase.yaml"
        super().__init__(config_path=str(config_path))
    
    def on_init(self):
        """初始化引擎"""
        super().on_init()
        
        # 注册插件
        self.register_plugin("log_monitor", LogMonitorPlugin(log_dir="./logs/analysis"))
        self.register_plugin("progress", ProgressPlugin())
        
        # 初始化进度任务
        progress_plugin = self.get_plugin("progress")
        if progress_plugin:
            progress_plugin.create_task("analysis", "Data analysis task", 100)
        
    def on_start(self):
        """启动引擎"""
        self.logger.INFO("AnalysisEngine starting...")
        
        # 模拟分析过程
        progress_plugin = self.get_plugin("progress")
        if progress_plugin:
            for i in range(1, 101):
                progress_plugin.update_progress("analysis", current=i, message=f"Analyzing data step {i}")
                time.sleep(0.1)
                
        self.logger.INFO("AnalysisEngine analysis completed")


def main():
    """主函数"""
    # 配置日志
    logger = LogManager.get_instance(use_mp=False, log_path="./logs")
    logger.start()
    
    # 创建进程管理器
    manager = ProcessManager()
    
    # 注册引擎
    manager.register("data", DataEngine())
    manager.register("analysis", AnalysisEngine())
    
    # 启动数据引擎
    print("Starting data engine...")
    manager.start("data")
    
    # 等待一段时间
    time.sleep(2)
    
    # 获取数据引擎状态
    print("\nData engine status:")
    print(manager.get_status("data"))
    
    # 获取数据引擎组件状态
    print("\nData engine component statuses:")
    for component_id, status in manager.get_component_status("data").items():
        print(f"{component_id}: {status}")
    
    # 启动分析引擎
    print("\nStarting analysis engine...")
    manager.start("analysis")
    
    # 等待分析完成
    time.sleep(2)
    
    # 停止所有引擎
    print("\nStopping all engines...")
    manager.stop_all()
    
    print("\nFinal status of all engines:")
    print(manager.get_status())
    
    # 停止日志管理器
    logger.stop()


if __name__ == "__main__":
    main() 