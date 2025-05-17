import time
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

from logixbase.engine import BaseEngine, BaseComponent
from logixbase.plugin.log_monitor import LogMonitorPlugin
from logixbase.plugin.progress import ProgressPlugin
from logixbase.logger import LogManager


class SimpleDataComponent(BaseComponent):
    """简单的数据组件示例"""
    
    def __init__(self, component_id, data_source, **kwargs):
        super().__init__(component_id, **kwargs)
        self.data_source = data_source
        self.data = []
        
    def _execute(self):
        """模拟数据处理"""
        self.logger.INFO(f"Processing data from {self.data_source}")
        
        # 模拟数据加载
        for i in range(10):
            self.data.append(f"Data {i} from {self.data_source}")
            time.sleep(0.1)  # 模拟处理时间
            
        self.logger.INFO(f"Processed {len(self.data)} items from {self.data_source}")
        
    def _stop_execution(self):
        """停止数据处理"""
        self.logger.INFO(f"Stopping data processing from {self.data_source}")
        self.data = []
        
    def get_status(self):
        """获取组件状态"""
        status = super().get_status()
        status.update({
            "data_source": self.data_source,
            "data_count": len(self.data)
        })
        return status


class SimpleEngine(BaseEngine):
    """简单的引擎示例"""
    
    def __init__(self):
        """初始化SimpleEngine，指定配置文件路径"""
        config_path = Path(__file__).parent / "configs" / "logixbase.yaml"
        super().__init__(config_path=str(config_path))
    
    def on_init(self):
        """初始化引擎"""
        super().on_init()
        
        # 注册插件
        self.register_plugin("log_monitor", LogMonitorPlugin(log_dir="./logs"))
        self.register_plugin("progress", ProgressPlugin())
        
        # 注册组件
        self.register_component("data1", SimpleDataComponent("data1", "source1"))
        self.register_component("data2", SimpleDataComponent("data2", "source2"))
        
        # 初始化进度任务
        progress_plugin = self.get_plugin("progress")
        if progress_plugin:
            progress_plugin.create_task("main_task", "Main processing task", 100)
        
    def on_start(self):
        """启动引擎"""
        self.logger.INFO("SimpleEngine starting...")
        
        # 启动组件
        self.start_component("data1")
        self.start_component("data2")
        
        # 更新进度
        progress_plugin = self.get_plugin("progress")
        if progress_plugin:
            for i in range(1, 101):
                progress_plugin.update_progress("main_task", current=i, message=f"Processing step {i}")
                time.sleep(0.05)  # 模拟处理时间
        
        self.logger.INFO("SimpleEngine processing completed")


def main():
    """主函数"""
    # 配置日志
    logger = LogManager.get_instance(use_mp=False, log_path="./logs")
    logger.start()
    
    # 创建并运行引擎
    engine = SimpleEngine()
    engine.run()
    
    # 打印最终状态
    print("\nFinal engine status:")
    print(engine.get_status())
    
    print("\nComponent statuses:")
    for component_id, status in engine.get_component_status().items():
        print(f"{component_id}: {status}")
        
    # 停止日志管理器
    logger.stop()


if __name__ == "__main__":
    main() 