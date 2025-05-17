import os
import glob
import time
from datetime import datetime
from typing import Dict, List, Any

from ..plugin.base import BasePlugin


class LogMonitorPlugin(BasePlugin):
    """
    日志监控插件，用于监控和管理日志文件
    """
    plugin_type = "log_monitor"
    
    def __init__(self, name="log_monitor", log_dir=None, max_logs=100, **kwargs):
        """
        初始化日志监控插件
        
        Args:
            name: 插件名称
            log_dir: 日志目录，如果为None则使用引擎配置
            max_logs: 最大日志文件数量
            **kwargs: 额外的元数据
        """
        super().__init__(name, **kwargs)
        self.log_dir = log_dir
        self.max_logs = max_logs
        self.logs_info = {}
        self.monitor_active = False
        self.monitor_interval = kwargs.get("monitor_interval", 60)  # 监控间隔，单位秒
        
    def on_init(self):
        """
        初始化日志监控
        """
        if not self.log_dir and self.engine:
            # 尝试从引擎配置获取日志目录
            self.log_dir = self.config.get("log_dir") or "./logs"
            
        # 创建日志目录
        if self.log_dir:
            os.makedirs(self.log_dir, exist_ok=True)
            if self.logger:
                self.logger.INFO(f"日志目录: {os.path.abspath(self.log_dir)}")
                
        # 初始获取日志信息
        self.get_logs()
        
    def on_start(self):
        """
        启动日志监控
        """
        if self.monitor_active:
            return
            
        self.monitor_active = True
        if self.engine and hasattr(self.engine, "executor"):
            self.engine.executor.submit(self._monitor_logs_task)
            if self.logger:
                self.logger.INFO("日志监控已启动")
        
    def on_stop(self):
        """
        停止日志监控
        """
        self.monitor_active = False
        if self.logger:
            self.logger.INFO("日志监控已停止")
        
    def get_logs(self) -> Dict[str, Dict[str, Any]]:
        """
        获取日志文件信息
        
        Returns:
            日志名称到信息的映射
        """
        if not self.log_dir:
            return {}
            
        log_files = glob.glob(f"{self.log_dir}/*.log")
        
        self.logs_info = {}
        for log_file in log_files:
            file_name = os.path.basename(log_file)
            stats = os.stat(log_file)
            self.logs_info[file_name] = {
                "path": log_file,
                "size": stats.st_size,
                "modified": datetime.fromtimestamp(stats.st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
                "lines": self._count_lines(log_file)
            }
            
        return self.logs_info
        
    def get_log_content(self, log_name: str, max_lines: int = 100, offset: int = 0) -> List[str]:
        """
        获取日志内容
        
        Args:
            log_name: 日志文件名
            max_lines: 最大行数
            offset: 起始行偏移量
            
        Returns:
            日志内容行的列表
        """
        if not self.log_dir:
            return []
            
        log_path = os.path.join(self.log_dir, log_name)
        if not os.path.exists(log_path):
            return []
            
        try:
            with open(log_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
                
            start = max(0, offset)
            end = min(start + max_lines, len(lines))
            return lines[start:end]
        except Exception as e:
            if self.logger:
                self.logger.ERROR(f"读取日志文件 {log_name} 时出错: {e}")
            return []
        
    def clean_old_logs(self, keep_days: int = 7) -> int:
        """
        清理旧日志
        
        Args:
            keep_days: 保留天数
            
        Returns:
            删除的文件数
        """
        if not self.log_dir:
            return 0
            
        log_files = glob.glob(f"{self.log_dir}/*.log")
        now = time.time()
        deleted = 0
        
        for log_file in log_files:
            stats = os.stat(log_file)
            file_age_days = (now - stats.st_mtime) / (24 * 3600)
            
            if file_age_days > keep_days:
                try:
                    os.remove(log_file)
                    deleted += 1
                    if self.logger:
                        self.logger.INFO(f"已删除旧日志文件: {os.path.basename(log_file)}")
                except Exception as e:
                    if self.logger:
                        self.logger.ERROR(f"删除日志文件 {os.path.basename(log_file)} 时出错: {e}")
                    
        return deleted
        
    def _count_lines(self, file_path: str) -> int:
        """
        计算文件行数
        
        Args:
            file_path: 文件路径
            
        Returns:
            文件行数
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return sum(1 for _ in f)
        except Exception:
            return -1
            
    def _monitor_logs_task(self):
        """
        日志监控任务
        """
        while self.monitor_active:
            try:
                self.get_logs()
                time.sleep(self.monitor_interval)
            except Exception as e:
                if self.logger:
                    self.logger.ERROR(f"日志监控任务出错: {e}")
                time.sleep(self.monitor_interval)
            
    def get_status(self) -> Dict[str, Any]:
        """
        获取插件状态
        
        Returns:
            插件状态信息
        """
        status = super().get_status()
        status.update({
            "log_dir": self.log_dir,
            "logs_count": len(self.logs_info),
            "monitor_active": self.monitor_active,
            "monitor_interval": self.monitor_interval
        })
        return status 