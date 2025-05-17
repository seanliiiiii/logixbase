import time
import threading
from typing import Dict, Any, Optional

from .base import BasePlugin


class ProgressPlugin(BasePlugin):
    """
    进度插件，用于追踪和显示任务进度
    """
    plugin_type = "progress"
    
    def __init__(self, name=None, **kwargs):
        """
        初始化进度插件
        
        Args:
            name: 插件名称
            **kwargs: 额外参数
        """
        super().__init__(name=name, **kwargs)
        self.tasks = {}  # 任务进度信息
        self.lock = threading.Lock()
        self.refresh_thread = None
        self.stop_event = threading.Event()
        self.refresh_interval = 1.0  # 进度刷新间隔，单位秒
        
    def on_init(self):
        """初始化进度插件"""
        if self.logger:
            self.logger.INFO("初始化进度插件")
        
        # 从配置中读取设置
        if self.config:
            self.refresh_interval = self.config.get("refresh_interval", self.refresh_interval)
        
    def on_start(self):
        """启动进度监控"""
        if self.refresh_thread and self.refresh_thread.is_alive():
            if self.logger:
                self.logger.WARNING("进度监控线程已在运行")
            return
            
        self.stop_event.clear()
        self.refresh_thread = threading.Thread(
            target=self._refresh_progress,
            name="ProgressMonitorThread",
            daemon=True
        )
        self.refresh_thread.start()
        if self.logger:
            self.logger.INFO("进度监控已启动")
    
    def on_stop(self):
        """停止进度监控"""
        if not self.refresh_thread or not self.refresh_thread.is_alive():
            return
            
        self.stop_event.set()
        self.refresh_thread.join(timeout=2.0)
        if self.refresh_thread.is_alive():
            if self.logger:
                self.logger.WARNING("进度监控线程未能正常停止")
        else:
            if self.logger:
                self.logger.INFO("进度监控已停止")
    
    def join(self):
        """等待进度监控线程结束"""
        if self.refresh_thread and self.refresh_thread.is_alive():
            self.refresh_thread.join()
    
    def _refresh_progress(self):
        """进度刷新线程"""
        try:
            while not self.stop_event.is_set():
                self.update_all_tasks()
                time.sleep(self.refresh_interval)
        except Exception as e:
            if self.logger:
                self.logger.ERROR(f"进度监控器出错: {str(e)}")
    
    def update_all_tasks(self):
        """更新所有任务的进度状态"""
        # 实际应用中，可能会根据实际任务情况进行更新
        with self.lock:
            for task_id, task in list(self.tasks.items()):
                # 自动更新已完成的任务
                if task["status"] == "running" and task["progress"] >= 100:
                    task["status"] = "completed"
                    task["end_time"] = time.time()
                    if self.logger:
                        self.logger.INFO(f"任务 {task_id} 已完成")
                
                # 清理过期的已完成任务
                if task["status"] in ["completed", "failed", "cancelled"]:
                    if "end_time" in task and time.time() - task["end_time"] > 3600:  # 1小时后清理
                        del self.tasks[task_id]
                        if self.logger:
                            self.logger.DEBUG(f"已从追踪列表中移除已完成的任务 {task_id}")
    
    def create_task(self, task_id: str, name: str, total: int = 100) -> Dict[str, Any]:
        """
        创建新任务
        
        Args:
            task_id: 任务ID
            name: 任务名称
            total: 任务总量
            
        Returns:
            任务信息字典
        """
        with self.lock:
            task = {
                "id": task_id,
                "name": name,
                "status": "created",
                "progress": 0,
                "total": total,
                "current": 0,
                "start_time": time.time(),
                "end_time": None,
                "message": "",
                "error": None
            }
            self.tasks[task_id] = task
            if self.logger:
                self.logger.INFO(f"已创建任务 {task_id}: {name}")
            return task
    
    def update_progress(self, task_id: str, current: Optional[int] = None, 
                        progress: Optional[float] = None, status: Optional[str] = None, 
                        message: Optional[str] = None, error: Optional[str] = None) -> Dict[str, Any]:
        """
        更新任务进度
        
        Args:
            task_id: 任务ID
            current: 当前进度值
            progress: 进度百分比 (0-100)
            status: 任务状态
            message: 任务消息
            error: 错误信息
            
        Returns:
            更新后的任务信息
        """
        with self.lock:
            if task_id not in self.tasks:
                if self.logger:
                    self.logger.WARNING(f"尝试更新未知任务: {task_id}")
                return None
                
            task = self.tasks[task_id]
            
            if current is not None:
                task["current"] = current
                task["progress"] = min(100, (current / task["total"]) * 100)
            
            if progress is not None:
                task["progress"] = min(100, progress)
                
            if status:
                old_status = task["status"]
                task["status"] = status
                
                if status == "running" and old_status == "created":
                    task["start_time"] = time.time()
                    
                if status in ["completed", "failed", "cancelled"] and old_status == "running":
                    task["end_time"] = time.time()
            
            if message:
                task["message"] = message
                
            if error:
                task["error"] = error
                if task["status"] != "failed":
                    task["status"] = "failed"
                    task["end_time"] = time.time()
            
            return task
    
    def get_task(self, task_id: str) -> Dict[str, Any]:
        """
        获取任务信息
        
        Args:
            task_id: 任务ID
            
        Returns:
            任务信息字典
        """
        with self.lock:
            return self.tasks.get(task_id)
    
    def get_all_tasks(self) -> Dict[str, Dict[str, Any]]:
        """
        获取所有任务信息
        
        Returns:
            所有任务信息的字典
        """
        with self.lock:
            return dict(self.tasks)
    
    def cancel_task(self, task_id: str) -> bool:
        """
        取消任务
        
        Args:
            task_id: 任务ID
            
        Returns:
            是否成功取消
        """
        with self.lock:
            if task_id not in self.tasks:
                return False
                
            task = self.tasks[task_id]
            if task["status"] in ["completed", "failed", "cancelled"]:
                return False
                
            task["status"] = "cancelled"
            task["end_time"] = time.time()
            task["message"] = "任务已取消"
            
            if self.logger:
                self.logger.INFO(f"任务 {task_id} 已取消")
            return True
    
    def get_status(self) -> Dict[str, Any]:
        """获取插件状态"""
        status = super().get_status()
        
        with self.lock:
            running = sum(1 for t in self.tasks.values() if t["status"] == "running")
            completed = sum(1 for t in self.tasks.values() if t["status"] == "completed")
            failed = sum(1 for t in self.tasks.values() if t["status"] == "failed")
            
            status.update({
                "tasks_count": len(self.tasks),
                "running_tasks": running,
                "completed_tasks": completed,
                "failed_tasks": failed,
                "monitor_running": self.refresh_thread is not None and self.refresh_thread.is_alive()
            })
            
        return status 