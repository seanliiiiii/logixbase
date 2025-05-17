import time
import threading
import multiprocessing
import sys
from pathlib import Path


sys.path.append(str(Path(__file__).parent.parent))
# 导入 Logger 模块
from logixbase.logger import LogManager, auto_log

# ---------------------------
# 示例：使用装饰器自动记录函数执行日志
# ---------------------------
@auto_log
def sample_function(x, y):
    """
    示例函数，用于演示 @auto_log 装饰器的使用。
    该装饰器会自动记录函数开始、结束及执行时长信息。
    """
    time.sleep(1)  # 模拟耗时操作
    return x + y


# ---------------------------
# 示例：单线程日志记录
# ---------------------------
def demo_single_thread_logging():
    """演示在单线程环境下记录日志"""
    logger = LogManager.get_instance(use_mp=False, log_path=r'd:\logs')
    logger.INFO("【Single Thread Demo】开始单线程日志记录。")
    for i in range(3):
        logger.DEBUG(f"单线程记录调试信息 {i}")
        time.sleep(0.3)
    logger.INFO("【Single Thread Demo】单线程日志记录结束。")


# ---------------------------
# 示例：多线程日志记录
# ---------------------------
def demo_multithread_logging():
    """演示在多线程环境下记录日志"""
    logger = LogManager.get_instance(use_mp=False, log_path=r'd:\logs')
    logger.INFO("【Multithread Demo】开始多线程日志记录。")

    def worker(thread_id):
        for i in range(3):
            logger.DEBUG(f"线程 {thread_id} 记录调试信息 {i}")
            time.sleep(0.3)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(3)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    logger.INFO("【Multithread Demo】多线程日志记录结束。")


# ---------------------------
# 示例：多进程日志记录（类场景）
# ---------------------------
def process_worker(logger, proc_id):
    """模拟多进程日志记录"""
    for i in range(3):
        # 将日志消息放入共享队列，由主进程统一写入日志
        logger.INFO(f"[子进程 {proc_id}] 记录日志消息 {i}")
        time.sleep(0.3)


class MultiprocessLogger:
    def __init__(self, use_mp=True):
        # 每次初始化时都动态创建 LogManager 实例，确保 use_mp 参数正确
        self.logger = LogManager.get_instance(use_mp=use_mp, log_path=r'd:\logs')

        # 确保只有在多进程模式下才尝试访问 mp_queue
        if self.logger.use_mp:
            self.shared_queue = self.logger.log_writer.log_queue  # 获取共享队列
        else:
            self.shared_queue = None

        self.processes = []

    def demo_multiprocess_logging(self):
        """演示多进程环境下记录日志"""
        if self.shared_queue is None:
            print("未启用多进程模式，无法演示多进程日志记录。")
            return

        logger = self.logger
        logger.INFO("【Multiprocess Demo】开始多进程日志记录（共享队列方式）。")

        # 启动多个子进程
        for pid in range(2):  # 启动 2 个子进程
            # p = multiprocessing.Process(target=process_worker, args=(self.shared_queue, pid))
            p = multiprocessing.Process(target=process_worker, args=(self.logger, pid))
            self.processes.append(p)
            p.start()
        self.logger.start()
        # 等待所有子进程执行完毕
        for p in self.processes:
            p.join()
        logger.INFO("【Multiprocess Demo】多进程日志记录结束。")


# ---------------------------
# 演示 Logger 模块常用用法
# ---------------------------
def demo_logger_usage(use_mp=False):
    """综合演示 Logger 的常用用法，包括直接日志调用、装饰器、以及多线程、多进程场景"""
    if use_mp:
        # 多进程模式
        print("\n执行多进程版本的日志记录...")
        mp_logger = MultiprocessLogger(use_mp=True)
        mp_logger.demo_multiprocess_logging()
    else:
        # 单进程模式
        print("\n执行单进程版本的日志记录...")
        # 单线程模式
        logger_single_thread = LogManager.get_instance(use_mp=False, log_path=r'd:\logs')
        logger_single_thread.INFO("【Single Thread Demo】开始演示单线程日志记录。")
        demo_single_thread_logging()

        # 多线程模式
        logger_multithread = LogManager.get_instance(use_mp=False, log_path=r'd:\logs')
        logger_multithread.INFO("【Multithread Demo】开始演示多线程日志记录。")
        demo_multithread_logging()

        # 演示装饰器：调用被 @auto_log 装饰的 sample_function
        result = sample_function(5, 7)
        LogManager.get_instance().INFO(f"sample_function(5, 7) 返回结果：{result}")

    # 等待一段时间确保所有异步日志都写入（可根据实际情况调整时间）
    time.sleep(5)
    LogManager.get_instance().stop()
    print("Logger demo 执行完毕，日志已写入至日志目录。")
    log_path = LogManager.get_instance().config.get("log_path", "./logs")
    print(f"请查看日志文件（目录：{log_path}）以验证输出。")


# ---------------------------
# Dashboard 用法说明
# ---------------------------
def demo_dashboard_usage():
    print("\n--------------------------------------")
    print("Dashboard 演示：")
    print("请打开终端，执行以下命令启动 Dashboard：")
    print("  uvicorn dashboard.main:app --reload")
    print("启动后，在浏览器中访问 http://127.0.0.1:8000 即可查看实时日志查询界面。")
    print("--------------------------------------\n")


# ---------------------------
# 主入口
# ---------------------------
if __name__ == '__main__':
    # 控制是否使用多进程
    demo_logger_usage(use_mp=True)  # 使用多进程版本
    # demo_logger_usage(use_mp=False)  # 使用单进程版本
    # demo_dashboard_usage()
    print("Demo 演示完成。")
