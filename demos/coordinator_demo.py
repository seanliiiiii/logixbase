import time
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

from logixbase.logger import LogManager
from logixbase.executor import Coordinator


def once_task(x, y, sum_val, from_upstream, logger=None):
    """Once 模式任务：执行一次并返回结果字典。"""
    logger.INFO(f"[ONCE] x={x}, y={y}, sum_val={sum_val}, from_upstream={from_upstream}")
    return {"x": x, "y": y, "sum_val": sum_val, "from_upstream": from_upstream}


def hybrid_task(prefix, from_upstream, logger=None):
    """Hybrid 模式任务：每轮处理上游数据并返回字符串。"""
    logger.INFO(f"[HYBRID] prefix={prefix}, from_upstream={from_upstream}")
    return f"{prefix}-{from_upstream}"


def loop_task(name, req_queue=None, pub_queue=None, stop_event=None, logger=None):
    """Loop 模式任务：持续监听 req_queue 并将处理结果推入 pub_queue。"""
    logger.INFO(f"[LOOP] {name} listening...")
    while not stop_event.is_set():
        try:
            upstream = req_queue.get()
            if upstream is None:
                continue

            logger.INFO(f"[LOOP] {name} got {upstream}")
            pub_queue.put(f"{name}-{upstream}")
        except Exception as e:
            logger.ERROR(f"[LOOP] {name} failed: {e}")


def run_demo(mode):
    task_modes = ["once", "hybrid", "loop"]

    for task_mode in task_modes:
        logger = LogManager(use_mp=True if mode in ("process", "pool") else False, log_path=r'd:\logs\coordinator')
        print(f"\n===== Running Coordinator Demo: mode = {task_mode} {mode} =====")
        # 初始化 Coordinator
        coor = Coordinator(mode=mode, task_mode=task_mode, max_workers=4)
        coor.bind_share(logger=logger)

        # 注册任务
        if task_mode == "once":
            coor.submit(1, once_task, 1, 2, 3, 4, task_name="onceA")
            coor.submit(2, once_task, 4, 5, 9, upstream_keys=["from_upstream"], task_name="onceB")
        elif task_mode == "hybrid":
            coor.submit(1, hybrid_task, "H1", 123, task_name="hybridA")
            coor.submit(2, hybrid_task, "H2", upstream_keys=["from_upstream"], task_name="hybridB")
        else:  # loop
            coor.submit(1, loop_task, name="L1", task_name="loopA")
            coor.submit(2, loop_task, name="L2", task_name="loopB")

        # 启动协调器
        coor.start()
        time.sleep(3)
        logger.start()
        # 运行一段时间以观察日志输出
        time.sleep(5)
        # 停止协调器
        coor.stop()
        logger.stop()

if __name__ == "__main__":
    run_demo("process")
