import time
import random
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent.parent))

from logixbase.executor import MultiTaskExecutor
from logixbase.logger import LogManager

# 示例函数（测试：参数组合、异常、内存返回）
def example_task(x, y, delay=0.1, logger=None):
    time.sleep(delay + random.uniform(0, 0.3))
    if x == -1:
        raise ValueError("intentional error")
    output = x ** 2 + y
    logger.INFO(f"Task input: x={x}, y={y} -> output={output}")
    return {"input": (x, y), "output": output, "memory_usage": random.uniform(10, 50)}


def generate_param_combinations():
    param_set = []
    for x in range(-1, 5):     # 包含一个错误任务（x = -1）
        for y in [0, 1, 2]:     # 多参数组合测试
            param_set.append((x, y))
    return param_set


def run_executor_demo(mode='process'):
    print(f"\n==== Running {mode.upper()} mode demo ====")

    loger = LogManager.get_instance(use_mp=True if mode in ("process", "pool") else False,
                                    log_path=r'd:\logs')

    executor = MultiTaskExecutor(mode=mode)
    executor.bind_share(logger=loger)

    # 多任务组合参数测试
    for x, y in generate_param_combinations():
        executor.submit(example_task, x, y, delay=0.1, group='combi', tags=[f"x={x}", f"y={y}"], task_name=f"task_{x}_{y}")

    # 执行任务
    executor.start()

    if mode == 'pool':
        executor.join()
        loger.start()
    else:
        loger.start()
        executor.join()
    loger.stop()
    # 报告与导出
    executor.report(r"d:\logs", "csv", True)
    # 错误与取消任务处理测试
    failed = [tid for tid in executor.get_task_ids() if executor.get_status(tid) == 'error']
    if failed:
        print(f"Retrying failed tasks: {failed}")
        executor.retry_failed()

    any_id = executor.get_task_ids()[0]
    executor.cancel_task(any_id)
    executor.rerun_cancelled()

if __name__ == "__main__":
    run_executor_demo('thread')
    # run_executor_demo('pool')
    # run_executor_demo('process')
