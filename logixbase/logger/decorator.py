import time
import traceback
from .core import LogManager


def auto_log(func):
    def wrapper(*args, **kwargs):
        lm = LogManager.get_instance()
        log_id = f"{func.__module__}.{func.__name__}"
        start_time = time.time()
        lm.log("INFO", f"函数 {func.__name__} 开始执行。", log_id=log_id)
        try:
            result = func(*args, **kwargs)
            elapsed = time.time() - start_time
            lm.log("INFO", f"函数 {func.__name__} 执行完毕，耗时 {elapsed:.3f} 秒。", log_id=log_id)
            return result
        except Exception as e:
            err_msg = f"函数 {func.__name__} 异常: {str(e)}\n{traceback.format_exc()}"
            lm.log("ERROR", err_msg, log_id=log_id)
            raise
    return wrapper
