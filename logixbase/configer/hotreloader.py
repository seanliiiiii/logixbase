from multiprocessing import Manager, Event, Value
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import time


from ..protocol import OperationProtocol as OPP


class HotReloader(OPP):
    def __init__(self, config_path: Path, loader):
        self.config_path = Path(config_path)

        self.loader = loader
        self._share_data: Manager.Event = None
        self._share_event: Event = None
        self._share_response: Manager.Queue = None
        self._proxy_num: Value = None
        self._logger = None

        self._observer = None
        self._handler = None
        self._callbacks = []
        self._last_reload_time = 0

    def INFO(self, msg: str):
        if self._logger is not None:
            self._logger.INFO(msg)
        else:
            print(f"[HotReload] [INFO] {msg}")

    def ERROR(self, msg: str):
        if self._logger is not None:
            self._logger.ERROR(msg)
        else:
            print(f"[HotReload] [ERROR] {msg}")

    def register_callback(self, callback):
        self._callbacks.append(callback)

    def bind_share(self, share_data: dict, share_event: Event, share_response: "Manager.Queue",
                   proxy_num: Value, logger = None):
        self._share_data = share_data
        self._share_event = share_event
        self._share_response = share_response
        self._proxy_num = proxy_num
        self._logger = logger

    def start(self):
        self._share_data.update(self.loader.config.model_dump())
        self._handler = self._build_handler()
        self._observer = Observer()
        self._observer.schedule(self._handler, str(self.config_path), recursive=True)
        self._observer.start()
        self.INFO("文件监听已启动")

    def join(self):
        pass

    def stop(self):
        if self._observer:
            self._observer.stop()
            self._observer.join()
            self.INFO("文件监听已停止")

    def _build_handler(self):
        manager = self

        class ChangeHandler(FileSystemEventHandler):
            def on_modified(self, event):
                if event.is_directory:
                    return

                if time.time() - manager._last_reload_time < 1.0:
                    return
                manager._last_reload_time = time.time()

                try:
                    manager.INFO(f"监听到文件变更: {event.src_path}")
                    new_instance = manager.loader.load(manager.config_path)
                    is_diff = manager._print_diffs(new_instance.model_dump())

                    if not is_diff:
                        manager.INFO("配置文件夹出现更新，但未识别到有效更改，请确认更改文件是否正确")
                        return
                    manager._share_data.clear()
                    manager._share_data.update(new_instance.model_dump())
                    manager._share_event.set()

                    for cb in manager._callbacks:
                        try:
                            cb()
                        except Exception as e:
                            manager.ERROR(f"[主进程回调错误] {cb.__name__}: {e}")
                except Exception as e:
                    manager.ERROR(f"[热更新失败] {e}")
                finally:
                    # 确保在多进程模式下所有子进程都接收到最新的配置并执行回调
                    if manager._proxy_num.value > 0:
                        count = 0
                        while count < manager._proxy_num.value:
                            manager._share_response.get()
                            count += 1
                    manager._share_event.clear()

        return ChangeHandler()

    def _print_diffs(self, new_cfg: dict):
        diff = False
        shared = self._share_data
        for module_name in shared.keys():
            if module_name not in new_cfg:
                continue
            old_module: dict = shared[module_name]
            new_module: dict = new_cfg[module_name]
            for field in old_module.keys():
                if field not in new_module:
                    continue
                old_val = old_module[field]
                new_val = new_module[field]
                if old_val != new_val:
                    self.INFO(f"[配置变更] 模块: {module_name}, 字段: {field}, 旧值: {old_val}, 新值: {new_val}")
                    diff = True
        return diff
