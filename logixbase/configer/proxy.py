import threading
import time
from multiprocessing import Manager, Event
from typing import Type, Union
from pydantic import BaseModel


class ConfigProxy:
    def __init__(self, schema_cls: Type[BaseModel]):
        self._schema_cls = schema_cls

        self._instance: Union[BaseModel, None] = None
        self._callbacks: list[callable] = []

        self._share_data = None
        self._event: Event = None
        self._response: Manager.Queue = None
        self._stop_signal = None
        self._listener_thread: Union[BaseModel, None] = None

    def bind_share(self, shared_data: dict, share_event: Event, share_response: "Manager.Queue"):
        self._share_data = shared_data
        self._event = share_event
        self._response = share_response

    def set_instance(self, new_instance: BaseModel):
        if not isinstance(new_instance, self._schema_cls):
            raise TypeError(f"配置实例类型错误，应为 {self._schema_cls.__name__}")
        self._instance = new_instance

    def get_instance(self) -> BaseModel:
        if not self._instance:
            raise RuntimeError("ConfigProxy 尚未绑定配置实例")
        return self._instance

    def register_callback(self, callback):
        self._callbacks.append(callback)

    def start_event_listener(self):
        if self._event is None or self._share_data is None:
            raise RuntimeError("事件或共享数据未绑定")
        if ((self._listener_thread is not None)
                and self._listener_thread.is_alive()):
            return
        self._stop_signal = threading.Event()
        self._listener_thread = threading.Thread(target=self._event_loop, daemon=True)
        self._listener_thread.start()

    def stop_event_listener(self):
        self._stop_signal.set()
        if self._listener_thread:
            self._listener_thread.join()

    def _event_loop(self):
        while not self._stop_signal.is_set():
            # 等待配置文件更新事件触发
            self._event.wait()
            # 通知HotReloader配置更新事件已接收，并等待10ms后执行更新和回调
            self._response.put(True)
            time.sleep(0.05)
            try:
                raw_data = dict(self._share_data)
                instance = self._schema_cls(**raw_data)
                self.set_instance(instance)
                self._fire_callbacks()
            except Exception as e:
                print(f"[子进程配置更新失败] {e}")

    def _fire_callbacks(self):
        for cb in self._callbacks:
            try:
                cb()
            except Exception as e:
                print(f"[回调错误] {cb.__name__}: {e}")

    def dict(self, *args, **kwargs):
        if self._instance is None:
            raise RuntimeError("ConfigProxy 尚未绑定配置实例")
        return self._instance.model_dump(*args, **kwargs)

    def json(self, *args, **kwargs):
        if self._instance is None:
            raise RuntimeError("ConfigProxy 尚未绑定配置实例")
        return self._instance.model_dump_json(*args, **kwargs)