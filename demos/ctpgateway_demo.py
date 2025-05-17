import sys
from pathlib import Path
from multiprocessing import Queue

sys.path.append(str(Path(__file__).parent.parent))

from logixbase.gateway import ctp
from logixbase.logger import LogManager
from logixbase.configer import load_schema


class Test:

    def __init__(self):
        super().__init__()
        self.q = Queue()

    def on_tick(self, tick):
        """"""
        if self.q.empty():
            self.q.put(tick)


if __name__ == "__main__":
    loger = LogManager.get_instance(use_mp="process", log_path=r'd:\logs')
    conn_cfg = load_schema(r'D:\licx\codes\usercfg\odin\account.ini', ctp.CtpConfig)
    callback = Test()

    gateway = ctp.CtpGateway(conn_cfg, loger)
    gateway.register_callback(callback.on_tick)
    gateway.connect()
    info = gateway.subscribe(["i2509", "MA510", "lu2509", "rb2510"])
    # instruments = spi.qry_instrument()
    # margin = spi.qry_margin_rate("rb2410")
    # comm = spi.qry_commission_rate("rb2410")
    # tick = spi.qry_depth_market_data("rb2410")
    # td_code = spi.qry_trading_code("SHFE")

    if not callback.q.empty():
        tick = callback.q.get()

    self = ctp.CtpTickClean()
    for k, v in info.items():
        self.register(k, v)
    tick = self.clean_tick(tick)

