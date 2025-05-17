import multiprocessing
import time
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent.parent.joinpath('logixbase')))

from configer.loader import ConfigLoader
from configer.schema import AppConfig  # 假设你已经在 schema.py 中定义好 AppConfig 结构

CONFIG_PATH = Path(".\configs")  # 所有配置文件目录，需自行准备

def test_1_load_single_path_all_formats():
    print("\n--- Test 1: 加载多格式文件（无热更新） ---")
    loader = ConfigLoader(CONFIG_PATH)
    loader.load()
    print("数据库地址:", loader.config.database.host)
    print("邮件SMTP:", loader.config.mail.host)

def test_2_ini_levels():
    print("\n--- Test 2: 加载 INI 多层级（无热更新） ---")
    loader = ConfigLoader(CONFIG_PATH, mode="prod")
    loader.load()
    print("Prod 模式密码:", loader.config.mail.password)

def test_3_single_file_load():
    print("\n--- Test 3: 加载单文件（无热更新） ---")
    for suffix in [".ini", ".yaml", ".py"]:
        file = CONFIG_PATH / f"CFG.base{suffix}"
        print(f"\n加载文件: {file}")
        loader = ConfigLoader(file)
        loader.load()
        print("邮箱用户名:", loader.config.mail.username)

    loader = ConfigLoader(CONFIG_PATH / "cfg.ini")
    loader.load()
    print(f"\n加载文件: {CONFIG_PATH / 'cfg.ini'}")
    print("邮箱用户名:", loader.config.mail.username)

def test_4_single_proc_hot_reload():
    print("\n--- Test 4: 单进程热更新测试（请手动修改配置文件观察） ---")
    loader = ConfigLoader(CONFIG_PATH)
    loader.load()
    loader.start()

    def on_change():
        print(">>> 主进程回调: 新配置:", loader.config.mail.host)

    loader.register_callback(on_change)

    try:
        while True:
            time.sleep(2)
    except KeyboardInterrupt:
        loader.stop()
        print("已停止热更新")

def worker_proc(loader: ConfigLoader):
    def on_child_update():
        print(f"[子进程] 配置已更新：当前SMTP = {loader.config.mail.host}")
    loader.register_mp_proxy()
    loader.register_proxy_callback(on_child_update)
    while True:
        time.sleep(2)

def test_5_multi_proc_hot_reload():
    print("\n--- Test 5: 多进程热更新测试（请手动修改配置文件观察） ---")
    loader = ConfigLoader(CONFIG_PATH, use_proxy=True)
    loader.load()

    # 启动子进程
    processes = []
    for _ in range(2):
        p = multiprocessing.Process(target=worker_proc, args=(loader,))
        p.start()
        processes.append(p)

    def on_main_update():
        print("[主进程] 配置已更新：当前数据库地址 =", loader.config.database.host)

    loader.register_callback(on_main_update)
    loader.start(CONFIG_PATH)

    try:
        while True:
            time.sleep(2)
    except KeyboardInterrupt:
        for p in processes:
            p.terminate()
        loader.stop()
        print("已终止所有子进程与热更新")


if __name__ == "__main__":
    # 执行入口（如需测试，请取消注释一项）
    # test_1_load_single_path_all_formats()
    # test_2_ini_levels()
    # test_3_single_file_load()
    # test_4_single_proc_hot_reload()
    test_5_multi_proc_hot_reload()
