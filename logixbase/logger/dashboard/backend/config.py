# backend/config.py
DEFAULT_CONFIG = {
    "HOST": "0.0.0.0",
    "PORT": 8000,
    "LOG_ROOT": "D:/logs",
    "POLLING_INTERVAL_SECONDS": 60,
    "CACHE_LOG_LINE_COUNT": 100
}


def parse_args():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", type=str)
    parser.add_argument("--port", type=int)
    parser.add_argument("--log_root", type=str)
    parser.add_argument("--poll_interval", type=int)
    parser.add_argument("--cache_lines", type=int)
    return parser.parse_args()


def merge_config(args) -> dict:
    config = DEFAULT_CONFIG.copy()
    if args.host:
        config["HOST"] = args.host
    if args.port:
        config["PORT"] = args.port
    if args.log_root:
        config["LOG_ROOT"] = args.log_root
    if args.poll_interval:
        config["POLLING_INTERVAL_SECONDS"] = args.poll_interval
    if args.cache_lines:
        config["CACHE_LOG_LINE_COUNT"] = args.cache_lines
    return config
