import pathlib
import re
from setuptools import setup

# 读取 __version__ 字符串
def read_version() -> str:
    """
    从 pyproject.toml 中通过正则提取 version 字段。
    注意：如果 pyproject.toml 格式非常复杂，可能需要更健壮的解析器。
    """
    text = pathlib.Path(__file__).parent.joinpath("pyproject.toml") \
                   .read_text(encoding="utf-8")
    # 匹配形如：version = "x.y.z"
    m = re.search(r'^\s*version\s*=\s*"([^"]+)"', text, re.MULTILINE)
    if not m:
        raise RuntimeError("Cannot find version in pyproject.toml")
    return m.group(1)

setup(
    version=read_version(),
)
