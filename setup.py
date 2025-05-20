import pathlib
import re
from setuptools import setup

# 读取 __version__ 字符串
def read_version():
    here = pathlib.Path(__file__).parent
    init_path = here / "logixbase" / "__init__.py"
    content = init_path.read_text(encoding="utf-8")
    match = re.search(r"__version__\s*=\s*['\"](.+?)['\"]", content)
    if not match:
        raise RuntimeError("Unable to find __version__ in logixbase/__init__.py")
    return match.group(1)

setup(
    version=read_version(),
)
