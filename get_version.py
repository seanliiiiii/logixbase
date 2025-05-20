# get_version.py
import re
from pathlib import Path

init_path = Path(__file__).parent / 'logixbase' / '__init__.py'

try:
    content = init_path.read_text(encoding='utf-8')
    match = re.search(r"__version__\s*=\s*['\"](.+?)['\"]", content)
    if match:
        print(match.group(1))
    else:
        print("VERSION_NOT_FOUND")
except Exception:
    print("VERSION_NOT_FOUND")
