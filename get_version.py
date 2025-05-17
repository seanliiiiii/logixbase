# get_version.py
import re

with open("logixbase/__init__.py", encoding="utf-8") as f:
    content = f.read()

match = re.search(r"__version__\s*=\s*['\"]([^'\"]+)['\"]", content)
if match:
    print(match.group(1))
else:
    print("VERSION_NOT_FOUND")
