# -*- coding: utf-8 -*-
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))


from demoproj.test import run


if __name__ == "__main__":
    print(run(100, 200))
