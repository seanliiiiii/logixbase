import os
import setuptools
import re

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

# 从__init__.py中获取版本信息
with open(os.path.join("logixbase", "__init__.py"), encoding="utf-8") as f:
    version = re.search(r"__version__\s*=\s*['\"]([^'\"]*)['\"]", f.read()).group(1)

# 递归获取所有包
packages = setuptools.find_packages()

setuptools.setup(
    name="logixbase",
    version=version,
    author="LogixBase开发团队",
    author_email="trader@logixquant.com",
    description="高度模块化的量化交易策略开发框架",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/339640170/logixbase",
    packages=packages,
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Financial and Insurance Industry",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Operating System :: OS Independent",
        "Topic :: Office/Business :: Financial :: Investment",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    python_requires=">=3.9",
    install_requires=[
        "numpy>=1.20.0",
        "pandas>=1.3.0",
        "pyyaml>=6.0",
        "pytz>=2022.1",
        "requests>=2.27.0",
        "sqlalchemy>=1.4.0",
        "matplotlib>=3.5.0",
        "numba>=0.55.0",
        "statsmodels>=0.13.0",
        "seaborn>=0.11.0",
        "scikit-learn>=1.0.0",
        "scipy>=1.7.0",
        "cryptography==41.0.5",
        "bcrypt==4.1.1",
        "Cython>=0.29.0",
        "pydantic>=1.9.0",
        "watchdog>=2.1.0",
        "psutil>=5.8.0",
        "tqsdk>=1.9.0",
        "path>=16.4.0",
        "cx_Oracle>=8.0.0",
        "pymssql>=2.2.0",
        "uvicorn>=0.17.0",
        "fastapi>=0.70.0",
        "pandas_market_calendars>=4.0.0",
        "paramiko==2.10.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=3.0.0",
            "black>=22.1.0",
            "flake8>=4.0.0",
            "mypy>=0.931",
            "sphinx>=4.4.0",
            "build>=0.8.0",
            "twine>=4.0.0",
        ],
    },
) 