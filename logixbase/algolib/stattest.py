import scipy.stats as st
import numpy as np

from statsmodels.tsa.stattools import adfuller


def kstest(data: np.array, dist: str = 'norm'):
    """
    对输入数据进行假设分布的Kolmogorov-Smirnov检验

    Args:
        data (np.array): 数据数组
        dist (str, optional): 假设的分布类型. 默认为 'norm'.

    Returns:
        Tuple[float, float]: 包含Kolmogorov-Smirnov检验的统计量和p值的元组
    """
    t, p = st.kstest(data, cdf=dist.lower())

    return t, p


def adfuller_test(data: np.array):
    """
    对给定的数据进行ADF（Augmented Dickey-Fuller）单位根检验。

    Args:
        data (np.array): 需要进行ADF检验的时间序列数据。

    Returns:
        tuple: ADF检验的结果，包括检验统计量、p值、滞后阶数、使用的观测值数量、临界值和置信区间等。

    """
    results = adfuller(data)
    return results


