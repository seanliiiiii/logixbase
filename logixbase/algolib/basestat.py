# 统计分析模块
"""
本模块包含了多个用于统计分析的函数。
主要功能包括：
1. 相关系数计算（皮尔逊相关系数、调整后的相关系数、等级相关系数）
2. 协方差计算
3. 均方误差计算
4. 偏度计算
5. 数组排名计算

这些函数可以帮助进行基本的统计分析，计算各种统计量，并评估数据之间的关系。
"""
import numpy as np
from numba import njit


@njit
def corr(x, y):
    """
    计算两个数组之间的皮尔逊相关系数。

    Args:
        x (array-like): 第一个数组。
        y (array-like): 第二个数组。

    Returns:
        float: 皮尔逊相关系数。

    """
    mu_x = np.nanmean(x)
    mu_y = np.nanmean(y)
    x_ = x - mu_x
    y_ = y - mu_y
    sum_x_2 = np.nansum(x_ ** 2)
    sum_y_2 = np.nansum(y_ ** 2)
    sum_x_y = np.nansum(x_ * y_)
    divisor = np.sqrt(sum_x_2 * sum_y_2)
    if divisor == 0:
        return np.nan
    else:
        return sum_x_y / divisor


@njit
def corr_adj(x, y, mu_x, mu_y):
    """
    计算x和y在给定均值假设下的皮尔逊相关系数

    Args:
        x (numpy.ndarray): 数组x
        y (numpy.ndarray): 数组y
        mu_x (float): x的假设均值
        mu_y (float): y的假设均值

    Returns:
        float: 调整后的相关系数

    """
    x_ = x - mu_x
    y_ = y - mu_y

    sum_x_2 = np.nansum(x_ ** 2)
    sum_y_2 = np.nansum(y_ ** 2)
    sum_x_y = np.nansum(x_ * y_)

    divisor = np.sqrt(sum_x_2 * sum_y_2)
    if divisor == 0:
        return np.nan
    else:
        return sum_x_y / divisor


@njit
def cov(x, y):
    """
    计算x和y之间的协方差

    Args:
        x (array): 数组x
        y (array): 数组y

    Returns:
        float: 协方差

    """
    mu_x = np.mean(x)
    mu_y = np.mean(y)
    x_ = x - mu_x
    y_ = y - mu_y

    return np.dot(x_, y_) / (len(x) - 1)


@njit
def rank_corr(x, y):
    """
    计算x和y之间的斯皮尔曼等级相关系数。

    Args:
        x (numpy.ndarray): 数组x。
        y (numpy.ndarray): 数组y。

    Returns:
        float: 等级相关系数。

    """
    x_ = sorted_rank(x)
    y_ = sorted_rank(y)

    n = len(x_)
    dd = np.nansum((x_ - y_) ** 2)

    nn = n * (n ** 2 - 1)

    return 1 - (6 * dd / nn)


@njit
def skew(x):
    """
    计算数组x的偏度（与pandas对齐）

    Args:
        x (np.ndarray): 输入数组

    Returns:
        float: 偏度

    """
    n = x.shape[0]
    devi = x - np.mean(x)
    div = np.sqrt(np.sum(devi ** 2) / (n - 1))
    if (n <= 2) or (div == 0):
        return np.nan
    else:
        return np.sum((devi / div) ** 3) * n / (n - 1) / (n - 2)

@njit
def sorted_rank(arr: np.array, reverse=True):
    """
    计算数组中每个值的排名。

    Args:
        arr (np.array): 输入数组。
        reverse (bool, optional): 是否降序排序。默认为True。

    Returns:
        np.array: 排名数组。

    """
    idx = np.arange(arr[np.isfinite(arr)].shape[0]) + 1
    sorted_args = np.argsort(arr)
    rank = np.full(arr.shape, np.nan)

    n = 0
    rank_tmp = idx[n]
    for x in range(1, sorted_args.shape[0] + 1):
        ix = -x
        arr_loc = sorted_args[ix]
        if not np.isnan(arr[arr_loc]):
            if not (arr[arr_loc] == arr[sorted_args[ix + 1]]):
                rank_tmp = idx[n]
                rank[arr_loc] = rank_tmp
            else:
                rank[arr_loc] = rank_tmp
            n += 1

    if not reverse:
        rank = arr.shape[0] - rank + 1

    return rank


@njit
def outlier_iqr(data: np.array):
    """
    使用四分位距（IQR）方法计算异常值检测的上限和下限。

    Args:
        data (np.array): 一维数据数组。

    Returns:
        Tuple[float, float]: 一个包含上限和下限的元组，用于检测异常值。

    """
    q1 = np.nanquantile(data, 0.25)
    q3 = np.nanquantile(data, 0.75)

    lower = q1 - 1.5 * (q3 - q1)
    upper = q3 + 1.5 * (q3 - q1)

    return upper, lower


@njit
def outlier_sig(data: np.array):
    """
    使用3.89个标准差的方法计算异常值检测的上下界

    Args:
        data (np.array): 一维数据数组

    Returns:
        Tuple[float, float]: 浮点数的元组，包含上界和下界
    """

    std = np.nanstd(data)

    lower = np.nanmean(data) - 3.89 * std
    upper = np.nanmean(data) + 3.89 * std

    return upper, lower


@njit
def eu_distance(y: np.array, x: np.array):
    """
    计算x和y之间分布的欧几里得距离值

    Args:
        y (np.array): 一维数组
        x (np.array): 一维数组

    Returns:
        float: 欧几里得距离的浮点数

    """
    return np.sqrt(np.sum(np.square(x - y)))


@njit
def cosine(y: np.array, x: np.array):
    """
    计算向量x和y之间的余弦相似度

    Args:
        y (np.array): 一维数组
        x (np.array): 一维数组

    Returns:
        float: x和y之间的余弦相似度

    """

    linglg_x = np.linalg.norm(x)
    linglg_y = np.linalg.norm(y)

    if linglg_x * linglg_y != 0:
        return np.dot(y, x) / (linglg_x * linglg_y)
    else:
        return np.nan


@njit
def _phi(arr: np.array, m: int, r: float):
    """
    计算给定数组在时间序列上的phi系数。

    Args:
        arr (np.array): 输入的时间序列数组。
        m (int): 移动窗口的大小。
        r (float): 用于计算距离的阈值倍数。

    Returns:
        float: 给定数组在时间序列上的phi系数。

    """
    # Prepare sub-set data on given step window
    size = arr.shape[0]
    x = np.full((size - m + 1, m), np.nan)
    thres = np.std(arr) * r
    for i in range(m):
        x[:, i] = arr[i:(size + i - m + 1)]

    c_ = np.zeros(x.shape[0])
    # Calculate distances of each sub-set
    for i in np.arange(len(x)):
        x_ = x[i]
        count = 0
        for j in np.arange(len(x)):
            y_ = x[j]
            count += max(np.abs(x_ - y_)) <= thres

        c_[i] = np.log(count / (size - m + 1))
    return 1 / (size - m + 1) * np.sum(c_)


@njit
def approximate_entropy(data: np.array, window: int, thres: float):
    """
    计算输入序列的近似熵

    Args:
        data (np.array): 数据数组
        window (int): 计算序列距离的窗口大小
        thres (float): 惩罚项

    Returns:
        float: 近似熵值
    """
    return np.abs(_phi(data, window + 1, thres) - _phi(data, window, thres))


@njit
def min_max(arr: np.array, lower: int = 0, upper: int = 1):
    """
    对数组进行归一化处理，将其缩放到指定的范围[lower, upper]之间。

    Args:
        arr (np.array): 需要进行归一化处理的数组。
        lower (int, optional): 归一化后的数组的最小值，默认为0。
        upper (int, optional): 归一化后的数组的最大值，默认为1。

    Returns:
        np.array: 归一化后的数组。

    """
    max_ = np.max(arr)
    min_ = np.min(arr)

    if max_ == min_:
        return np.ones(arr.shape) * upper
    else:
        return (upper - lower) * (arr - min_) / (max_ - min_) + lower


@njit
def uniform_weighting(data: np.array) ->np.array:
    """
    对数组进行均匀加权处理。

    Args:
        data (np.array): 输入的numpy数组。

    Returns:
        np.array: 经过均匀加权处理后的numpy数组。

    """
    abs_sum = sum(abs(data))
    result_ = abs(data)/abs_sum
    return result_


@njit
def corr_prob(x: np.array, y: np.array, mu_x: float, mu_y: float):
    """
    计算数组x和数组y中的元素与各自均值mu_x和mu_y相比，显示出相同方向的概率。

    Args:
        x (np.array): 一个numpy数组，包含需要分析的数据点。
        y (np.array): 一个numpy数组，包含需要分析的数据点。
        mu_x (float): 数组x的均值。
        mu_y (float): 数组y的均值。

    Returns:
        float: x和y中元素相对于各自均值显示出相同方向的概率。

    """
    ttl = x != mu_x
    hit = ((x > mu_x) & (y > mu_y)) | ((x < mu_x) & (y < mu_y))

    if np.sum(ttl) != 0:
        return np.sum(hit) / np.sum(ttl) - 0.5
    else:
        return 0
