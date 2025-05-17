# 时间序列分析模块
"""
本模块包含了多个用于时间序列分析的函数。
主要功能包括：
1. 累积和计算
2. 指数加权移动平均（EWMA）计算
3. 未来窗口统计量计算
4. Hurst指数计算
5. 滚动窗口统计量计算

这些函数可以帮助进行基本的时间序列分析，计算各种统计量，并评估时间序列数据的特性。
"""

import numpy as np
import math
from numba import jit, njit
from .regression import regress
from .basestat import sorted_rank


def rolling_calculation(input_data: np.ndarray, how: str, window: int, min_data: int = 2,
                        min_periods: int = 10):
    """
    计算滚动窗口统计量

    Args:
        input_data (np.ndarray): 输入数据数组，支持一维或多维数组。
        how (str): 计算方法，可选值包括 'mean'（均值）、'std'（标准差）、'max'（最大值）、'min'（最小值）、
                   'sum'（总和）、'count'（计数）、'median'（中位数）、'z_score'（Z分数）、'quantile'（分位数）、
                   'beta'（贝塔分布）、'residual'（残差）、'rank'（排名）。
        window (int): 滚动窗口的大小。
        min_data (int, optional): 窗口计算所需的最小数据数量，默认为2。
        min_periods (int, optional): 开始计算所需的最小周期数，默认为10。如果为None，则使用窗口大小作为最小周期数。

    Returns:
        np.ndarray: 计算结果数组，形状与输入数组相同，包含计算得到的滚动统计量。

    """
    raw_data = input_data.copy().astype(float)

    if min_periods is None:
        min_periods = window
    shape = len(raw_data.shape)
    start = np.where(raw_data == raw_data[np.isfinite(raw_data)][0])[0][0] + min_periods

    result = np.full(raw_data.shape, np.nan)
    # 对于向量输入的滚动计算：仅考虑沿列的情况
    if shape == 1:
        for i in range(start - 1, len(raw_data)):
            data = raw_data[max(0, i - window + 1):(i + 1)]
            if np.sum(np.isfinite(data)) < min_data:
                continue
            else:
                result[i] = func(data, how)
    # 对于多维输入的滚动计算：考虑沿列/沿行的情况
    elif shape > 1:
        for j in range(raw_data.shape[1]):
            for i in range(start - 1, raw_data.shape[0]):
                data = raw_data[max(0, i - window + 1):(i + 1), j]
                if np.sum(np.isfinite(data)) < min_data or np.isnan(data[-1]):
                    continue
                else:
                    result[i, j] = func(data, how)
    return result


@njit
def func(data: np.array, method: str):
    """
    根据指定方法计算统计量。

    Args:
        data (np.array): 输入数据数组。
        method (str): 计算方法，可以是以下值之一：
                      'MEAN' - 计算平均值
                      'STD' - 计算标准差
                      'MAX' - 计算最大值
                      'MIN' - 计算最小值
                      'SUM' - 计算总和
                      'COUNT' - 计算有效值的数量
                      'MEDIAN' - 计算中位数
                      'Z_SCORE' - 计算最后一个有效值的Z分数
                      'QUANTILE' - 计算最后一个有效值的分位数
                      'BETA' - 计算与时间的线性回归系数
                      'RESIDUAL' - 计算与时间的线性回归的最后一个残差
                      'RANK' - 计算最后一个有效值的排序位置

    Returns:
        float: 计算结果，如果输入数据无效或方法无效，则返回NaN。

    """
    stat = np.nan
    mask = np.isfinite(data)
    tmp = data[mask]

    if np.isnan(data[-1]) or tmp.shape[0] < 2:
        return np.nan

    if method.upper() == 'MEAN':
        stat = np.mean(tmp)
    elif method.upper() == 'STD':
        stat = np.sqrt(((tmp - np.mean(tmp)) ** 2).sum() / (len(tmp) - 1))
    elif method.upper() == 'MAX':
        stat = np.max(tmp)
    elif method.upper() == 'MIN':
        stat = np.min(tmp)
    elif method.upper() == 'SUM':
        stat = np.sum(tmp)
    elif method.upper() == 'COUNT':
        stat = tmp.shape[0]
    elif method.upper() == 'MEDIAN':
        stat = np.median(tmp)
    elif method.upper() == 'Z_SCORE':
        stdev = np.sqrt(np.sum(((tmp - np.mean(tmp)) ** 2)) / (len(tmp) - 1))
        stat = (tmp[-1] - np.mean(tmp)) / stdev if stdev != 0 else np.nan
    elif method.upper() == 'QUANTILE':
        stat = tmp[tmp < tmp[-1]].shape[0] / (len(tmp) - 1)
    elif method.upper() == 'BETA':
        time_array = np.arange(1, len(data) + 1)
        stat = regress(tmp, time_array[mask])[0]
    elif method.upper() == 'RESIDUAL':
        time_array = np.arange(1, len(data) + 1)
        stat = regress(tmp, time_array[mask])[2][-1]
    elif method.upper() == 'RANK':
        stat = sorted_rank(tmp)[-1]

    return stat


@njit
def bootstrap_dt(ts_len: int, total_ts_len: int, max_overlap: int, min_len: int):
    """
    对时间序列进行Bootstrap抽样，以估计其分布

    Args:
        ts_len (int): 每个Bootstrap样本的长度
        total_ts_len (int): 总样本的长度
        max_overlap (int): 重叠长度的最大限制
        min_len (int): 每个样本的最小长度

    Returns:
        ndarray: 二维数组，其中每一列记录每个样本的起始和结束位置

    """
    mask = np.ones(total_ts_len)
    mask[total_ts_len - min_len + 1:] = 0
    array_a = np.arange(total_ts_len)

    mx = np.zeros((mask.shape[0], 2))

    count = 0
    while np.any(mask):
        random_num = np.random.choice(array_a[mask == 1])
        forbidden_range = np.arange(max(0, random_num - max_overlap + 1),
                                    min(total_ts_len, random_num + max_overlap - 1 + 1))
        mask[forbidden_range] = 0
        mx[count, 0] = random_num
        mx[count, 1] = min(random_num + ts_len, total_ts_len)
        count += 1
    mx = mx[:count]
    return mx


@jit
def ewma_vol(input_ret, lambda_para):
    """
    计算指数加权移动平均（EWMA）波动率。

    Args:
        input_ret (numpy.ndarray): 输入的收益率数组。
        lambda_para (float): EWMA的lambda参数，其值应在0到1之间。

    Returns:
        float: 计算得到的EWMA波动率。

    """
    raw_ret = input_ret.copy()
    size = len(raw_ret)
    avg = np.mean(raw_ret)

    e = np.arange(size - 1, -1, -1)
    r = np.repeat(lambda_para, size)
    vec_lambda = np.power(r, e)

    sxxewm = (np.power(raw_ret - avg, 2) * vec_lambda).sum()
    vart = sxxewm / vec_lambda.sum()
    vol = math.sqrt(vart)

    return vol


@njit
def ewma(x, lamb):
    """
    计算指数加权移动平均（EWMA）

    Args:
        x (np.ndarray): 输入数组
        lamb (float): EWMA的lambda参数

    Returns:
        float: EWMA值

    """
    size = int(len(x))

    e = np.arange(size - 1, -1, -1)
    r = np.ones((size,)) * lamb
    vec_lambda = r ** e

    v = np.sum(x * vec_lambda / np.sum(vec_lambda))

    return v


@njit
def future_calculation(input_data: np.ndarray, window: int, how: str):
    """
    在未来窗口上计算统计量

    Args:
        input_data (np.ndarray): 输入数据数组
        window (int): 窗口大小
        how (str): 计算方法，可选 'std', 'max', 'min', 'sum', 'mean'

    Returns:
        np.ndarray: 计算结果数组

    """
    raw_data = input_data.copy()
    result = np.full(raw_data.shape[0], np.nan)

    for i in range(result.shape[0] - window):
        data = raw_data[(i + 1):(i + 1 + window)]
        data = data[np.isfinite(data)]
        if how == 'std':
            result[i] = np.sqrt(((data - np.mean(data)) ** 2).sum() / (len(data) - 1))
        elif how == 'max':
            result[i] = np.max(data)
        elif how == 'min':
            result[i] = np.min(data)
        elif how == 'sum':
            result[i] = np.sum(data)
        elif how == 'mean':
            result[i] = np.mean(data)

    return result


@njit
def calculate_rolling_mean_returns(data: np.ndarray, window: int) -> np.ndarray:
    """
    计算指定窗口大小内回报的滚动平均值。

    Args:
        data (np.ndarray): 输入的一维数组，代表时间序列数据。
        window (int): 窗口大小，用于计算滚动平均值。

    Returns:
        np.ndarray: 输出的一维数组，包含输入数据在指定窗口大小内的滚动平均值。

    """
    size = data.shape[0]
    target = np.full(size, np.nan)

    for i in range(window, size):
        d_ = data[:i+1]
        target[i - window] = np.nanmean(d_[-window:])

    return target


@njit
def log_mean_ret(close: np.ndarray, prev_close: np.ndarray, window: int):
    """
    计算对数收益率的均值。

    Args:
        close (np.ndarray): 收盘价数组。
        prev_close (np.ndarray): 前一日收盘价数组。
        window (int): 窗口大小，表示计算均值时的样本数。

    Returns:
        np.ndarray: 对数收益率的均值数组。

    """
    """Mean of bar log return"""
    size = close.shape[0]
    target = np.full(size, np.nan)

    for i in range(window, size):
        close_ = close[:i+1]
        prev_ = prev_close[:i+1]
        target[i - window] = np.nanmean(np.log(close_[-window:]) - np.log(prev_[-window:]))
    return target


@njit
def log_mean_ret_riskadj(close: np.ndarray, prev_close: np.ndarray, window: int):
    """
    计算风险调整后的对数收益率均值。

    Args:
        close (np.ndarray): 收盘价数组。
        prev_close (np.ndarray): 前一日收盘价数组。
        window (int): 窗口大小，用于计算对数收益率的滑动窗口大小。

    Returns:
        np.ndarray: 风险调整后的对数收益率均值数组。

    """
    size = close.shape[0]
    target = np.full(size, np.nan)

    window_ = max(50, int(window * 2))

    for i in range(window, size):
        close_ = close[:i+1]
        prev_ = prev_close[:i+1]

        if close_.shape[0] < window_ + window:
            continue

        log_ret = np.log(close_[-window_ - window:]) - np.log(prev_[-window_ - window:])
        std = np.nanstd(log_ret[-window_ - window:window])
        log_ret = log_ret[-window:]

        if np.isfinite(std) and std != 0:
            res = np.nanmean(log_ret) / std
            res = np.sign(res) * min(np.abs(res), 3)

            target[i - window] = res

    return target


@jit
def hurst(ts):
    """
    计算Hurst指数，用于分析时间序列数据的长期记忆性。

    Hurst指数（H）是一种统计指标，用于量化时间序列数据的持久性、均值回归或趋势性。
    - H < 0.5: 表示时间序列具有均值回归的特性，即过去的趋势在未来可能会被逆转。
    - H = 0.5: 表示时间序列遵循几何布朗运动，即未来的变化与过去无关。
    - H > 0.5: 表示时间序列具有趋势性，即过去的趋势可能会在未来持续。

    Args:
        ts (np.ndarray): 时间序列数据，一维numpy数组。

    Returns:
        float: Hurst指数。

    """
    # 创建滞后值范围
    max_lag = min(int(len(ts) * 0.9), 100)
    lags = range(2, max_lag)
    # 计算滞后差分的方差数组
    tau = [np.sqrt(np.std(np.subtract(ts[lag:], ts[:-lag]))) for lag in lags]
    # 使用线性拟合来估计Hurst指数
    poly = np.polyfit(np.log(lags), np.log(tau), 1)
    # 返回多项式拟合输出中的Hurst指数
    return poly[0] * 2.0


@njit
def kalman_filter(z: np.array, n_iter=20):
    """
    卡尔曼滤波器函数

    Args:
        z (np.array): 观测值数组
        n_iter (int, optional): 迭代次数，默认为20

    Returns:
        np.array: 状态估计值数组

    """
    """Kalman filter"""
    noise = 1e-6
    x_hat = np.zeros(n_iter)
    x_hat_minus = np.zeros(n_iter)

    p = np.zeros(n_iter)
    p[0] = 1.0
    p_minus = np.zeros(n_iter)
    k = np.zeros(n_iter)

    r = 0.1 ** 2
    a = 1
    h = 1

    for i in range(1, n_iter):
        x_hat_minus[i] = a * x_hat[i - 1]
        p_minus[i] = a * p[i - 1] + noise

        k[i] = p_minus[i] / (p_minus[i] + r)
        x_hat[i] = x_hat_minus[i] + k[i] * (z[i] - h * x_hat_minus[i])
        p[i] = (1 - k[i] * h) * p_minus[i]

    return x_hat


# 阶跃函数
@njit
def step_minus(theta, t, mode):
    """
    根据时间t和模式mode，计算theta的衰减因子。

    Args:
        theta (float): 衰减因子开始衰减的时间点。
        t (float): 当前时间。
        mode (str): 衰减模式，可以是"constant"或"linear"。

    Returns:
        float: 根据当前时间和衰减模式计算得到的衰减因子。

    """
    scale = 1
    if mode == "constant":
        if t <= theta:
            return 1.0 / scale
        else:
            return 0.0
    elif mode == "linear":
        if t <= theta:
            return 1.0 / scale * (theta - t)
        else:
            return 0.0


@njit
def step_plus(theta, t, mode):
    """
    计算根据指定模式对时间步进行增量的函数。

    Args:
        theta (float): 阈值时间。
        t (float): 当前时间步。
        mode (str): 模式类型，'constant' 或 'linear'。

    Returns:
        float: 根据模式计算得到的增量值。

    Raises:
        ValueError: 如果 mode 参数不是 'constant' 或 'linear' 之一时抛出异常。

    """
    scale = 1
    if mode == 'constant':
        if t <= theta:
            return 0.0
        else:
            return -1.0 / scale
    elif mode == 'linear':
        if t <= theta:
            return 0.0
        else:
            return 1.0 / scale * (t - theta)


# 系数矩阵
@njit
def f_theta(theta, n):
    """
    计算给定参数theta和整数n的函数值f_theta。

    Args:
        theta (float): 输入参数theta。
        n (int): 整数n，表示f_theta的输出维度。

    Returns:
        numpy.ndarray: 返回一个形状为(n, 4)的numpy数组，表示函数值f_theta。

    """
    f = np.zeros((n, 4))
    for j in np.arange(n):
        f[j, 0] = step_minus(theta, j, "constant")
        f[j, 1] = step_minus(theta, j, "linear")
        f[j, 2] = step_plus(theta, j, "linear")
        f[j, 3] = step_plus(theta, j, "constant")
    return f


# 残差的方差阵
@jit
def omega_theta(theta, s, n):
    """
    计算矩阵omega_theta。

    Args:
        theta (float): 参数theta的值。
        s (list): 包含两个元素的列表，分别代表s1和s2的值。
        n (int): 矩阵的维度。

    Returns:
        numpy.ndarray: 计算得到的矩阵omega_theta。

    """
    s_ = np.array(s)
    om = np.zeros([n, n])
    for j in np.arange(n):
        step_m = step_minus(theta=theta, t=j, mode="linear")
        step_p = step_plus(theta=theta, t=j, mode="linear")
        om[j, j] = int(np.power(1 + s_[1] * step_m + s_[2] * step_p, 2))
    return om


@jit
def beta_star(y, f_tominv_f, f_tominv):
    """
    使用JIT加速求解beta*。

    Args:
        y (np.ndarray): 观测向量。
        f_tominv_f (np.ndarray): 函数矩阵的逆矩阵与函数矩阵的乘积。
        f_tominv (np.ndarray): 函数矩阵的逆矩阵。

    Returns:
        numpy.ndarray: beta*向量。

    """
    target = np.matmul(f_tominv, y)
    return np.linalg.solve(f_tominv_f, target)


@jit
def residual_uum_sq(y, beta_s, om_inv, f):
    """
    计算似然函数的残差。

    Args:
        y (numpy.ndarray): 观测值数组。
        beta_s (numpy.ndarray): 参数向量。
        om_inv (numpy.ndarray): 协方差矩阵的逆矩阵。
        f (numpy.ndarray): 设计矩阵。

    Returns:
        float: 似然函数的残差。

    """
    f_beta_start = np.matmul(f, beta_s)
    y_to_betastar = np.subtract(y, f_beta_start)
    y_to_betastar_t = np.transpose(y_to_betastar)
    tmp = np.matmul(y_to_betastar_t, om_inv)
    result = np.matmul(tmp, y_to_betastar)
    residual = result[0, 0]

    return residual


@njit
def sigma_hat(rsq, n):
    """
    使用Numba的@njit装饰器加速的函数，计算误差估计值。

    Args:
        rsq (np.ndarray): 输入的平方误差数组。
        n (int): 数据点的数量。

    Returns:
        numpy.ndarray: 误差估计值数组。

    """
    return np.sqrt(rsq / (n + 1))

