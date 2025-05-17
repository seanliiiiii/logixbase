# 随机过程分析模块
"""
本模块包含了多个用于随机过程分析的函数，特别是Ornstein-Uhlenbeck过程相关的计算。
主要功能包括：
1. OU过程的z分数计算
2. CUSUM统计量计算
3. OU过程参数估计
4. 非平稳过程参数估计
5. 时间分布分析
6. 均值回归策略的期望收益计算

这些函数可以帮助进行随机过程的分析和建模，特别适用于金融时间序列的分析和策略开发。
"""

import math
import numpy as np
from scipy import optimize
from scipy.special import erfi
import scipy.stats as st
from statsmodels.tsa.arima.model import ARIMA

from .regression import regress


def ou_process_zscore(data):
    """
    计算Ornstein-Uhlenbeck过程的z分数

    Args:
        data (pandas.DataFrame): 输入数据

    Returns:
        float: z分数

    """
    y = data.iloc[1:].values
    x = data.shift(1).iloc[1:].values

    b, a, resi = regress(y, x)

    theta = 1 - b
    ueq = a / (1 - b)
    voleq = np.sqrt(np.var(resi) / (2 * theta))

    score = (y[-1] - ueq) / voleq

    return score


def cuscore(data):
    """
    计算CUSUM统计量

    Args:
        data (numpy.ndarray): 输入数据

    Returns:
        float: CUSUM统计量

    """
    size = data.shape[0]
    time = np.arange(1, size + 1, 1)

    b, a, resi = regress(data, time)

    s = np.sum((data - b * time) * time)

    return s


def estimate_ou_para(x, dt):
    """
    使用ARIMA模型估计包含Ornstein-Uhlenbeck过程的随机微分方程参数

    使用ARIMA模型对输入数据进行拟合，以估计Ornstein-Uhlenbeck过程的参数。
    假设随机微分方程如下：
    St = Xt + Mt; Xt表示OU过程; Mt表示非平稳过程
    dXt = -alpha * Xt * dt + eta * dWt; dMt = gamma * dt + theta * dZt
    其中，Wt和Zt是独立的高斯过程，Corr(Wt, Zt) = 0。

    Args:
        x (array-like): 输入数据。
        dt (float): 时间步长。

    Returns:
        tuple: 包含三个元素的元组，依次为alpha、eta和d_w。

    """
    # 用ARIMA拟合X来获取alpha, eta
    mdl_arma_x = ARIMA(x, order=(1, 0, 0))
    with mdl_arma_x.fix_params({'const': 0}):
        mdl_arma_x_fit = mdl_arma_x.fit()

    alpha = (1 - mdl_arma_x_fit.params[1]) / dt
    residual = mdl_arma_x_fit.resid
    eta = np.std(residual)
    d_w = residual / eta

    return alpha, eta, d_w


def estimate_ns_para(m, dw, dt, init_para: list = None):
    """
    估计与OU过程随机微分方程对齐的非平稳随机微分方程参数

    Args:
        m (numpy.ndarray): 输入数据。
        dw (numpy.ndarray): OU过程的白噪声。
        dt (float): 时间步长。
        init_para (list, optional): 初始参数列表，默认为None。若未提供，则使用[0.001, 0.1]作为初始参数。

    Returns:
        tuple: 包含gamma和theta的元组。

    """
    # 用最大似然估计拟合gamma和theta
    if init_para is None:
        init_para = [0.001, 0.1]

    def helper(param):
        """辅助函数"""
        return calc_loglik(param, m, dw, dt)

    def calc_loglik(param, x, d_w, d_t):
        """计算对数似然"""
        # 拟合非平稳随机微分方程的白噪声: dMt = gamma * dt + theta * dZt
        d_z = fit(param, x, d_t)
        # 计算似然变量
        d_w_ks = st.kstest(d_w, 'norm')[0]
        d_z_ks = st.kstest(d_z, 'norm')[0]
        # 计算最大对数似然指标
        mloglik = d_w_ks * d_z_ks

        return mloglik

    def fit(param, s, dts):
        """拟合非平稳随机微分方程的白噪声: dMt = gamma * dt + theta * dZt"""
        (gam, the) = param
        t = len(s)
        d_z = np.full((t,), np.nan)

        d_m = np.zeros((t,))
        d_m[1:] = s[1:] - s[:-1]

        for i in range(t):
            d_z[i] = (d_m[i] - gam * dts) / the

        return d_z

    gamma, theta = optimize.fmin(helper, init_para)

    return gamma, theta


def time_distribution(zscores, thres, exit_thres):
    """
    计算时间分布矩阵。

    Args:
        zscores (np.ndarray): z分数数组。
        thres (int, float): 进入阈值。
        exit_thres (int, float): 退出阈值。

    Returns:
        np.ndarray: 时间分布矩阵。

    """
    if not isinstance(zscores, np.ndarray) or zscores.size == 0:
        return np.zeros((0, 4))

    if not (isinstance(thres, (int, float)) and isinstance(exit_thres, (int, float))):
        raise ValueError("Thresholds must be numeric")

    time_dist = np.zeros((zscores.shape[0], 4))
    cols = {1: 0, -1: 1, 0: 2}
    trigger = 0
    status = 0
    round_start = 0
    count = 1

    for i, zscore in enumerate(zscores):
        col = cols[status]

        if status == 0:
            change_status = (abs(zscore) >= thres)
        else:
            change_status = (abs(zscore) <= exit_thres) or (status * zscore >= thres)

        if change_status:
            time_dist[trigger, col] += count
            status = (abs(zscore) >= thres) * -np.sign(zscore)
            count = 1

            if status == round_start and trigger < time_dist.shape[0] - 1:
                trigger += 1
                round_start = status
        else:
            count += 1

        if i + 1 == zscores.shape[0]:
            time_dist[trigger, col] = count

    time_dist[:, 3] = np.sum(time_dist[:, :3], axis=1)
    return time_dist[:trigger+1] if trigger < time_dist.shape[0] else time_dist


def calc_exp_num(b1, b2, b3, c, gamma, theta, tita):
    """
    计算OU过程均值回归策略的期望收益和波动率

    Args:
        b1 (float): 参数b1
        b2 (float): 参数b2
        b3 (float): 参数b3
        c (float): 参数c
        gamma (float): gamma参数
        theta (float): theta参数
        tita (numpy.ndarray): 时间分布矩阵

    Returns:
        tuple: 期望收益u和波动率v的元组

    """
    e_tita = np.mean(1 / tita[:, 3])
    e_tita_diff = np.mean(gamma * (tita[:, 0] - tita[:, 1]) / tita[:, 3])

    u = (b2 - b1 - 2 * b3 - 2 * c) * e_tita + e_tita_diff

    v_tita = (theta ** 2) * e_tita
    v_tita_diff = ((b2 - b1 - 2 * b3 - 2 * c) + gamma * (tita[:, 0] - tita[:, 1])) / tita[:, 3]
    vol = ((v_tita_diff - np.mean(v_tita_diff)) ** 2).sum() / (len(v_tita_diff) - 1) + v_tita

    return u, vol


def calc_exp_analytic(b1, b2, b3, c, params, tita):
    """
    计算OU过程均值回归策略的解析期望收益

    Args:
        b1 (float): 参数b1
        b2 (float): 参数b2
        b3 (float): 参数b3
        c (float): 参数c
        params (list): [alpha, eta, gamma, theta]参数列表
        tita (numpy.ndarray): 时间分布矩阵

    Returns:
        tuple: 期望收益u和波动率v

    计算公式:
        u(b1,b2,b3,c) = (b2-b1-2b3-2c)*alpha/(pi*(erfi(b2*sqrt(alpha)/eta)-erfi(b1*sqrt(alpha)/eta))) +
                        E(gamma*(tita1-tita2) / tita)
        v(b1,b2,b3,c) = V[((b2-b1-2b3-2c)+gamma*(tita1-tita2))/tita] + alpha*theta**2/(pi*(erfi(b2*sqrt(alpha)/eta)
                        -erfi(b1*sqrt(alpha)/eta)))
    """
    alpha, eta, gamma, theta = params

    stationary_u_error = math.pi * (erfi(b2 * np.sqrt(alpha) / eta) - erfi(b1 * np.sqrt(alpha) / eta))
    u_1 = alpha * (b2 - b1 - 2 * b3 - 2 * c) / stationary_u_error
    e_tita_diff = np.mean(gamma * (tita[:, 0] - tita[:, 1]) / tita[:, 3])

    u = u_1 + e_tita_diff

    v_tita_diff = ((b2 - b1 - 2 * b3 - 2 * c) + gamma * (tita[:, 0] - tita[:, 1])) / tita[:, 3]
    noise_v = alpha * (theta ** 2) / stationary_u_error

    v = ((v_tita_diff - np.mean(v_tita_diff)) ** 2).sum() / (len(v_tita_diff) - 1) + noise_v

    return u, v


def calc_exp_num_st(xt, b3, c, gamma, theta, tita, tita_col):
    """
    计算OU过程均值回归策略的数值期望收益

    使用给定的参数计算OU过程均值回归策略的数值期望收益和波动率。

    Args:
    xt (numpy.ndarray): Xt过程。
    b3 (float): 参数b3。
    c (float): 参数c。
    gamma (float): gamma参数。
    theta (float): theta参数。
    tita (numpy.ndarray): 时间分布矩阵。
    tita_col (int): 时间列索引。

    Returns:
    tuple: 返回一个包含期望收益u和波动率v的元组。
    """
    e_tita = np.mean(1 / tita[:, 3])
    e_tita_gamma = np.mean(gamma * tita[:, tita_col] / tita[:, 3])

    u = (xt - b3 - c) * e_tita + e_tita_gamma

    v_tita = (theta ** 2) * e_tita
    v_tita_diff = (xt - b3 - c + gamma * tita[:, tita_col]) / tita[:, 3]
    vol = ((v_tita_diff - np.mean(v_tita_diff)) ** 2).sum() / (len(v_tita_diff) - 1) + v_tita

    return u, vol


def calc_exp_analytic_st(xt, b1, b2, b3, c, params, tita, tita_col):
    """
    计算OU过程均值回归策略的预期收益。

    Args:
        xt (float): 时间序列中的当前值。
        b1 (float): 均值回归参数。
        b2 (float): 均值回归参数。
        b3 (float): 均值回归参数。
        c (float): 交易成本参数。
        params (tuple): 包含alpha, eta, gamma, theta的元组。
            - alpha (float): OU过程的均值回归速率。
            - eta (float): OU过程的波动率。
            - gamma (float): 杠杆参数。
            - theta (float): OU过程的长期均值。
        tita (numpy.ndarray): 二维数组，包含交易数据。
        tita_col (int): tita数组中用于计算的列索引。

    Returns:
        tuple: 包含两个元素的元组，分别表示预期收益u和波动率v。
            - u (float): 预期收益。
            - v (float): 波动率。

    """
    alpha, eta, gamma, theta = params

    stationary_u_error = math.pi * (erfi(b2 * np.sqrt(alpha) / eta) - erfi(b1 * np.sqrt(alpha) / eta))
    u_1 = alpha * (abs(abs(xt) - abs(b3)) - c) / stationary_u_error
    e_tita_gamma = np.mean(gamma * (tita[:, tita_col]) / tita[:, 3])

    u = u_1 + e_tita_gamma

    v_tita_gamma = ((abs(abs(xt) - abs(b3)) - c) + gamma * (tita[:, tita_col])) / tita[:, 3]
    noise_v = (theta ** 2) * np.mean(tita[:, tita_col] / (tita[:, 3] ** 2))

    v = ((v_tita_gamma - np.mean(v_tita_gamma)) ** 2).sum() / (len(v_tita_gamma) - 1) + noise_v

    return u, v

