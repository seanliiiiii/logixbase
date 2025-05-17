# 回归分析模块
"""
本模块包含了多个用于回归分析的函数。
主要功能包括：
1. 普通最小二乘法回归（OLS）
2. 无截距项的线性回归
3. t统计量计算
4. R方计算
5. F统计量和p值计算
6. 均方误差计算
这些函数可以帮助进行基本的线性回归分析，计算回归系数，并评估模型的拟合优度。
"""
import numpy as np
import scipy.stats as st
import statsmodels.api as sm

from numba import njit, jit
from math import sqrt


@njit
def regress(y: np.array, x: np.array):
    """
    对y和x进行线性回归，返回斜率、截距和残差。

    Args:
        y (np.array): 因变量数组。
        x (np.array): 自变量数组。

    Returns:
        tuple: 包含斜率(b1)，截距(b0)和残差(r)的元组。

    """
    mask = np.isfinite(x) & np.isfinite(y)
    x_ = x[mask]
    y_ = y[mask]

    sum_xy = 0
    sum_xx = 0
    sum_yy = 0

    x_mean = np.mean(x_)
    y_mean = np.mean(y_)

    for i in range(x_.shape[0]):
        sum_xy += (x_[i] - x_mean) * (y_[i] - y_mean)
        sum_xx += pow(x_[i] - x_mean, 2)
        sum_yy += pow(y_[i] - y_mean, 2)

    if sum_xx != 0:
        b1 = sum_xy / sum_xx
    else:
        b1 = 0
    b0 = y_mean - b1 * x_mean

    r = y - (b1 * x + b0)

    return b1, b0, r


@njit
def regress_nointercept(y, x):
    """
    对y和x进行无截距的线性回归

    Args:
        y (np.ndarray): 因变量数组
        x (np.ndarray): 自变量数组

    Returns:
        tuple: 包含两个元素，分别是斜率(b)和残差(r)

            - b (float): 斜率
            - r (numpy.ndarray): 残差数组

    """
    sum_xx = np.sum(x ** 2)
    sum_xy = np.sum(y * x)

    if sum_xx != 0:
        b = sum_xy / sum_xx
    else:
        b = 0
    r = y - b * x
    return b, r


@njit
def regress_full(y, x):
    """
    线性回归函数，用于计算回归系数和截距，并返回相应的统计量。

    Args:
        x (np.ndarray): 自变量数组。
        y (np.ndarray): 因变量数组。

    Returns:
        tuple: 包含回归系数a、a的t值、截距b、b的t值。

    """
    n = int(len(x))
    mean_x = np.mean(x)
    mean_y = np.mean(y)

    # 计算回归系数 a 和截距 b
    s_xy = np.sum((x - mean_x) * (y - mean_y))
    s_xx = np.sum((x - mean_x) ** 2)
    a = s_xy / s_xx
    b = mean_y - a * mean_x

    # 计算预测值与残差
    y_hat = a * x + b
    residuals = y - y_hat
    sse = np.sum(residuals ** 2)  # 平方和
    mse = sse / (n - 2)           # 均方误差（自由度为 n-2）

    # 标准误差计算
    se_a = sqrt(mse / s_xx)
    se_b = sqrt(mse * (1.0 / n + mean_x**2 / s_xx))

    # t 值
    t_a = a / se_a
    t_b = b / se_b

    return (a, t_a), (b, t_b)


@njit
def r_square(x, y):
    """
    计算x对y线性回归的R方

    Args:
        y (np.ndarray): 因变量数组
        x (np.ndarray): 自变量数组

    Returns:
        float: R方值

    """
    x_mean = np.mean(x)
    y_mean = np.mean(y)

    ssr = 0
    x_var = 0
    y_var = 0

    for i in range(x.shape[0]):
        err_x = x[i] - x_mean
        err_y = y[i] - y_mean

        ssr += err_x * err_y
        x_var += err_x ** 2
        y_var += err_y ** 2

    sst = np.sqrt(x_var * y_var)

    if sst == 0:
        return 0
    else:
        return (ssr / sst) ** 2


@njit
def f_stat(y: np.array, y_: np.array, n_freedom: int):
    """
    计算模型的F统计量

    Args:
        y (np.array): 实际值数组
        y_ (np.array): 预测值数组
        n_freedom (int): 自由度

    Returns:
        float: F统计量

    """
    n = y.shape[0] - n_freedom - 1
    u = np.nanmean(y)

    ssr = np.nansum((y_ - u) ** 2)
    sse = np.nansum((y_ - y) ** 2)

    if (n_freedom == 0) or (sse == 0):
        return np.nan
    else:
        return (ssr / n_freedom) / (sse / n)


@jit
def f_pvalue(y: np.array, y_: np.array, n_freedom: int):
    """
    @jit
    计算F统计量的p值

    Args:
        y (np.array): 实际值数组
        y_ (np.array): 预测值数组
        n_freedom (int): 自由度

    Returns:
        float: p值

    """
    n = y.shape[0] - n_freedom - 1
    f_value = f_stat(y, y_, n_freedom)
    p = 1 - st.f.cdf(f_value, n_freedom, n)
    return p


@njit
def mse(x, y):
    """
    计算x和y之间的均方误差

    Args:
        x (numpy.ndarray): 数组x
        y (numpy.ndarray): 数组y

    Returns:
        float: 均方误差

    Note:
        使用numba库的@njit装饰器进行JIT编译，以提高计算效率。
    """
    if not y.shape[0]:
        return np.nan
    else:
        return np.sum((y - x) * (y - x)) / y.shape[0]


def ivxlh(yt: np.array, xt: np.array, k: int, print_res: bool = False):
    """
    Based on the MATLAB Code provided by Kostakis(2015)
    Modification:
    1. Adjust OLS estimation in accordance to K
    2. Add IVX estimation of intercept for prediction purpose

    Kostakis, A., Magdalinos, T., & Stamatogiannis, M. P. (2015), Robust econometric inference for stock return
        predictability, The Review of Financial Studies, 28(5), 1506-1553.
    The function estimates the predictive regression: y{t}=mu+A*x{t-1}+e{t} (1),
        and the VAR model: x{t}=R*x{t-1}+u{t}, with R being a diagonal matrix.
    :param yt: Array, T*1 vector which is the precticted variable y{t}
    :param xt: Array, T*k reg matrix including the regressors as columns.
                - The program automatically includes an intercept in the predictive regression,
                    so there is no need for a column of 1's.
                - There is no need to input the lagged series; the program lags the series.
    :param k: Int, horizon (special case K=1 corresponds to a short-horizon regression)
    :param print_res: Bool to control result print
    :return:
        - Aols: Array, (kreg+1)*1 vector which is the OLS estimator of the intercept mu
                 (1st element) and the slope coefficients (A)
        - Aivx: Array, 1*kreg vector which is the IVX estimator of A in (1)
        - Wivx: Array, 2*1 vector with the first element being the Wald statistic of a test for overall sifnificance,
                whereas the second gives the corresponding p-value (from the chi-square distribution)
        - WivxInd: Array, 2*kreg matrix the first row gives the individual test of siginificance for each
                    predictor (i.e. restricting each predictor's coefficient to be equal to 0 letting the rest
                     coefficients free to be estimated),
                    whereas the second row gives the corresponding p-values (for a two-sided test).
        - Q: Array, the variance-covariance matrix of the IVX estimator for A.
        - res_corrmat: Array, the correlation between the residuals of the predictive regression (e{t})
                        and the residuals of the autoregressive part (u{t})
    """
    # Adjust variable shape
    if len(xt.shape) == 1:
        xt = xt.reshape((len(xt), 1))
    m = xt.shape[0]
    xt_mean = np.mean(xt[:(m - k + 1), :], axis=0)

    xlag = xt[:-1, :]
    xt = xt[1:, :]
    y = yt[1:]

    nn, m = xlag.shape
    x_ = np.hstack((np.ones((nn, 1)), xlag))

    wivx = np.zeros(2)
    wivx_ind = np.zeros((2, m))

    # Predictive regression residual estimation
    md = sm.OLS(y, x_)
    res = md.fit()
    epshat = res.resid

    rn = np.zeros((m, m))
    for i in range(m):
        md_1 = sm.OLS(xt[:, i], xlag[:, i])
        res_1 = md_1.fit()
        rn[i, i] = res_1.params

    # autoregressive residual estimation
    u = xt - xlag.dot(rn)

    # residuals correlation matrix
    res_corrmat = np.corrcoef(np.hstack((epshat.reshape((len(epshat), 1)), u)).T)

    # covariance matrix estimation(predictive regression)
    covepshat = epshat.dot(epshat) / nn
    covu = np.zeros((m, m))
    if len(u.shape) == 1:
        u = u.reshape((len(u), 1))
    for t in range(nn):
        covu = covu + u[t:(t + 1), :].T.dot(u[t:(t + 1), :])

    # covariance matrix estimation (autoregression)
    covu = covu / nn
    covuhat = np.zeros((1, m))
    for j in range(m):
        covuhat[0, j] = np.sum(epshat * u[:, j])

    # covariance matrix between 'epshat' and 'u'
    covuhat = covuhat.T / nn

    m = int(np.floor(np.power(nn, 1 / 3)))
    uu = np.zeros((m, m))
    for h in range(m):
        a = np.zeros((m, m))
        for s in range(nn - h - 1):
            t = s + h + 1
            a = a + u[t:t + 1, :].T.dot(u[t - h - 1:t - h, :])
        uu = uu + (1 - (h + 1) / (1 + m)) * a

    uu = uu / nn
    omegauu = covu + uu + uu.T

    q = np.zeros((m, m))
    for h in range(m):
        p = np.zeros((nn - h, m))
        for t in range(nn - h - 1):
            t = t + h + 1
            p[t - h, :] = u[t, :] * epshat[t - h - 1]
        q[h, :] = (1 - (h + 1) / (1 + m)) * p.sum(axis=0)

    residue = q.sum(axis=0) / nn
    omegaeu = covuhat + residue.reshape((len(residue), 1))

    # instrument construction
    n = nn - k + 1
    rz = (1 - 1 / np.power(nn, 0.95)) * np.eye(m)
    diffx = xt - xlag
    z = np.zeros((nn, m))
    z[0, :] = diffx[0, :]
    for i in range(nn - 1):
        i = i + 1
        z[i, :] = z[i - 1:i, :].dot(rz) + diffx[i, :]

    z = np.vstack((np.zeros((1, m)), z[0:n - 1, :]))

    zz = np.vstack((np.zeros((1, m)), z[0:nn - 1, :]))

    zk = np.zeros((n, m))
    for i in range(n):
        zk[i, :] = zz[i:i + k, :].sum(axis=0)

    yy = np.zeros((n, 1))
    for i in range(n):
        yy[i] = y[i:i + k].sum()

    if k == 1:
        md = sm.OLS(yy, x_)
    else:
        md = sm.OLS(yy, x_[0:-k + 1, :])
    res = md.fit()

    aols = res.params
    apval = res.pvalues

    x_k = np.zeros((n, m))
    for i in range(n):
        x_k[i, :] = xlag[i:i + k, :].sum(axis=0)

    meanx_k = x_k.mean(axis=0)
    y_t = yy - yy.mean()
    x_t = np.zeros((n, m))
    for j in range(m):
        x_t[:, j] = x_k[:, j] - meanx_k[j] * np.ones(n)

    aivx = np.matmul(y_t.T.dot(z), np.linalg.pinv(x_t.T.dot(z)))
    meanz_k = zk.mean(axis=0)
    meanz_k = meanz_k.reshape((1, len(meanz_k)))

    f_m = covepshat - omegaeu.T.dot(np.linalg.inv(omegauu)).dot(omegaeu)
    m_ = zk.T.dot(zk) * covepshat - n * meanz_k.T.dot(meanz_k) * f_m

    h_ = np.eye(m)
    aa = h_.dot(np.linalg.pinv(z.T.dot(x_t))).dot(m_)
    bb = np.linalg.pinv(x_t.T.dot(z)).dot(h_.T)
    q_ = aa.dot(bb)

    wivx[0] = h_.dot(aivx.T).T.dot(np.linalg.pinv(q_)).dot(h_).dot(aivx.T)
    wivx[1] = 1 - st.chi2.cdf(wivx[0], m)

    wivx_ind[0, :] = aivx / np.sqrt(np.diag(q_)).T
    wivx_ind[1, :] = 1 - st.chi2.cdf(np.power(wivx_ind[0, :], 2), 1)

    # IVX estimator of Intercept
    mu_ivx = yy.mean() - aivx.dot(xt_mean)

    if print_res == 1:
        print('Horizon: ')
        print(k)
        print('       ')

        print('IVX estimator of mu: ')
        print(mu_ivx)
        print('     ')

        print('IVX estimator of A: ')
        print(aivx)
        print('       ')

        print('Individual Tests of Significance:(pValues)')
        print(wivx_ind[1, :])
        print('     ')

        print('Test of Overall Model Significance:(pValues)')
        print(wivx[1])
        print('     ')

        print('OLS estimator of coefficient')
        print(aols)
        print('     ')

        print('Standard t-test of OLS estimator Significannce(pValues):')
        print(apval)
        print('     ')

        print('Residuals Correlation Matrix')
        print(res_corrmat)
        print('       ')

    return mu_ivx, aivx, wivx_ind, wivx, res_corrmat, aols, apval

