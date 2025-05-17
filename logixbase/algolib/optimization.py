# 投资组合优化模块
"""
本模块包含了多个用于投资组合优化的函数。
主要功能包括：
1. 最小方差投资组合优化
2. 最大夏普比率投资组合优化
3. 风险平价投资组合优化
4. 等权重投资组合
5. 等风险贡献投资组合
6. 最大分散化投资组合优化
7. 均值-条件风险价值(CVaR)投资组合优化
8. Black-Litterman投资组合优化
9. 层次风险平价投资组合优化

这些函数可以帮助进行各种投资组合的构建和优化，适用于不同的投资策略和风险偏好。
"""

import numpy as np
from scipy import optimize
import warnings

warnings.simplefilter("ignore")


def min_variance(returns, constraint=None, weight_bounds=(0, 1)):
    """
    最小方差投资组合优化

    Args:
        returns (DataFrame): 收益率矩阵
        constraint (dict, optional): 约束条件，默认为None。如果提供，应为包含约束条件的字典。
        weight_bounds (tuple, optional): 权重边界，默认为(0, 1)。表示权重可以在0到1之间。

    Returns:
        numpy.ndarray: 最优权重数组

    """
    n = returns.shape[1]
    returns = returns.dropna()

    # 计算协方差矩阵
    cov_mat = returns.cov().values

    # 定义目标函数
    def objective(weights):
        portfolio_variance = np.dot(weights.T, np.dot(cov_mat, weights))
        return portfolio_variance

    # 初始权重
    initial_weights = np.array([1.0 / n] * n)

    # 权重约束
    bounds = tuple(weight_bounds for _ in range(n))

    # 权重和为1的约束
    constraints = [{'type': 'eq', 'fun': lambda x: np.sum(x) - 1.0}]
    if constraint is not None:
        constraints.append(constraint)

    # 优化
    result = optimize.minimize(
        objective,
        initial_weights,
        method='SLSQP',
        bounds=bounds,
        constraints=constraints
    )

    return result['x']


def max_sharpe(returns, risk_free_rate=0, constraint=None, weight_bounds=(0, 1)):
    """
    最大夏普比率投资组合优化

    Args:
        returns (pandas.DataFrame): 收益率矩阵，每列代表一个资产的收益率序列。
        risk_free_rate (float, optional): 无风险利率，默认为0。
        constraint (dict, optional): 约束条件，默认为None。如果提供了约束条件，会添加到优化过程中。
        weight_bounds (tuple, optional): 权重边界，默认为(0, 1)，表示权重在0到1之间。

    Returns:
        numpy.ndarray: 最优权重数组，表示投资组合中各资产的权重。

    """
    n = returns.shape[1]
    returns = returns.dropna()

    # 计算均值和协方差
    mean_returns = returns.mean().values
    cov_mat = returns.cov().values

    # 定义目标函数（负夏普比率，因为我们要最小化）
    def objective(weights):
        portfolio_return = np.sum(mean_returns * weights)
        portfolio_std_dev = np.sqrt(np.dot(weights.T, np.dot(cov_mat, weights)))
        sharpe_ratio = (portfolio_return - risk_free_rate) / portfolio_std_dev
        return -sharpe_ratio

    # 初始权重
    initial_weights = np.array([1.0 / n] * n)

    # 权重约束
    bounds = tuple(weight_bounds for _ in range(n))

    # 权重和为1的约束
    constraints = [{'type': 'eq', 'fun': lambda x: np.sum(x) - 1.0}]
    if constraint is not None:
        constraints.append(constraint)

    # 优化
    result = optimize.minimize(
        objective,
        initial_weights,
        method='SLSQP',
        bounds=bounds,
        constraints=constraints
    )

    return result['x']


def risk_parity(returns, risk_target=None, constraint=None, weight_bounds=(0, 1)):
    """
    风险平价投资组合优化

    Args:
        returns (pd.DataFrame): 收益率矩阵，每一列代表一个资产的收益率序列。
        risk_target (float, optional): 目标风险，如果提供，则最终优化出的权重对应的风险会调整到该目标值。默认为None。
        constraint (dict, optional): 额外的约束条件，格式为Scipy的minimize函数接受的约束条件。默认为None。
        weight_bounds (tuple, optional): 权重的边界，格式为(min_weight, max_weight)。默认为(0, 1)。

    Returns:
        np.ndarray: 最优权重数组，形状为(n,)，其中n为资产的数量。

    """
    n = returns.shape[1]
    returns = returns.dropna()

    # 计算协方差矩阵
    cov_mat = returns.cov().values

    # 定义目标函数
    def objective(weights):
        # 计算每个资产的风险贡献
        portfolio_variance = np.dot(weights.T, np.dot(cov_mat, weights))
        portfolio_std_dev = np.sqrt(portfolio_variance)
        marginal_risk = np.dot(cov_mat, weights) / portfolio_std_dev
        risk_contribution = weights * marginal_risk

        # 计算风险贡献的方差
        target_risk_contribution = portfolio_std_dev / n
        risk_diff = risk_contribution - target_risk_contribution
        return np.sum(risk_diff ** 2)

    # 初始权重
    initial_weights = np.array([1.0 / n] * n)

    # 权重约束
    bounds = tuple(weight_bounds for _ in range(n))

    # 权重和为1的约束
    constraints = [{'type': 'eq', 'fun': lambda x: np.sum(x) - 1.0}]
    if constraint is not None:
        constraints.append(constraint)

    # 优化
    result = optimize.minimize(
        objective,
        initial_weights,
        method='SLSQP',
        bounds=bounds,
        constraints=constraints
    )

    # 如果有目标风险，进行调整
    weights = result['x']
    if risk_target is not None:
        portfolio_std_dev = np.sqrt(np.dot(weights.T, np.dot(cov_mat, weights)))
        weights = weights * (risk_target / portfolio_std_dev)

    return weights


def equal_weight(returns):
    """
    等权重投资组合

    Args:
        returns (numpy.ndarray): 收益率矩阵，其中每一列代表一个资产的收益率序列。

    Returns:
        numpy.ndarray: 等权重投资组合的权重数组，每个资产的权重相同，为 1/n，其中 n 为资产数量。

    """
    n = returns.shape[1]
    return np.array([1.0 / n] * n)


def equal_risk_contribution(returns, risk_target=None):
    """
    等风险贡献投资组合函数。

    Args:
        returns (numpy.ndarray): 收益率矩阵，形状为 (n_assets, n_periods)，其中 n_assets 是资产数量，n_periods 是时间周期数。
        risk_target (float, optional): 目标风险水平。默认为 None，表示不设置目标风险。

    Returns:
        numpy.ndarray: 最优权重向量，长度为 n_assets，表示每个资产的最优配置权重。

    """
    return risk_parity(returns, risk_target)


def max_diversification(returns, constraint=None, weight_bounds=(0, 1)):
    """
    最大分散化投资组合优化

    Args:
        returns (DataFrame): 收益率矩阵
        constraint (function, optional): 自定义约束条件，默认为None。
        weight_bounds (tuple, optional): 权重边界，默认为(0, 1)。

    Returns:
        ndarray: 最优权重

    """
    n = returns.shape[1]
    returns = returns.dropna()

    # 计算协方差矩阵和波动率
    cov_mat = returns.cov().values
    vol = np.sqrt(np.diag(cov_mat))

    # 定义目标函数（负分散化比率，因为我们要最小化）
    def objective(weights):
        portfolio_vol = np.sqrt(np.dot(weights.T, np.dot(cov_mat, weights)))
        weighted_vol = np.sum(weights * vol)
        diversification_ratio = weighted_vol / portfolio_vol
        return -diversification_ratio

    # 初始权重
    initial_weights = np.array([1.0 / n] * n)

    # 权重约束
    bounds = tuple(weight_bounds for _ in range(n))

    # 权重和为1的约束
    constraints = [{'type': 'eq', 'fun': lambda x: np.sum(x) - 1.0}]
    if constraint is not None:
        constraints.append(constraint)

    # 优化
    result = optimize.minimize(
        objective,
        initial_weights,
        method='SLSQP',
        bounds=bounds,
        constraints=constraints
    )

    return result['x']


def mean_cvar(returns, alpha=0.05, constraint=None, weight_bounds=(0, 1)):
    """
    均值-条件风险价值(CVaR)投资组合优化

    Args:
        returns (pandas.DataFrame): 收益率矩阵，每列代表一个资产的收益率序列。
        alpha (float, optional): 显著性水平，用于计算条件风险价值(CVaR)。默认为0.05。
        constraint (dict, optional): 优化问题的额外约束条件。默认为None。
        weight_bounds (tuple, optional): 权重的上下界。默认为(0, 1)，表示权重必须在0到1之间。

    Returns:
        numpy.ndarray: 最优权重向量。

    """
    n = returns.shape[1]
    returns = returns.dropna()
    T = returns.shape[0]

    # 定义目标函数
    def objective(weights):
        portfolio_returns = np.dot(returns, weights)
        sorted_returns = np.sort(portfolio_returns)
        var_index = int(alpha * T)
        cvar = -np.mean(sorted_returns[:var_index])
        return cvar

    # 初始权重
    initial_weights = np.array([1.0 / n] * n)

    # 权重约束
    bounds = tuple(weight_bounds for _ in range(n))

    # 权重和为1的约束
    constraints = [{'type': 'eq', 'fun': lambda x: np.sum(x) - 1.0}]
    if constraint is not None:
        constraints.append(constraint)

    # 优化
    result = optimize.minimize(
        objective,
        initial_weights,
        method='SLSQP',
        bounds=bounds,
        constraints=constraints
    )

    return result['x']


def black_litterman(returns, market_caps, views=None, view_confidences=None, tau=0.05, risk_aversion=2.5):
    """
    Black-Litterman投资组合优化算法。

    Args:
        returns (pandas.DataFrame): 收益率矩阵，每一列代表一个资产的历史收益率。
        market_caps (pandas.Series): 资产的市场资本权重，索引应与returns的列名对应。
        views (dict, optional): 投资者的观点，键为观点名称，值为包含资产和对应权重的字典。默认值为None。
        view_confidences (list, optional): 观点的置信度，应与views中的观点一一对应。默认值为None。
        tau (float, optional): 不确定性参数，用于调整市场隐含收益率和投资者观点之间的权重。默认值为0.05。
        risk_aversion (float, optional): 风险厌恶系数，用于调整投资组合的风险偏好。默认值为2.5。

    Returns:
        numpy.ndarray: 最优权重数组，表示每个资产在投资组合中的权重。

    """
    n = returns.shape[1]
    returns = returns.dropna()

    # 计算协方差矩阵
    cov_mat = returns.cov().values

    # 计算市场隐含收益率
    market_weights = market_caps / np.sum(market_caps)
    pi = risk_aversion * np.dot(cov_mat, market_weights)

    # 如果没有观点，直接返回市场权重
    if views is None:
        return market_weights

    # 处理观点
    k = len(views)
    P = np.zeros((k, n))
    q = np.zeros(k)

    for i, (view, assets) in enumerate(views.items()):
        for asset, weight in assets.items():
            asset_idx = returns.columns.get_loc(asset)
            P[i, asset_idx] = weight
        q[i] = view

    # 观点不确定性矩阵
    omega = np.diag([1 / conf for conf in view_confidences]) if view_confidences else np.eye(k)

    # 计算后验收益率
    tau_cov = tau * cov_mat
    inv_tau_cov = np.linalg.inv(tau_cov)
    inv_omega = np.linalg.inv(omega)

    post_cov = np.linalg.inv(inv_tau_cov + np.dot(P.T, np.dot(inv_omega, P)))
    post_ret = np.dot(post_cov, np.dot(inv_tau_cov, pi) + np.dot(P.T, np.dot(inv_omega, q)))

    # 使用后验收益率和协方差进行均值方差优化
    def objective(weights):
        portfolio_return = np.sum(post_ret * weights)
        portfolio_variance = np.dot(weights.T, np.dot(cov_mat, weights))
        utility = portfolio_return - 0.5 * risk_aversion * portfolio_variance
        return -utility

    # 初始权重
    initial_weights = market_weights.copy()

    # 权重约束
    bounds = tuple((0, 1) for _ in range(n))

    # 权重和为1的约束
    constraints = [{'type': 'eq', 'fun': lambda x: np.sum(x) - 1.0}]

    # 优化
    result = optimize.minimize(
        objective,
        initial_weights,
        method='SLSQP',
        bounds=bounds,
        constraints=constraints
    )

    return result['x']


def hierarchical_risk_parity(returns):
    """
    层次风险平价投资组合优化算法。

    Args:
        returns (pandas.DataFrame): 收益率矩阵，其中每一列代表一个资产的收益率序列。

    Returns:
        numpy.ndarray: 最优权重向量，表示每个资产在投资组合中的权重。

    """
    n = returns.shape[1]
    returns = returns.dropna()

    # 计算协方差矩阵和相关性矩阵
    cov_mat = returns.cov().values
    corr_mat = returns.corr().values

    # 计算距离矩阵
    dist = np.sqrt(0.5 * (1 - corr_mat))

    # 层次聚类
    from scipy.cluster import hierarchy
    link = hierarchy.linkage(dist, 'single')

    # 获取聚类顺序
    order = hierarchy.leaves_list(hierarchy.optimal_leaf_ordering(link, dist))

    # 计算方差
    var = np.diag(cov_mat)

    # 初始化权重
    weights = np.ones(n) / n

    # 递归二分求解
    def bisect(cluster, weights):
        if len(cluster) == 1:
            return

        # 二分聚类
        mid = len(cluster) // 2
        left_cluster = cluster[:mid]
        right_cluster = cluster[mid:]

        # 计算子聚类的方差
        left_var = np.sum(var[left_cluster])
        right_var = np.sum(var[right_cluster])

        # 按照方差的倒数分配权重
        left_weight = 1 / left_var / (1 / left_var + 1 / right_var)
        right_weight = 1 - left_weight

        # 更新权重
        weights[left_cluster] *= left_weight
        weights[right_cluster] *= right_weight

        # 递归处理子聚类
        bisect(left_cluster, weights)
        bisect(right_cluster, weights)

    # 开始递归
    bisect(order, weights)

    return weights

