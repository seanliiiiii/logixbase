"""
AlgoLib - 算法交易和金融分析的综合库

这个包提供了一系列用于金融分析、算法交易和数据处理的工具：

1. 统计分析 (basestat):
   - 相关性分析
   - 描述性统计
   - 异常值检测

2. 数据可视化 (chart):
   - 各种金融图表的绘制

3. 金融计算 (fin):
   - 信息增量分析

4. 矩阵操作 (matops):
   - 矩阵运算和数组操作

5. 投资组合优化 (optimization):
   - 最小方差组合
   - 最大夏普比率组合
   - 风险平价组合等

6. 回归分析 (regression):
   - 线性回归
   - 回归诊断

7. 统计检验 (stattest):
   - 正态性检验
   - 单位根检验

8. 随机过程分析 (stochastic):
   - Ornstein-Uhlenbeck过程
   - 随机微分方程参数估计

9. 时间序列分析 (timeseries):
   - 移动平均
   - 指数平滑
   - Hurst指数计算
   - Kalman滤波

10. 通用工具 (utils):
    - 累积和计算
    - 正态分布PDF计算

使用示例：
from odin.algolib import corr, ChartWizard, min_variance
result = corr(data1, data2)
chart = ChartWizard()
weights = min_variance(returns)

更多详细信息，请参阅各个模块的文档。
"""
# 从basestat导入函数
from .basestat import (
    corr, corr_adj, cov, rank_corr, skew, sorted_rank,
    outlier_iqr, outlier_sig, eu_distance, cosine,
    approximate_entropy, min_max, uniform_weighting, corr_prob
)

# 导入其他模块的函数和类
from .chart import ChartWizard
from .fin import info_increment
from .matops import splmtset, array_shift, inverse_matrix
from .optimization import (
    min_variance, max_sharpe, risk_parity, equal_weight,
    equal_risk_contribution, max_diversification, mean_cvar,
    black_litterman, hierarchical_risk_parity
)
from .regression import (
    regress, regress_nointercept, regress_full,
    r_square, f_stat, f_pvalue, mse, ivxlh
)
from .stattest import kstest, adfuller_test
from .stochastic import (
    ou_process_zscore, cuscore, estimate_ou_para,
    estimate_ns_para, time_distribution, calc_exp_num,
    calc_exp_analytic, calc_exp_num_st, calc_exp_analytic_st
)
from .timeseries import (
    rolling_calculation, bootstrap_dt, ewma_vol, ewma,
    future_calculation, calculate_rolling_mean_returns,
    log_mean_ret, log_mean_ret_riskadj, hurst, kalman_filter,
    step_minus, step_plus, f_theta, omega_theta, beta_star,
    residual_uum_sq, sigma_hat
)
from .utils import cumsum, normal_pdf, digit_num

# 定义__all__变量
__all__ = [
    # basestat
    'corr', 'corr_adj', 'cov', 'rank_corr', 'skew', 'sorted_rank',
    'outlier_iqr', 'outlier_sig', 'eu_distance', 'cosine',
    'approximate_entropy', 'min_max', 'uniform_weighting', 'corr_prob',
    # chart
    'ChartWizard',
    # fin
    'info_increment',
    # matops
    'splmtset', 'array_shift', 'inverse_matrix',
    # optimization
    'min_variance', 'max_sharpe', 'risk_parity', 'equal_weight',
    'equal_risk_contribution', 'max_diversification', 'mean_cvar',
    'black_litterman', 'hierarchical_risk_parity',
    # regression
    'regress', 'regress_nointercept', 'regress_full',
    'r_square', 'f_stat', 'f_pvalue', 'mse', 'ivxlh',
    # stattest
    'kstest', 'adfuller_test',
    # stochastic
    'ou_process_zscore', 'cuscore', 'estimate_ou_para',
    'estimate_ns_para', 'time_distribution', 'calc_exp_num',
    'calc_exp_analytic', 'calc_exp_num_st', 'calc_exp_analytic_st',
    # timeseries
    'rolling_calculation', 'bootstrap_dt', 'ewma_vol', 'ewma',
    'future_calculation', 'calculate_rolling_mean_returns',
    'log_mean_ret', 'log_mean_ret_riskadj', 'hurst', 'kalman_filter',
    'step_minus', 'step_plus', 'f_theta', 'omega_theta', 'beta_star',
    'residual_uum_sq', 'sigma_hat',
    # utils
    'cumsum', 'normal_pdf', 'digit_num'
]