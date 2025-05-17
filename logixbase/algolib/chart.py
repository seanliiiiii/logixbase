# -*- coding: utf-8 -*-
"""
图表可视化模块
本模块提供了各种金融数据分析和可视化的绘图功能
"""

import numpy as np
import pandas as pd
from itertools import product
import statsmodels.api as sm
import matplotlib.pyplot as plt
from matplotlib.colors import TwoSlopeNorm
import seaborn as sns
from sklearn.linear_model import LinearRegression

from .utils import normal_pdf

# 配置matplotlib支持中文字符
plt.rcParams['font.sans-serif'] = ['SimHei']
plt.rcParams['axes.unicode_minus'] = False


class ChartWizard:
    """图表绘制工具类，封装了各种数据可视化的绘图功能"""

    @staticmethod
    def plot_heatmap(df: pd.DataFrame, v_max=None, v_min=None, c=0, reverse=False):
        """
        创建二维热力图

        参数:
            df: 目标数据框，用于可视化
            v_max: 颜色比例尺的最大值
            v_min: 颜色比例尺的最小值
            c: 发散色板的中心值

        返回:
            matplotlib.figure.Figure: 生成的热力图对象
        """
        if not isinstance(df, pd.DataFrame):
            raise ValueError("Input must be a pandas DataFrame")
        sns.set(style="white")

        if reverse:
            cmap = sns.diverging_palette(220, 10, l=60, as_cmap=True)
        else:
            cmap = sns.diverging_palette(10, 220, l=60, as_cmap=True)

        fig, ax = plt.subplots(figsize=(11, 9))  # 设置图形大小为(11,9)
        sns.heatmap(data=df, cmap=cmap, vmax=v_max, center=c, vmin=v_min,
                    annot=True, square=True, linewidths=.5, cbar_kws={"shrink": .5})
        return ax

    @staticmethod
    def plot_sharp_heat_map(input_ret: dict, para_range: list, year_trade_date=252,
                            v_max=3, v_min=0):
        """
        绘制夏普比率热力图

        参数:
            input_ret: 包含收益率数据的字典
            para_range: 参数范围列表
            year_trade_date: 年交易日数，默认252天
            vmax: 热力图最大值，默认3
            vmin: 热力图最小值，默认0

        返回:
            matplotlib.figure.Figure: 生成的热力图对象
        """
        shape = (int(len(para_range[-2])), int(len(para_range[-1])))
        sharps = pd.DataFrame()
        for x in product(*para_range[:-2]):
            paras_ret = [input_ret[str(list(x)+[i, j])] for i in sorted(para_range[-2], reverse=True)
                        for j in para_range[-1]]
            paras_ret = [x[np.isfinite(x)] for x in paras_ret]

            sharps_temp = [np.sum(data) / data.shape[0] * year_trade_date /
                          np.sqrt(((data - np.mean(data)) ** 2).sum() / (len(data)-1)) /
                          np.sqrt(year_trade_date) for data in paras_ret]

            index = [str(list(x)+[m]) for m in sorted(para_range[-2], reverse=True)]
            sharps_temp = pd.DataFrame(np.array(sharps_temp).reshape(shape), columns=para_range[-1], index=index)
            sharps = pd.concat([sharps, sharps_temp])

        cmap = sns.diverging_palette(220, 10, sep=10, as_cmap=True)
        fmt = '.0f' if max(abs(v_max), abs(v_min)) >= 20 else \
              '.1f' if max(abs(v_max), abs(v_min)) >= 2 else '.2f'

        ax = sns.heatmap(data=sharps, vmax=v_max, vmin=v_min, center=1,
                         robust=True, annot=True, annot_kws={'color': 'black'},
                         fmt=fmt, cmap=cmap)
        plt.title('夏普比率')
        ax.tight_layout()
        return ax

    @staticmethod
    def plot_sharpe_scatter(x, y, z, sharpe, center=0.5, vmax=3, vmin=0):
        """
        绘制 Sharpe 散点图，center 值为白色，小于 center 为蓝，大于 center 为红。
        超出 vmin/vmax 范围自动截断。

        x/y/z/sharpe 的序列长度保持一致，对应相同location的点表示当取(X, Y, Z)值时的sharp值
        参数:
            x: 参数1的序列
            y: 参数2的序列
            z: 参数3的序列
            sharpe: (X, Y, Z)取值时的sharp值
            vmax：热力图最大值，默认3
            vmin: 热力图最小值，默认0
            center：热力图为白色时的取值，默认0.5

        返回:
            matplotlib.figure.Figure: 生成的热力图对象
        """
        fig = plt.figure(figsize=(10, 8))
        ax = fig.add_subplot(111, projection='3d')

        sharpe_clipped = np.clip(sharpe, 0, vmax)

        norm = TwoSlopeNorm(vmin=0, vcenter=center, vmax=vmax)
        colors = plt.cm.bwr(norm(sharpe_clipped))

        ax.scatter(x, y, z, c=colors, s=30, alpha=0.8)

        sm = plt.cm.ScalarMappable(cmap='bwr', norm=norm)
        sm.set_array([])
        cbar = plt.colorbar(sm, ax=ax, shrink=0.5, aspect=10)
        cbar.set_label(f'Sharpe Ratio (center={center})')

        ax.set_title(f'3D Sharpe Scatter (center={center})')
        ax.set_xlabel('X param')
        ax.set_ylabel('Y param')
        ax.set_zlabel('Z param')

        return fig

    @staticmethod
    def quotes_returns_plot(quotes, returns):
        """
        绘制资产价格和收益率图表

        参数:
            quotes: 资产价格数据
            returns: 资产收益率数据

        返回:
            matplotlib.figure.Figure: 生成的价格和收益率图表对象
        """
        fig = plt.figure(figsize=(9, 6))
        plt.subplot(211)
        quotes.plot()
        plt.ylabel('资产价格')
        plt.grid(True)
        plt.axis('tight')

        plt.subplot(212)
        returns.plot()
        plt.ylabel('资产收益率')
        plt.grid(True)
        plt.axis('tight')
        return fig

    @staticmethod
    def histogram(data):
        """
        创建带概率密度函数的直方图

        参数:
            data: 用于创建直方图的输入数据

        返回:
            matplotlib.figure.Figure: 生成的直方图对象
        """
        fig = plt.figure(figsize=(9, 5))
        x = np.linspace(np.min(data), np.max(data), 100)
        plt.hist(data, bins=50)
        y = normal_pdf(x, np.mean(data), np.std(data))
        plt.plot(x, y, linewidth=2)
        plt.xlabel('对数数据')
        plt.ylabel('频率/概率')
        plt.grid(True)
        return fig

    @staticmethod
    def qq_plot(data):
        """
        创建Q-Q图用于正态性检验

        参数:
            data: 用于创建Q-Q图的输入数据

        返回:
            matplotlib.figure.Figure: 生成的Q-Q图对象
        """
        fig = sm.qqplot(data, line='s')
        plt.grid(True)
        plt.xlabel('理论分位数')
        plt.ylabel('样本分位数')
        return fig

    @staticmethod
    def realized_vol_plot(real_vol):
        """
        绘制已实现波动率图表

        参数:
            real_vol: 已实现波动率数据

        返回:
            matplotlib.figure.Figure: 生成的波动率图表对象
        """
        fig = plt.figure(figsize=(9, 5))
        real_vol.plot()
        plt.ylabel('已实现波动率')
        plt.grid(True)
        return fig

    @staticmethod
    def plot_price_series(df, ts1, ts2):
        """
        绘制两个价格序列的对比图

        参数:
            df: 包含价格序列的数据框
            ts1: 第一个时间序列的名称
            ts2: 第二个时间序列的名称

        返回:
            matplotlib.figure.Figure: 生成的价格序列对比图对象
        """
        fig, ax = plt.subplots()
        ax.plot(df.dropna().index, df[ts1], label=ts1)
        ax.plot(df.dropna().index, df[ts2], label=ts2)
        ax.xaxis.set_ticks(range(0, df.index.shape[0], max(int(df.index.shape[0]/6), 1)))
        ax.grid(True)
        fig.autofmt_xdate()

        plt.xlabel('日期')
        plt.ylabel('价格')
        plt.title(f'{ts1}和{ts2}日度价格')
        plt.legend()
        return fig

    @staticmethod
    def plot_scatter_series(df, ts1, ts2):
        """
        创建两个序列的散点图

        参数:
            df: 包含序列数据的数据框
            ts1: 第一个序列的名称
            ts2: 第二个序列的名称

        返回:
            matplotlib.figure.Figure: 生成的散点图对象
        """
        fig = plt.figure()
        plt.xlabel(ts1)
        plt.ylabel(ts2)
        plt.title(f'{ts1}和{ts2}散点图')
        plt.scatter(df[ts1], df[ts2])
        return fig

    @staticmethod
    def plot_residuals(df, x, y):
        """
        绘制线性回归残差图

        参数:
            df: 包含变量的数据框
            x: 自变量名称
            y: 因变量名称

        返回:
            matplotlib.figure.Figure: 生成的残差图对象
        """
        lr = LinearRegression()
        lr.fit(df.dropna()[[x]], df.dropna()[[y]])
        coef = lr.coef_
        intercept = lr.intercept_

        df = df.copy()
        df['residual'] = df[[y]].values - coef * df[[x]].values - intercept

        fig, ax = plt.subplots()
        ax.plot(df.dropna().index, df.dropna()['residual'], label='残差')
        ax.xaxis.set_ticks(range(0, df.index.shape[0], max(int(df.index.shape[0]/6), 1)))
        ax.grid(True)
        fig.autofmt_xdate()

        plt.xlabel('日期')
        plt.ylabel('价格')
        plt.title('残差图')
        plt.legend()
        return fig

    @staticmethod
    def plot_net_drawdown(input_net, drawdown_array):
        """
        绘制净值和最大回撤图

        参数:
            input_net: 包含净值数据的数据框
            drawdown_array: 回撤值数组

        返回:
            matplotlib.figure.Figure: 生成的净值和回撤图对象
        """
        daily_net = input_net.copy()
        dd_array = drawdown_array.copy()

        time_array = daily_net.index.astype(str)
        net_fig = plt.figure(figsize=(8, 8))
        grid = plt.GridSpec(9, 9, wspace=0.1, hspace=0.1)

        ax1 = net_fig.add_subplot(grid[0:6, :])
        ax1.plot(daily_net.values, label='净值')
        ax1.axes.get_xaxis().set_visible(False)
        ax1.legend(loc='best')
        ax1.grid()

        ax2 = net_fig.add_subplot(grid[6:9, :], sharex=ax1)
        ax2.fill_between(time_array, dd_array, where=dd_array < 0,
                        facecolor='green', alpha=0.7, label='最大回撤')
        ax2.xaxis.set_ticks(range(0, len(time_array), max(1, int(len(time_array)/6))))
        ax2.set_xlabel('日期')
        ax2.legend(loc='lower left')
        return net_fig

    @staticmethod
    def plot_nav(input_net):
        """
        绘制多个净值序列图

        参数:
            input_net: 包含净值序列的数据框

        返回:
            matplotlib.figure.Figure: 生成的净值序列图对象
        """
        daily_net = input_net.copy()
        time_array = daily_net.index.astype(str)
        net_fig = plt.figure(figsize=(10, 8))
        ax = net_fig.add_subplot(1, 1, 1)

        for col in daily_net.columns:
            ax.plot(time_array, daily_net[col], linestyle='-', label=col)
        ax.xaxis.set_ticks(range(0, len(time_array), max(1, int(len(time_array)/6))))
        ax.legend(loc='best')
        return net_fig

    @staticmethod
    def plot_ret_distribution(input_ret):
        """
        绘制收益率分布直方图

        参数:
            input_ret: 包含收益率数据的数据框

        返回:
            matplotlib.figure.Figure: 生成的分布图对象
        """
        daily_ret = input_ret.copy()
        dist_fig = plt.figure(figsize=(10, 8))
        dist = dist_fig.add_subplot(1, 1, 1)
        dist.hist(daily_ret.NaV.values, label='收益率分布', bins=80)
        dist.set_xlabel('日期')
        dist.legend(loc='best')
        return dist_fig

    @staticmethod
    def plot_monthly_returns(input_ret):
        """
        绘制月度收益率柱状图和箱线图

        参数:
            input_ret: 包含带有日期索引的收益率数据的数据框

        返回:
            matplotlib.figure.Figure: 生成的月度收益率图对象
        """
        daily_ret = input_ret.copy()
        daily_ret['month'] = daily_ret.index.map(lambda x: x.month)
        daily_ret['year'] = daily_ret.index.map(lambda x: x.year)
        monthly_ret = daily_ret.groupby(['month', 'year']).sum()['NaV'].unstack()

        month_fig = plt.figure(figsize=(10, 10))
        grid = plt.GridSpec(10, 10, wspace=0.1, hspace=0.5)

        ax1 = month_fig.add_subplot(grid[:5, :])
        ax1.bar(monthly_ret.index, monthly_ret.mean(axis=1), label='月度收益')
        ax1.axes.get_xaxis().set_visible(False)
        ax1.legend(loc='best')
        ax1.grid()

        ax2 = month_fig.add_subplot(grid[5:, :], sharex=ax1)
        ax2.boxplot(monthly_ret.dropna(axis=1).values.tolist(),
                   showmeans=True, patch_artist=True)
        ax2.grid()
        ax2.set_xlabel('月份')
        return month_fig

    @staticmethod
    def plot_asset_returns(category_return_table):
        """
        绘制资产收益率柱状图和箱线图

        参数:
            category_return_table: 包含资产收益率的数据框

        返回:
            matplotlib.figure.Figure: 生成的资产收益率图对象
        """
        asset_returns = category_return_table.copy()
        asset_returns = asset_returns.stack().reset_index()
        asset_returns.columns = ['日期', '合约', '盈亏']

        asset_returns['month'] = asset_returns.日期.map(lambda x: x.month)
        group = asset_returns.groupby(['合约', 'month']).sum()['盈亏'].unstack()

        asset_fig = plt.figure(figsize=(10, 10))
        grid = plt.GridSpec(10, 10, wspace=0.1, hspace=0.1)

        ax1 = asset_fig.add_subplot(grid[5:, :])
        ax1.bar(group.index, group.sum(axis=1), label='品种收益')
        ax1.legend(loc='best')
        ax1.grid()

        ax2 = asset_fig.add_subplot(grid[:5, :])
        ax2.boxplot(group.values.tolist(), showmeans=True, patch_artist=True)
        ax2.grid()
        ax2.set_xlabel('品种')
        ax2.axes.get_xaxis().set_visible(False)
        return asset_fig

