# 工具函数模块
"""
本模块包含了一些通用的工具函数。
主要功能包括：
1. 正态分布PDF计算
2. 字符串数字统计
3. 信息增量分析

这些函数可以在各种场景下使用，提供了一些基本的工具性功能。
"""
import numpy as np
from numba import njit


@njit
def cumsum(array, axis=0):
    """
    计算累积和。

    Args:
        array (np.ndarray): 输入数组。
        axis (int, optional): 计算轴，0为列，1为行。默认为0。

    Returns:
        np.ndarray: 累积和数组。

    """
    y = np.full(array.shape, np.nan)
    if np.array(array.shape).shape[0] == 1 or y.shape[1] == 1:
        x = 0
        for i in range(array.shape[0]):
            x = x + array[i]
            y[i] = x
    else:
        if axis == 0:
            for j in range(array.shape[1]):
                x = 0
                for i in range(array.shape[0]):
                    x = x + array[i, j]
                    y[i, j] = x
        elif axis == 1:
            for i in range(array.shape[0]):
                x = 0
                for j in range(array.shape[1]):
                    x = x + array[i, j]
                    y[i, j] = x
    return y


@njit
def normal_pdf(x, mu, sigma):
    """
    计算正态分布的概率密度函数(PDF)

    Args:
        x (float): 输入值
        mu (float): 均值
        sigma (float): 标准差

    Returns:
        float: PDF值

    """

    z = (x - mu) / sigma
    pdf = np.exp(-0.5 * z ** 2) / np.sqrt(2 * np.pi * sigma ** 2)
    return pdf


@njit
def digit_num(string):
    """
    统计字符串中数字的个数。

    Args:
        string (str): 输入字符串。

    Returns:
        int: 字符串中数字的个数。

    """
    num = 0
    for i in string:
        if i.isdigit():
            num += 1
    return num


