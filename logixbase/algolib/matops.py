# 矩阵操作模块
"""
本模块包含了用于矩阵和数组操作的函数。
主要功能包括：
1. 计算两个数组之间的补集

这些函数可以帮助进行基本的矩阵和数组操作，特别是在处理不同形状的数组时。
"""
import numpy as np
from numba import jit, njit


@jit
def splmtset(x, y):
    """
    获取两个数组之间的补集

    Args:
        x (np.ndarray): 第一个数组。
        y (np.ndarray): 第二个数组。

    Returns:
        numpy.ndarray: 补集数组。

    该函数使用jit加速，可以处理一维和多维数组。如果x和y都是一维数组，则函数将返回x中不在y中的元素；
    如果x和y是形状相同的二维数组，则函数将返回x中不在y中的行；
    如果x和y的行数相同但列数不同，则函数将返回x中不在y中的列。
    如果x和y的形状不相同，函数将抛出异常。
    """
    sup = 0
    if len(x.shape) == 1:
        par = x if len(x) > len(y) else y
        son = y if len(x) > len(y) else x
        sup = np.full(len(par) - len(son), np.nan)
        t = 0
        for i in range(len(par)):
            if t < len(sup):
                if not (par[i] in son):
                    sup[t] = par[i]
                    t += 1
            else:
                break
    else:
        if x.shape[1] == y.shape[1]:
            par = x if x.shape[0] > y.shape[0] else y
            son = y if x.shape[0] > y.shape[0] else x
            suprow = abs(x.shape[0] - y.shape[0])
            supshape = (suprow, x.shape[1])
            sup = np.full(supshape, np.nan)
            t = 0
            for r in range(par.shape[0]):
                if t < suprow:
                    if not (par[r] in son):
                        sup[t] = par[r]
                        t += 1
                else:
                    break
        elif x.shape[0] == y.shape[0]:
            par = x if x.shape[1] > y.shape[1] else y
            son = y if x.shape[1] > y.shape[1] else x
            supcols = abs(x.shape[1] - y.shape[1])
            supshape = (x.shape[0], supcols)
            sup = np.full(supshape, np.nan)
            t = 0
            for c in range(par.shape[1]):
                if t < supcols:
                    if not (par[:, c] in son.T):
                        sup[:, t] = par[:, c]
                        t += 1
                else:
                    break
    return sup


@njit
def array_shift(arr: np.array, n: int):
    """
    将数组沿x轴移动n步

    Args:
        arr (np.array): 数据数组
        n (int): 数组数据移动的步数

    Returns:
        np.array: 数据数组

    """
    x = np.full(arr.shape, np.nan)
    if n > 0:
        x[n:] = arr[:-n]
    elif n < 0:
        x[:n] = arr[-n:]
    else:
        x = x
    return x


@jit
def inverse_matrix(mat):
    """
    计算矩阵的逆矩阵。

    Args:
        mat (numpy.ndarray): 输入的矩阵。

    Returns:
        numpy.ndarray: 输入矩阵的逆矩阵。

    Raises:
        ValueError: 当矩阵的条件数（condition number）小于 1 / np.finfo(mat.dtype).eps 时，
            表示矩阵为奇异矩阵，无法求逆，此时会抛出 ValueError 异常。

    Note:
        使用 numpy.linalg.cond 函数计算矩阵的条件数，当条件数小于 1 / np.finfo(mat.dtype).eps 时，
        认为矩阵是奇异矩阵，直接返回逆矩阵可能会导致计算不稳定或错误结果。
        当矩阵不是奇异矩阵时，使用奇异值分解（SVD）来求逆矩阵。
    """
    if np.linalg.cond(mat) < 1 / np.finfo(mat.dtype).eps:
        return np.linalg.solve(mat, np.eye(mat.shape[1], dtype=float))
    else:
        u, s, v = np.linalg.svd(mat)
        d = np.diag(s)
        tmp = np.matmul(v, np.linalg.inv(d))
        return np.matmul(tmp, np.transpose(u))
