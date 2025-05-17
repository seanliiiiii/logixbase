import numpy as np
from numba import jit
import scipy.stats as st
from .regression import regress_full


@jit
def info_increment(ret_1, ret_2, thres: float = 0.05):
    """
    根据两个输入的时间序列数据，计算它们之间的增量信息。

    Args:
        ret_1 (np.ndarray): 第一个时间序列数据。
        ret_2 (np.ndarray): 第二个时间序列数据。
        thres (float, optional): 显著性水平阈值，默认值为0.05。

    Returns:
        tuple: 包含两个元素的元组，分别表示两个时间序列之间的增量信息。
            - 第一个元素为1时表示ret_1对ret_2有显著的增量信息，为0时表示没有。
            - 第二个元素为1时表示ret_2对ret_1有显著的增量信息，为0时表示没有。
    """
    df = ret_1.shape[0] - 2
    # x: 1, y: 2
    t0_1 = regress_full(ret_2, ret_1)[1][1]
    p1 = 2 * st.t.sf(abs(t0_1), df)
    # x: 2, y: 1
    t0_2 = regress_full(ret_1, ret_2)[1][1]
    p2 = 2 * st.t.sf(abs(t0_2), df)

    if (p1 <= thres) and (p2 <= thres):
        return 1, 1
    elif (p1 <= thres) and (p2 > thres):
        return 0, 1
    elif (p1 > thres) and (p2 <= thres):
        return 1, 0
    else:
        return 0, 0
