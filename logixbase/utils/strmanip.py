import re


def split_camel_case_to_snake_case(s):
    """
        将字符串中的双驼峰（CamelCase）单词拆分并用单下划线（_）连接转换为蛇形命名（snake_case）。

        Args:
            s (str): 输入的字符串，其中包含双驼峰命名的单词。

        Returns:
            str: 拆分并连接后的字符串，采用蛇形命名法。

        """
    # 使用正则表达式匹配双驼峰单词，并进行拆分
    s = re.sub(r'(?<!^)(?=[A-Z])', '_', s).lower()
    return s
