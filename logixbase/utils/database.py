from abc import ABC, abstractmethod
from typing import Any
import cx_Oracle
import pymssql
import pandas as pd
import socket
import os
from pathlib import Path
from jinja2 import Template

from .schema import DatabaseConfig
from ..protocol import DatabaseProtocol as DBP

class DatabaseConnector(DBP):
    """
    数据库连接接口基类
    """

    @abstractmethod
    def connect(self) -> Any:
        """
        获取数据库连接
        """
        pass

    @abstractmethod
    def disconnect(self) -> bool:
        """
        关闭数据库连接
        """
        pass

    @abstractmethod
    def is_connected(self):
        """检查数据库连接"""
        pass

    @abstractmethod
    def check_connect(self):
        pass

class DatabaseExecutor(ABC):
    """
    数据库执行接口基类
    """

    @abstractmethod
    def exec_query(self, sql: str) -> pd.DataFrame:
        """
        执行查询操作，返回DataFrame
        """
        pass

    @abstractmethod
    def exec_insert(self, sql: str, data: tuple) -> None:
        """
        执行单条插入操作
        """
        pass

    @abstractmethod
    def exec_multi_insert(self, sql: str, data: list, insert_lim: int = 10000) -> None:
        """
        执行批量插入操作
        """
        pass

    @abstractmethod
    def exec_nonquery(self, sql: str) -> None:
        """
        执行非查询操作（如更新、删除）
        """
        pass


class DealWithSql(DatabaseConnector, DatabaseExecutor):
    """
    初始化数据库连接对象。
    Args:
        conn_cfg : 包含连接信息的字典。
    Attributes:
        host (str): 数据库主机地址。
        port (int): 数据库端口号，默认为1433。
        username (str): 数据库用户名。
        password (str): 数据库密码。
        db (str, optional): 数据库名称，默认为None。
        conna (object): 数据库连接对象。
    Raises:
        KeyError: 如果`conn_info`字典中缺少必要的键（如"host", "username", "password"）则抛出此异常。
    """
    def __init__(self, conn_cfg: DatabaseConfig, trys: int = 1):
        self._trys: int = trys
        self.host = conn_cfg.host
        self.port = conn_cfg.port
        self.username = conn_cfg.username
        self.password = conn_cfg.password
        self.db = conn_cfg.database
        # Create sql connector
        self.conna = self.connect()

    def connect(self) -> Any:
        """
        创建一个数据库连接。
        Returns:
            pymssql.Connection: 如果成功连接数据库，则返回连接对象；否则返回None。

        """
        # Check sql server existence in network
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(1)
        status = sock.connect_ex((self.host, self.port))
        if status:
            print(f"未检测到SqlServer数据库: {self.host}:{self.port}")
            return None
        # Establish connection
        i = 0
        conna = None
        while i < self._trys:
            try:
                conna = pymssql.connect(host=self.host, database=self.db,
                                        user=self.username, password=self.password, charset="utf8")
                break
            except Exception as e:
                print(f"SqlServer数据库连接失败 | IP：{self.host} | 用户：{self.username} | 第{i + 1}次尝试 |{e}")
                i += 1
                conna = None
        return conna

    def is_connected(self):
        """
        检查数据库连接是否成功。
        Returns:
            bool: 如果数据库连接成功，则返回 True；否则返回 False。

        Raises:
            无

        详细说明：
            尝试使用数据库连接对象 `self.conna` 创建一个游标对象 `cur`，
            并执行一个简单的 SQL 查询（"SELECT 1"）。
            如果查询成功执行，表示数据库连接正常，返回 True。
            如果执行过程中出现异常，捕获异常并打印错误信息，返回 False。
        """
        try:
            cur = self.conna.cursor()
            cur.execute("SELECT 1")
            return True
        except Exception as e:
            print(f"SqlServer连接已断开: {e}")
            return False

    def check_connect(self):
        """检查SqlServer数据库连接"""
        while not self.is_connected():
            self.conna = self.connect()
            if self.conna is None:
                print("连接至SqlServer失败")

    def __get_cur(self):
        """
        创建并返回一个游标对象。
        Returns:
            cursor: 数据库游标对象
        Raises:
            无
        说明：
            该方法首先检查数据库连接是否有效，如果无效则尝试重新连接。
            如果重新连接成功，则返回一个游标对象；如果连接失败，则打印错误信息。
        """
        self.check_connect()
        return self.conna.cursor()

    def disconnect(self):
        """
        关闭 SQL 数据库连接。
        Returns:
            bool: 总是返回 True，表示连接已关闭。
        说明:
            如果 self.conna 不是 None，则调用其 close 方法关闭数据库连接，并将 self.conna 设置为 None。
            如果 self.conna 已经是 None，则直接返回 True，不做任何操作。
        """
        if self.conna is not None:
            self.conna.close()
            self.conna = None
        return True

    def exec_query(self, sql: str):
        """
        执行查询函数

        Args:
            sql (str): 要执行的SQL语句。

        Returns:
            pandas.DataFrame: 如果查询成功，返回一个包含查询结果的DataFrame对象；
                              如果查询结果为空，则返回None。

        描述:
            此函数用于执行传入的SQL查询语句，并返回一个包含查询结果的pandas DataFrame对象。
            如果查询结果为空（即没有返回列信息），则函数返回None。
        """
        # Create cursor
        cur = self.__get_cur()
        cur.execute(sql)
        if cur.description is None:
            return None
        column_names = [item[0] for item in cur.description]
        data = pd.DataFrame(list(cur.fetchall()), columns=column_names)

        return data

    def exec_multi_insert(self, sql: str, data: list, insert_lim: int = 10000):
        """
        批量插入数据到SQL Server中。

        Args:
            sql (str): SQL代码字符串，例如 INSERT INTO table_name VALUES (%d, %s, %s)。
            data (list): 数据列表，每个元素是一个元组，例如 [(1, "John Smith", "John Doe"), (2, "Jane Doe", "Joe Dog")]。
            insert_lim (int, optional): 单次插入的数据条数限制，默认为10000。

        Returns:
            无返回值。

        Raises:
            无特定异常类型，但会捕获并处理 pymssql.Error 异常。

        """
        cur = self.__get_cur()
        try:
            for i in range(0, int(len(data)), insert_lim):
                insert_temp = data[i:i + insert_lim]
                cur.executemany(sql, insert_temp)
            self.conna.commit()
        except pymssql.Error as e:
            print(f"SqlServer数据库批量插入失败: {e}")
            self.conna.rollback()

    def exec_insert(self, sql: str, data: tuple):
        """
        向SQL服务器执行单条插入操作

        Args:
            sql (str): SQL代码字符串，例如：INSERT INTO table_name VALUES (%d, %s, %s)
            data (tuple): 数据元组，例如：(1, "John Smith", "John Doe"), (2, "Jane Doe", "Joe Dog")

        Returns:
            无返回值

        Raises:
            pymssql.Error: 如果在执行SQL时出现错误，则抛出此异常

        说明：
            该函数用于向SQL服务器中插入单条记录。它首先获取数据库连接游标，然后执行事务开始操作，
            接着执行传入的SQL语句和数据，最后执行事务提交操作。如果在执行过程中出现错误，则捕获异常，
            回滚事务，并打印错误信息。最后，无论操作是否成功，都会关闭游标并释放资源。
        """
        cur = self.__get_cur()
        try:
            cur.execute(sql, data)
            self.conna.commit()
        except pymssql.Error as e:
            print(f"SqlServer数据库插入失败: {e}")
            self.conna.rollback()

    def exec_nonquery(self, sql: str):
        """
        执行非查询和非插入操作的SQL语句。

        Args:
            sql (str): 要执行的SQL语句，例如truncate table或delete操作。

        Returns:
            无返回值。

        Raises:
            pymssql.Error: 如果执行SQL语句时发生错误，将捕获此异常并回滚事务。

        说明：
            该函数用于执行除插入和查询操作之外的其他SQL语句，如truncate table或delete操作。
            在执行SQL语句之前，会开启一个事务，并在执行完毕后提交事务。
            如果执行过程中发生错误，将捕获异常并回滚事务，然后打印错误信息。
            最后，无论事务是否成功，都会提交数据库连接。
        """
        cur = self.__get_cur()
        try:
            cur.execute(sql)
            self.conna.commit()
        except pymssql.Error as e:
            print(f"SqlServer数据库查询失败: {e}")
            self.conna.rollback()

    def execute_sqlfile(self, file: [Path, str], context: dict = None):
        cur = self.__get_cur()
        if not Path(file).suffix.lower() == ".sql":
            print("当前文件不是.sql文件")
            return

        with open(file, 'r', encoding='utf-8') as f:
            sql = f.read()

            if context:
                sql = Template(sql).render(context)
            try:
                cur.execute(sql)
                self.conna.commit()
                print(f"{file}成功执行.")
            except Exception as e:
                print(f"Error in {file}: {e}")


    def batch_execute_sqlfile(self, directory: [Path, str], context: dict = None):
        for filename in os.listdir(directory):
            if filename.endswith('.sql'):
                self.execute_sqlfile(Path(directory).joinpath(filename), context)


class DealWithOracle:
    """
    初始化Oracle数据库处理类。

    Args:
        conn_cfg (dict, optional): 包含数据库连接信息的字典。如果未提供，则使用默认连接信息。

    Returns:
        无

    Notes:
        此构造函数用于初始化Oracle数据库处理类的实例。如果未提供`conn_info`参数，则使用默认的登录信息。
        这些默认信息包括主机地址、端口、用户名、密码和服务名称。
        pymssql：https://www.lfd.uci.edu/~gohlke/pythonlibs/#pymssql
        此外，请注意，为了使用Oracle数据库，您需要在SQL Server配置管理器中打开TCP/IP连接。
    """
    def __init__(self, conn_cfg: DatabaseConfig):
        self.host = conn_cfg.host
        self.port = conn_cfg.port
        self.username = conn_cfg.username
        self.password = conn_cfg.password
        self.service_name = conn_cfg.database
        # initialize connection variable
        self.conna = None

    def connect(self):
        """
        获取数据库连接。
        Returns:
            conna (cx_Oracle.Connection): 数据库连接对象。
        """
        conn_info = f"{self.username}/{self.password}@{self.host}:{self.port}/{self.service_name}"
        conna = cx_Oracle.connect(conn_info)
        conna.autocommit = False

        return conna

    # Create cursor
    @staticmethod
    def __get_cur(conna):
        """
        获取当前数据库连接的游标对象。
        Args:
            conna: 数据库连接对象。
        Returns:
            cur: 游标对象。
        Raises:
            NameError: 如果无法连接到Oracle数据库，则引发此异常。
        """
        cur = conna.cursor()
        if not cur:
            raise(NameError, "Fail to connect Oracle database")
        else:
            return cur

    def _reconnect(self):
        """
        重新建立与Oracle数据库的连接。
        Returns:
            无
        Raises:
            无
        Notes:
            - 如果当前没有连接（self.conna 为 None），则直接调用 connect 方法建立新连接。
            - 如果当前已有连接，则先尝试关闭该连接，并删除该连接属性。
            - 如果在关闭连接时遇到异常，会捕获该异常并打印错误信息，然后继续尝试重新建立连接。
            - 最终，无论之前是否有连接，都会通过调用 connect 方法来确保有一个有效的数据库连接。
        """
        if self.conna is None:
            print("Lose connection to Oracle, reconnect...")
            self.conna = self.connect()
        else:
            try:
                self.conna.close()
                del self.conna
            except Exception as e:
                print(f"Failed oracle reconnect: {e}")
                del self.conna
            self.conna = self.connect()

    def disconnect(self):
        """
        关闭连接。
        Returns:
            无
        Description:
            此方法用于关闭当前对象的连接。如果 `self.conna` 不为 None，则调用其 `close` 方法关闭连接，并将 `self.conna` 设置为 None。
        """
        if self.conna is not None:
            self.conna.close()
            self.conna = None

    def exec_query(self, sql: str):
        """
        执行SQL查询并返回查询结果的数据框。
        Args:
            sql (str): 要执行的SQL查询语句。
        Returns:
            pd.DataFrame: 查询结果的数据框。
        """
        # Check connection status
        self._reconnect()
        # Create cursor
        cur = self.__get_cur(self.conna)
        # Execute query
        cur.execute(sql)
        # Formatting query data
        column_names = [item[0] for item in cur.description]
        data = pd.DataFrame(list(cur.fetchall()), columns=column_names)

        return data

    def exec_nonquery(self, sql: str):
        """
        执行非查询 SQL 语句。
        Args:
            sql (str): 要执行的 SQL 语句。
        Returns:
            None
        Raises:
            无直接异常抛出，但在 SQL 执行失败时会打印错误信息并回滚事务。
        """

        self._reconnect()
        # Create cursor
        cur = self.__get_cur(self.conna)
        try:
            cur.execute(sql)
        except Exception as e:
            print(f"ExecNonQuery Errors: {sql[:min(20, len(sql))]}, Error: {e}... ")
            self.conna.rollback()  # rollback
        finally:
            self.conna.commit()

    def exec_insert(self, sql: str, data: tuple) -> None:
        """
        执行插入操作。
        Args:
            sql (str): 要执行的SQL插入语句。
            data (tuple): 要插入的数据，以元组形式提供。
        Returns:
            None: 该方法不返回任何值。
        Raises:
            NotImplementedError: 抛出此异常，表示Oracle数据库不支持单条插入操作。
        Notes:
            此方法在当前Oracle数据库环境中不支持单条插入操作，调用此方法会抛出异常。
        """
        raise NotImplementedError("Oracle数据库不支持单条插入操作")

    def exec_multi_insert(self, sql: str, data: list, insert_lim: int = 10000) -> None:
        """
        执行批量插入操作。
        Args:
            sql (str): 要执行的SQL语句。
            data (list): 包含要插入的数据的列表。
            insert_lim (int, optional): 每次批量插入的数据量限制，默认为10000。
        Returns:
            None
        Raises:
            NotImplementedError: 抛出 NotImplementedError 异常，表示 Oracle 数据库不支持批量插入操作。
        Notes:
            由于Oracle数据库不支持批量插入操作，因此该方法总是抛出 NotImplementedError 异常。
        """
        raise NotImplementedError("Oracle数据库不支持批量插入操作")
