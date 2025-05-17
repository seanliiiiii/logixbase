import paramiko
import smtplib
import os
from email.utils import COMMASPACE, formatdate
from email import encoders
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText


os.environ["NLS_LANG"] = "AMERICAN_AMERICA.AL32UTF8"


class SSHConnection(object):
    """
    类的初始化方法。
    Args:
        host (str): 服务器的主机名或IP地址。
        port (int): 服务器监听的端口号。
        username (str): 登录服务器所需的用户名。
        pwd (str): 登录服务器所需的密码。
    Attributes:
        host (str): 存储服务器的主机名或IP地址。
        port (int): 存储服务器监听的端口号。
        username (str): 存储登录服务器所需的用户名。
        pwd (str): 存储登录服务器所需的密码。
        __k (Any): 私有属性，用于存储特定信息（默认为None）。
        __transport (Any): 私有属性，用于存储传输对象（默认为None）。
    """
    def __init__(self, host, port, username, pwd):
        self.host = host
        self.port = port

        self.username = username
        self.pwd = pwd
        self.__k = None
        self.__transport = None

    def connect(self):
        """
        连接到远程服务器。
        使用paramiko库与远程服务器建立SSH连接。该函数首先创建一个paramiko.Transport对象，
        并使用self.host和self.port指定的主机和端口进行初始化。然后，通过调用transport.connect()方法，
        使用self.username和self.pwd指定的用户名和密码进行连接。最后，将创建的transport对象赋值给
        类的私有属性__transport，以便后续使用。
        """
        transport = paramiko.Transport((self.host, self.port))
        transport.connect(username=self.username, password=self.pwd)
        self.__transport = transport

    def close(self):
        """
        关闭与远程服务器的连接。
        该函数会调用内部传输对象的close方法，以关闭与远程服务器的连接。
        """
        self.__transport.close()

    def upload(self, local_path, target_path):
        """
        将本地文件上传到远程服务器。
        Args:
            local_path (str): 本地文件路径。
            target_path (str): 远程服务器上的目标文件路径。
        Returns:
            None
        Raises:
            Exception: 如果文件上传过程中发生错误，将引发异常。
        """
        sftp = paramiko.SFTPClient.from_transport(self.__transport)
        sftp.put(local_path, target_path)

    def download(self, remote_path, local_path):
        """
        从远程服务器下载文件到本地。
        Args:
            remote_path (str): 远程文件的路径。
            local_path (str): 本地文件的保存路径。
        Returns:
            None
        Raises:
            无
        """
        sftp = paramiko.SFTPClient.from_transport(self.__transport)
        sftp.get(remote_path, local_path)

    def cmd(self, command):
        """
        执行远程命令。
        Args:
            command (str): 要执行的远程命令。
        Returns:
            bytes: 命令执行结果的字节数据。
        """
        ssh = paramiko.SSHClient()
        ssh._transport = self.__transport
        # 执行命令
        stdin, stdout, stderr = ssh.exec_command(command)
        # 获取命令结果
        result = stdout.read()
        print(str(result, encoding="utf-8"))
        return result


def send_mail_file(subject: str,
                   input_data: tuple,
                   mailto: list,
                   mail_setting: dict):
    """
    发送包含附件的邮件。

    Args:
        subject (str): 邮件主题。
        input_data (tuple): 包含邮件正文和附件文件的元组。第一个元素为邮件正文（字符串），第二个元素为附件文件列表。
        mailto (list): 收件人邮箱地址列表。
        mail_setting (dict): 邮件设置字典，包含以下键：
            - host (str): SMTP 服务器地址。
            - username (str): 邮箱用户名。
            - password (str): 邮箱密码。
            - mailname (str): 发件人邮箱地址。

    Returns:
        bool: 如果邮件发送成功，返回 True；如果发送失败，返回 False。

    Raises:
        AssertionError: 如果附件文件列表不是列表类型，将引发此异常。

    """
    text, files = input_data

    mail_host = mail_setting["host"]
    mail_user = mail_setting["username"]
    mail_pass = mail_setting["password"]
    mail_name = mail_setting["mailname"]
    assert type(files) == list

    msg = MIMEMultipart()
    msg["From"] = mail_user
    msg["Subject"] = subject
    msg["To"] = COMMASPACE.join(mailto)  # COMMASPACE==", "
    msg["Date"] = formatdate(localtime=True)
    # msg.attach(MIMEText(text,"plain","utf-8"))
    msg.attach(MIMEText(text, _subtype="html", _charset="utf-8"))
    msg["Accept-Language"] = "zh-CN"
    msg["Accept-Charset"] = "ISO-8859-1,utf-8"

    for file in files:
        if file.split(".")[-1] in ["xls", "xlsx", "csv"]:
            part = MIMEText(str(open(file, "rb").read()), "xlsx", "gb2312")
            part["Content-Type"] = "application/octet-stream"
            part["Content-Disposition"] = "attachment;filename= " + file.split("\\")[-1]
        else:
            part = MIMEBase("application", "octet-stream")  # "octet-stream": binary data
            part.set_payload(open(file, "rb").read())
            encoders.encode_base64(part)
            part.add_header("Content-Disposition", "attachment; filename='%s'" % os.path.basename(file))
        msg.attach(part)

    server = smtplib.SMTP()
    try:
        server.connect(mail_host)
        server.login(mail_user, mail_pass)
        server.sendmail(mail_name, mailto, msg.as_string())
        server.close()
        print("Email sent.")
        return True
    except NotImplementedError:
        print("Email sending failed")
        server.close()
        return False


def send_email_text(subject: str,
                    content: str,
                    mailto: list,
                    mail_setting: dict):
    """
    发送文本邮件的函数。

    Args:
        subject (str): 邮件的主题。
        content (str): 邮件的正文内容。
        mailto (list): 接收邮件的邮箱地址列表。
        mail_setting (dict): 邮件设置，包含主机名、用户名、密码和发件人名称。

    Returns:
        bool: 如果邮件发送成功，返回 True；如果发送失败，返回 False。

    Raises:
        无。
    """
    mail_host = mail_setting["host"]
    mail_user = mail_setting["username"]
    mail_pass = mail_setting["password"]
    mail_name = mail_setting["mailname"]

    mail_sender = "<" + mail_name + ">"
    # msg = MIMEText(content,_subtype="plain",_charset="gb2312")
    msg = MIMEText(content, _subtype="html", _charset="gb2312")
    msg["Subject"] = subject
    msg["From"] = mail_sender
    msg["To"] = ";".join(mailto)

    server = smtplib.SMTP()
    try:
        server.connect(mail_host)
        server.login(mail_user, mail_pass)
        server.sendmail(mail_sender, mailto, msg.as_string())
        server.close()
        print("Email sent.")
        return True
    except NotImplementedError:
        print("Email sending failed")
        server.close()
        return False


