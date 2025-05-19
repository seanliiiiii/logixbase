import yaml
import uuid
import multiprocessing as mp
import configparser
from pathlib import Path
from typing import Type, Union, get_type_hints, Dict, List, Callable
from importlib.util import spec_from_file_location, module_from_spec

from pydantic import BaseModel

from .schema import BaseConfig
from .proxy import ConfigProxy
from .hotreloader import HotReloader

from ..protocol import OperationProtocol as OPP, LoaderProtocol as LDP, IdentityProtocol as IDP


EXT_LOADERS = {
    ".py": lambda f: load_py_config(f),
    ".ini": lambda f: load_ini_config(f),
    ".yaml": lambda f: load_yaml_config(f),
    ".yml": lambda f: load_yaml_config(f),
}


LAYER_ORDER = {"base": 0, "dev": 1, "prod": 1, "env": 2}


class ConfigLoader(OPP, LDP, IDP):
    """
    初始化配置管理类实例。
    配置文件层级：base > dev / prod > env

    关键函数：
    load：加载配置schema实例
    register_schema：注册自定义配置数据类
    start_hot_reload：启动热更新模式，可用于单进程/多进程模式下的全局配置同步，多进程下必须在子进程启动后再启动热更新。
    stop_hot_reload：停止热更新模式
    register_callback：注册主进程的热更新回调函数
    register_mp_proxy： 注册多进程模式下的配置代理，仅在多进程模式下使用，子进程中调用。
    stop_mp_proxy：停止多进程模式下的配置代理
    register_proxy_callback：注册子进程的热更新回调函数

    config：配置文件最新实例
    share_variable：全局共享数据
    dict：配置文件最新实例的字典形式
    json：配置文件的json形式

    Args:
        schema_cls (Type[BaseModel], optional): 配置模式的类，默认为BaseConfig。Defaults to BaseConfig.
        mode (str, optional): 配置模式，指定对应的level，默认为"dev"。Defaults to "dev".
    Attributes:
        schema_cls (Type[BaseModel]): 配置模式的类。
        mode (str): 配置模式。
        config_path (Union[str, Path]): 配置文件的路径。
        _registry_schema (dict): 注册的配置模式。
        _share_data (Union[dict, None]): 共享数据字典。
        _share_event (Union[mp.Event, None]): 共享事件。
        _share_response (Union[mp.Manager.Queue, None]): 共享响应队列。
        _proxy_num (mp.Value): 代理计数。
        proxy (ConfigProxy): 配置代理对象。
        _hot_manager (Union[HotReloader, None]): 热重载管理器。
    """
    def __init__(self, config_path: Union[Path, str], schema_cls: Type[BaseModel] = BaseConfig,
                 mode: str = "dev"):
        self.config_path: Union[str, Path] = Path(config_path)

        self.schema_cls = schema_cls
        self.mode = mode

        self._name = "配置管理器"
        self._uuid: str = str(uuid.uuid4())

        self._registry_schema = {}
        self._share_data: Union[dict, None] = None
        self._share_event: Union[mp.Event, None] = None
        self._share_response: Union[mp.Manager.Queue, None] = None
        self._proxy_num: mp.Value = None
        self._logger = None

        self.proxy = ConfigProxy(schema_cls)
        self._hot_manager: Union[HotReloader, None] = None

    def get_id(self):
        return f"{self._name}:{self._uuid}"

    def get_name(self):
        return self._name

    def _find_schema_class(self, module_name: str) -> Union[Type[BaseModel], None]:
        """
        根据模块名称查找对应的Schema类。
        Args:
            module_name (str): 要查找的模块名称。
        Returns:
            Type[BaseModel]: 对应的Schema类。
        Raises:
            ValueError: 如果在 self.schema_cls 中未声明模块且未在注册表中注册该模块，则抛出 ValueError 异常。
        """
        module_map = get_type_hints(self.schema_cls)
        for k, v in module_map.items():
            if k.lower() == module_name.lower():
                return v
        if module_name.lower() in self._registry_schema:
            return self._registry_schema[module_name.lower()]
        print(f"未在 {self.schema_cls.__name__} 中声明模块，且未注册: {module_name}")
        return None

    def set_config_path(self, config_path: Union[Path, str]):
        """
        设置配置文件路径。
        Args:
            config_path (Union[Path, str]): 配置文件的路径，可以是Path对象或字符串。
        Returns:
            None
        """
        self.config_path = Path(config_path)

    def load(self) -> BaseModel:
        """
        从配置文件中加载配置并实例化模型。
        Returns:
            BaseModel: 实例化后的模型对象。
        Raises:
            ValueError: 如果缺少必要的配置模块，则抛出异常。
        """
        # 初始化模块配置数据的字典
        module_config_data: Dict[str, dict] = {}
        # 初始化分组文件的字典
        grouped_files: Dict[str, List[Path]] = {"base": [], self.mode: [], "env": []}
        # 判断传入的配置路径是文件还是目录
        if self.config_path.is_file():
            files = [self.config_path]
        else:
            # 如果是目录，则获取目录下所有符合特定后缀的文件
            files = [f for f in self.config_path.glob("**/*") if f.suffix in EXT_LOADERS]
        # 遍历获取到的文件列表
        for file in files:
            # 解析文件的层级
            layer = self._parse_layer(file)
            # 将文件添加到对应的分组中
            if layer in grouped_files:
                grouped_files[layer].append(file)
        # 将分组中的文件合并为一个列表
        merged_files = grouped_files["base"] + grouped_files[self.mode] + grouped_files["env"]
        # 遍历合并后的文件列表
        for file in merged_files:
            # 读取配置文件的内容
            raw_data = read_config(file)
            # 遍历配置文件中的每个section
            for section, section_data in raw_data.items():
                # 如果section不在module_config_data中，则初始化一个空字典
                if section not in module_config_data:
                    module_config_data[section] = {}
                # 合并当前section的配置数据
                module_config_data[section] = merge_dicts(module_config_data[section], section_data)
        # 初始化schema数据的字典
        schema_data = {}
        # 遍历module_config_data中的每个模块配置数据
        for module, config_dict in module_config_data.items():
            # 查找对应模块的schema类
            schema_class = self._find_schema_class(module)
            if schema_class is None:
                continue
            # 生成配置实例
            instance = create_schema(config_dict, schema_class)
            # 将实例添加到schema_data中
            schema_data[module] = instance
        # 获取schema_cls中所有需要的类型提示的键，并将其转换为小写
        required_modules = [k.lower() for k in get_type_hints(self.schema_cls).keys()]
        # 找出schema_data中缺失的模块
        missing = [m for m in required_modules if m not in schema_data]
        # 如果存在缺失的模块，则抛出异常
        if missing:
            raise ValueError(f"以下配置模块缺失: {missing}")

        # 使用schema_data中的数据实例化schema_cls
        instance = self.schema_cls(**schema_data)
        # 将实例设置到代理对象中
        self.proxy.set_instance(instance)
        # 返回实例化后的对象
        return instance

    def start(self):
        self.start_hot_reload()

    def join(self):
        pass

    def stop(self):
        self.stop_hot_reload()

    @staticmethod
    def _parse_layer(file: Path) -> str:
        """
        解析文件名以获取层名称。
        Args:
            file (Path): 文件路径对象。
        Returns:
            str: 提取的层名称，如果无法确定层名称，则返回 "base"。
        """
        parts = file.stem.split(".")
        layer = parts[-1] if len(parts) > 1 and parts[-1] in LAYER_ORDER else "base"
        return layer

    def _init_share_variable(self):
        """
        初始化共享变量。
        Returns:
            无
        """
        if not self._share_data:
            mgr = mp.Manager()
            self._share_data = mgr.dict()
            self._share_event = mp.Event()
            self._share_response = mgr.Queue()
            self._proxy_num = mp.Value("i", 0)

    def register_schema(self, name: str, schema: Type[BaseModel]):
        """
        注册一个模式到内部注册表。
        Args:
            name (str): 模式的名称。
            schema (Type[BaseModel]): 模式的类型，必须是 BaseModel 的子类。
        Returns:
            None
        Raises:
            TypeError: 如果 schema 不是 BaseModel 的子类。
        将指定的模式注册到内部注册表 self._registry_schema 中，
        键为模式名称的小写形式，值为模式的类对象。
        """
        self._registry_schema[name.lower()] = schema

    def setup_logger(self, logger):
        self._logger = logger

    def start_hot_reload(self):
        if self._share_event is None:
            self._init_share_variable()

        if not self._hot_manager:
            # 创建HotReloader实例
            self._hot_manager = HotReloader(self.config_path, self)
            self._hot_manager.bind_share(self._share_data, self._share_event, self._share_response, self._proxy_num,
                                         self._logger)
            self._hot_manager.start()

    def stop_hot_reload(self):
        """
        停止热重载功能。
        Returns:
            无
        """
        if self._hot_manager:
            self._hot_manager.stop()
            self._hot_manager = None

    def register_callback(self, callback: Callable):
        """
        注册主进程回调函数。
        Args:
            callback (Callable): 回调函数，类型为 Callable。
        Returns:
            None
        """
        self._hot_manager.register_callback(callback)

    def register_mp_proxy(self):
        """
        注册多进程模式下的子进程消息代理，用于子进程与主进程热更新模块的全局配置同步。
        Returns:
            无
        Raises:
            无
        """
        self._proxy_num.value += 1
        self.proxy.bind_share(self._share_data, self._share_event, self._share_response)
        self.proxy.start_event_listener()

    def stop_mp_proxy(self):
        """
        停止MP代理的事件监听。
        Returns:
            无
        """
        self.proxy.stop_event_listener()

    def register_proxy_callback(self, callback: Callable):
        """
        注册代理回调函数。
        Args:
            callback (Callable): 回调函数，当代理状态变化时会被调用。
        Returns:
            None
        """
        self.proxy.register_callback(callback)

    @property
    def config(self) -> BaseModel:
        return self.proxy.get_instance()

    @property
    def share_variable(self) -> tuple:
        return self._share_data, self._share_event, self._share_response

    def dict(self) -> dict:
        return self.proxy.dict()

    def json(self) -> dict:
        return self.proxy.json(indent=2, ensure_ascii=False)


def create_schema(cfg_dict: dict, schema: Type[BaseModel]):
    """
    根据配置字典创建指定类型的Schema实例。

    Args:
        cfg_dict (dict): 包含配置信息的字典。
        schema (Type[BaseModel]): 需要创建的Schema类。

    Returns:
        : 根据配置字典创建的Schema实例。

    """
    # 初始化一个空字典用于存储类型化的数据
    typed_data = {}
    # 遍历schema类的类型提示
    for field, field_type in get_type_hints(schema).items():
        # 如果字段在配置字典中存在，则进行类型化处理
        if field in cfg_dict:
            typed_data[field] = normalize_value(cfg_dict[field], field_type, field)
    # 使用类型化的数据实例化schema类
    instance = schema(**typed_data)

    return instance


def load_schema(config_path: Union[Path, str], schema: Type[BaseModel], spec: str = ""):
    """
    加载指定路径下的配置文件，并返回相应的模型类实例。

    Args:
        config_path (Union[Path, str]): 配置文件的路径，可以是Path对象或字符串类型。
        schema (Type[BaseModel]): 需要创建的Schema类。
        spec: 指定读取配置文件中的配置字段
    Returns:
        根据配置文件创建的模型类实例。
    """
    cfg_dict = read_config(config_path)
    cfg_dict = {k.lower(): i for k, i in cfg_dict.items()}

    if spec:
        create_schema(cfg_dict[spec.lower()], schema)
    elif schema.__name__.lower() in cfg_dict:
        return create_schema(cfg_dict[schema.__name__.lower()], schema)
    else:
        return create_schema(cfg_dict[schema.__name__.lower().split("config")[0]], schema)


def read_config(config_path: Union[str, Path]):
    """
    读取配置文件，返回配置内容的字典。

    Args:
        config_path (Union[str, Path]): 配置文件的路径，可以是字符串或Path对象。

    Returns:
        Dict[str, dict]: 包含配置内容的字典，键为模块名（小写），值为包含配置项的字典。

    """
    file = Path(config_path)
    loader = EXT_LOADERS[file.suffix]
    raw_data = loader(file)
    result: Dict[str, dict] = {k.lower(): {i: j for (i, j) in v.items()} for (k, v) in raw_data.items()}
    return result

def load_py_config(file_path: Path) -> dict:
    """
    从指定的Python文件中加载配置，返回一个包含配置项的字典。

    Args:
        file_path (Path): 包含配置项的Python文件路径。

    Returns:
        dict: 包含配置项的字典，键为配置项名称，值为配置项的值。

    Raises:
        FileNotFoundError: 如果指定的文件路径不存在，将引发此异常。
        SyntaxError: 如果指定的Python文件存在语法错误，将引发此异常。

    """
    spec = spec_from_file_location("config_module", str(file_path))
    module = module_from_spec(spec)
    spec.loader.exec_module(module)  # type: ignore
    return {
        key: getattr(module, key) for key in dir(module)
        if not key.startswith("_") and isinstance(getattr(module, key), (str, int, float, bool, dict, list))
    }


def my_optionxform(optionstr):
    return optionstr


def load_ini_config(file_path: Path) -> dict:
    """
    从指定路径的ini配置文件中加载配置信息，并返回为字典形式。

    Args:
        file_path (Path): ini配置文件的路径。

    Returns:
        dict: 包含配置信息的字典，其中键为section名，值为对应section下的配置项及其值组成的字典。

    """
    parser = configparser.ConfigParser()
    parser.optionxform = my_optionxform
    parser.read(file_path, encoding="utf-8")
    result = {}
    for section in parser.sections():
        result[section] = dict(parser.items(section))
    return result


def load_yaml_config(file_path: Path) -> dict:
    """
    从指定的YAML文件中加载配置信息。

    Args:
        file_path (Path): 包含YAML配置文件的路径。

    Returns:
        dict: 包含YAML配置信息的字典。如果文件不存在或解析失败，将返回一个空字典。

    """
    with open(file_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def merge_dicts(base: dict, override: dict) -> dict:
    """
    合并两个字典。

    Args:
        base (dict): 基础字典，将被覆盖的字典。
        override (dict): 覆盖字典，用于覆盖基础字典中对应的键值对。

    Returns:
        dict: 合并后的字典。

    """
    for k, v in override.items():
        if k in base and isinstance(base[k], dict) and isinstance(v, dict):
            base[k] = merge_dicts(base[k], v)
        else:
            base[k] = v
    return base


def normalize_value(value, expected_type, field_name=""):
    try:
        """
    对输入的值进行类型校验和标准化处理。

    Args:
        value (Any): 待校验和标准化的值。
        expected_type (type): 期望的值的类型。
        field_name (str, optional): 字段名称，默认为空字符串。

    Returns:
        Any: 标准化处理后的值。

    Raises:
        ValueError: 如果值的类型不符合期望的类型或转换过程中发生错误，则引发此异常。

    """
        if expected_type == bool:
            if str(value).lower() in ["1", "true"]:
                return True
            elif str(value).lower() in ["0", "false"]:
                return False
            else:
                raise ValueError(f"非法布尔值: {value}")
        elif expected_type == int:
            return int(str(value).replace(",", ""))
        elif expected_type == float:
            return float(str(value).replace(",", ""))
        elif expected_type == str:
            return str(value).strip().replace("%", "%%")
        elif expected_type == dict:
            value = eval(value) if not isinstance(value, dict) else value
            return {k.strip(): v for k, v in value.items()}
        elif expected_type == list:
            return eval(value) if not isinstance(value, list) else value
        else:
            return value
    except Exception as e:
        raise ValueError(f"字段[{field_name}] 类型校验失败: {e}")
