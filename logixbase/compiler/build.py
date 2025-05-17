# -*- coding: utf-8 -*-
import os
import shutil
import sys
import py_compile
from typing import Union
from pathlib import Path
from importlib.util import (spec_from_file_location, module_from_spec)
from distutils.core import setup
from Cython.Build import cythonize
from distutils.extension import Extension


from ..configer import read_config, create_schema
from .schema import CompileConfig
from ..protocol import OperationProtocol as OPP


r"""
Require C++ Build Tool 14.0+
Execute in CMD with:
    (python full path) build.py build_ext --inplace
    e.g. D:\env\python39\python.exe build.py build_ext --inplace
.pyd file will be generated in this directory
"""


class BuildProject(OPP):
    """
    Compile all project files to .pyc / .pyd
    """
    def __init__(self, task: str, config_path: Union[Path, str], save_path: Union[Path, str]):

        self._task: str = task
        self._cfg_path: Union[Path, str] = Path(config_path)
        self._save_path: Union[Path, str] = Path(save_path)
        self._project_name: str = ""                        # 项目名称

        cfg = read_config(self._cfg_path).get(self._task.lower(), {})
        if not cfg:
            raise ValueError(f"指定配置文件中无任务配置: {self._task} {self._cfg_path}")
        self.cfg: CompileConfig = create_schema(cfg, CompileConfig)
        self.parse_config()

        self._use_cython: list = []                         # 使用Cython编译的pyd文件
        self._use_nbcc: list = []                           # 使用numba.pycc.CC编译的pyd文件
        self._use_pyc: list = []                            # 编译为.pyc文件的文件夹/文件
        self._skip_file: list = []                          # 被指定跳过的文件夹/文件

    def parse_config(self):
        self._project_name = self.cfg.path.split("\\")[-1]
        if not self.cfg.module:
            self.cfg.module = self._project_name
        self.cfg.except_file = self._parse_all_file(self.cfg.except_file)
        self.cfg.pyc_file = self._parse_all_file(self.cfg.pyc_file)
        self.cfg.clear_file = self._parse_all_file(self.cfg.clear_file)

    def _parse_all_file(self, files: list):
        """"""
        res = []
        for k in files:
            dir_ = Path(self.cfg.path).joinpath(k)

            if dir_.is_dir():
                res.extend([Path(r).joinpath(i) for (r, v, j) in os.walk(dir_) for i in j])
            elif dir_.is_file():
                res.append(dir_)

        return res

    def start(self):
        """Execute project building"""
        # Setup target
        self.setup_ext()
        # Compilation
        self.compile()

    def join(self):
        pass

    def gen_new_dir(self, raw_dir, new_dir):
        """Get new directory with same structure as raw dir"""
        parts = list(Path(str(new_dir).split(self._project_name)[0]).parts)
        structure = self.get_structure(raw_dir)
        new_parts = parts[1:] + list(structure)
        res = Path(parts[0])
        for k in new_parts:
            if k != "\\":
                res = res.joinpath(k)
        return res

    def get_structure(self, raw_dir):
        """Get structure from directory"""
        structure = Path(raw_dir).parts
        structure = structure[structure.index(self._project_name):]
        return structure

    @staticmethod
    def check_cc(file: Path):
        """Check whether to use numba.pycc.CC to compile pyd"""
        spec = spec_from_file_location(file.name.split(".")[0], location=file)
        mod = module_from_spec(spec)
        spec.loader.exec_module(mod)
        return getattr(mod, "cc", None)

    def setup_ext(self):
        """Setup targets"""
        build_to = self._save_path.joinpath(self._project_name)

        for (root, folders, files) in os.walk(self.cfg.path):
            # Skip cache folder
            if Path(root).parts[-1] == "__pycache__":
                continue
            for file in files:
                file_dir = Path(root).joinpath(file)
                new_dir = self.gen_new_dir(file_dir, build_to)
                if os.path.exists(Path(root).joinpath(file.split(".")[0] + ".c")):
                    os.remove(Path(root).joinpath(file.split(".")[0] + ".c"))
                # File not contained from built project
                if file_dir in self.cfg.clear_file:
                    continue
                # File kept but no compilation required
                elif (file_dir in self.cfg.except_file) or (not file.endswith(".py")):
                    self._skip_file.append((file_dir, new_dir))
                # __ini__ files always compiled to pyc
                elif (file == "__init__.py") or (file_dir in self.cfg.pyc_file):
                    new_dir = Path(str(new_dir).replace(".py", ".pyc"))
                    self._use_pyc.append((file_dir, new_dir))
                # Compile pyd files
                else:
                    cc = self.check_cc(file_dir)
                    # Numba file, use numba.pycc.CC to compile pyd
                    if cc is not None:
                        cc.raw_output_dir = str(cc.output_dir)
                        cc.output_dir = str(new_dir.parent)
                        self._use_nbcc.append(cc)
                    # Non-numba file, use Cython to compile pyd
                    else:
                        structure = self.get_structure(str(file_dir).replace(".py", ""))
                        mod = ".".join(self.cfg.module.split(".") + list(structure[1:]))
                        # mod = ".".join(structure)
                        # mod = structure[-1]
                        ext = Extension(name=mod,
                                        sources=[str(file_dir)],
                                        )
                        ext.output_dir = str(new_dir.parent)
                        self._use_cython.append(ext)
        print(f"编译任务准备就绪: Cython {len(self._use_cython)} | Numba.pycc {len(self._use_nbcc)} |"
              f" Pyc {len(self._use_pyc)}")

    def compile(self):
        """Compile .py to .pyd / .pyc"""
        # Clear latest built project
        root = self._save_path.joinpath(self._project_name)
        if os.path.exists(root):
            shutil.rmtree(root)
        # Compile Cython pyd
        print("========================")
        print(f"执行Cython编译任务：总计 {len(self._use_cython)}...")
        for ext in self._use_cython:
            source_ = Path(ext.sources[0])
            sys.path.append(str(source_.parent))
            # Compile
            options = {"script_args": ["build_ext", f"--build-lib={ext.output_dir}"],
                       "ext_modules": cythonize(ext)
                       }
            setup(**options)
            sys.path.remove(str(source_.parent))
            # Clear .c file
            c_path = source_.parent.joinpath(source_.name.replace(".py", ".c"))
            if os.path.exists(c_path):
                os.remove(c_path)
            # Rename file and move to right structure
            now_dir = Path(ext.output_dir)
            for k in ext.name.split(".")[:-1]:
                now_dir = now_dir.joinpath(k)
            files = os.listdir(now_dir)
            for file in files:
                ele = file.split(".")
                new_name = ele[0] + "." + ele[-1]
                file_now = Path(now_dir).joinpath(new_name)
                os.rename(Path(now_dir).joinpath(file), file_now)
                shutil.move(file_now, Path(ext.output_dir).joinpath(new_name))
            print(f"Cython编译任务执行完毕: {ext.sources[0]}")
        # Compile numba pyd
        print("========================")
        print(f"执行Numba.pycc编译任务：总计 {len(self._use_nbcc)}...")
        for cc in self._use_nbcc:
            print(f"子任务: {cc.name}")
            cc_dir = str(cc.raw_output_dir)
            sys.path.append(cc_dir)
            cc.compile()
            sys.path.remove(cc_dir)
            print(f"子任务完成: {cc.output_dir}")

        print("========================")
        print(f"执行Pyc编译任务：{len(self._use_pyc)}...")
        # Execute compiling pyc
        for file in self._use_pyc:
            py_compile.compile(file[0], file[1])
            print(f"子任务完成: {file}")

    def stop(self):
        """清理被编译项目中的临时文件"""
        build_dir = Path(os.getcwd()).joinpath("build")
        if os.path.exists(build_dir):
            shutil.rmtree(build_dir)
        print("已清理build文件夹")
        # Remove .c files
        for (root, folder, files) in os.walk(self.cfg.path):
            for file in files:
                if file.endswith(".c"):
                    c_path = Path(root).joinpath(file)
                    os.remove(c_path)
        print("已清理.c文件")
        # Remove redundant name tag
        root = self._save_path.joinpath(self._project_name)
        for (k, v, j) in os.walk(root):
            for i in j:
                raw = Path(k).joinpath(i)
                ele = i.split(".")
                file = Path(k).joinpath(ele[0] + "." + ele[-1])
                os.rename(raw, file)
        print("已清理临时文件")
        # Copy no-compile files
        for (file, new_file) in self._skip_file:
            if not os.path.exists(new_file.parent):
                os.makedirs(new_file.parent, exist_ok=True)
            shutil.copyfile(file, new_file)
        print("已复制无需编译文件")
        # Remove cache files
        for (r, dirs, f) in os.walk(root, topdown=False):
            for k in dirs:
                if k == "__pycache__":
                    shutil.rmtree(Path(r).joinpath(k))
            if not f:
                shutil.rmtree(r)
        print("已清理缓存文件")
