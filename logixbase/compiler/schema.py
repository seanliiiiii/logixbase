# -*- coding: utf-8 -*-
from pydantic import BaseModel, Field


class CompileConfig(BaseModel):
    path: str = Field(description="项目路径")
    module: str = Field(default="", description="指定项目模块，e.g. logixbase.compiler")
    except_file: list = Field(default=[], description="跳过编译的文件夹/文件，无需包含项目根目录路径")
    pyc_file: list = Field(default=[], description="编译为pyc的文件夹/文件，无需包含项目根目录路径")
    clear_file: list = Field(default=[], description="编译结果中不包含的文件夹/文件，无需包含项目根目录路径")

