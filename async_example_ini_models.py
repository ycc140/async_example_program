# -*- coding: utf-8 -*-
"""
License: Apache 2.0

VERSION INFO::

      $Repo: async_example_program
    $Author: Anders Wiklund
      $Date: 2023-09-28 20:42:35
       $Rev: 1
"""

# BUILTIN modules
from typing import Union, List, Dict, Tuple

# Third party modules
from pydantic import BaseModel, field_validator, FieldValidationInfo

# Tools modules
from tools.log_level import LogLevel

# Constants
NESTED = ['document_types', 'schedules']
""" Nested data types defined in INI file. """


# -----------------------------------------------------------------------------
# Unique for this program.
#
class ConfigModel(BaseModel):
    """ Representation of config section INI file parameters.


    :ivar user: Current user that is logged in.
    :ivar mongo_pwd: Mongo password.
    """
    user: str
    mongo_pwd: str


# -----------------------------------------------------------------------------
# required in every program (but content changes).
#
class IniFileModel(BaseModel):
    """ Representation of ExampleProgram INI file parameters.


    :ivar log_level: Current log level.
    :ivar config: ConfigModel parameters.
    :ivar document_types: Current document types.
    :ivar schedules: Current schedules.
    """
    log_level: LogLevel
    config: ConfigModel
    document_types: Dict[str, list]
    schedules: List[Tuple[str, str]]

    @field_validator(*NESTED, mode='before')
    @classmethod
    def nested_of(cls, value: str, info: FieldValidationInfo) -> Union[dict, list]:
        """ Unpack nested structure string """
        try:
            return eval(value)
        except SyntaxError as why:
            raise ValueError(f'<{info.field_name}> '
                             f'parameter is incorrect => {why.msg}')


# -----------------------------------------------------------------------------
# Unique for this program.
#
class Win32Model(IniFileModel):
    """ Representation of INI parameters with WIN32 platform-specific parameters.


    :ivar in_path: Current win32 in_path.
    :ivar out_path: Current win32 out_path.
    :ivar error_path: Current win32 error_path.
    :ivar offline_path: Current win32 offline_path.
    """
    in_path: str
    out_path: str
    error_path: str
    offline_path: str


# -----------------------------------------------------------------------------
# Unique for this program.
#
class LinuxModel(IniFileModel):
    """ Representation of INI parameters with LINUX platform specific parameters.


    :ivar in_path: Current linux in_path.
    :ivar out_path: Current linux out_path.
    :ivar error_path: Current linux error_path.
    :ivar offline_path: Current linux offline_path.
    """
    in_path: str
    out_path: str
    error_path: str
    offline_path: str
