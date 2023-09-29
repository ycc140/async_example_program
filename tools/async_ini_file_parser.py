# -*- coding: utf-8 -*-
"""
License: Apache 2.0

VERSION INFO::

      $Repo: async_example_program
    $Author: Anders Wiklund
      $Date: 2023-09-29 02:09:35
       $Rev: 6
"""

# BUILTIN modules
import os
import sys
from pathlib import Path
from typing import Optional, Callable, Any
from configparser import ConfigParser, ExtendedInterpolation, NoOptionError

# Third party modules
import aiofiles

# Local modules
from .secrets_manager import SecretsManager
from .environment_manager import EnvironmentManager

# Constants
DEFAULT = 'DEFAULT'
""" Defaults section name. """
PLATFORM = sys.platform
""" Current platform. """


# -----------------------------------------------------------------------------
#
class AsyncIniFileParser(ConfigParser):
    """ This class handles a program ini file.

    It keeps track of changed parameter values during program execution,
    if needed. Environment variables and secrets are expanded. Parameter
    values are accessed using dot notation.

    Expanded parameter ini file syntax:
      - @{<secrets-name>}
      - &{<environment-variable-name>}


    :ivar filename: Name if ini file.
    :type filename: `str`
    :ivar default_paths: Default paths defined.
    :type default_paths: `list`
    :ivar default_params: Default parameters defined.
    :type default_params: `list`
    :ivar supervise_change:
        Supervise parameter change when the program is running.
    :type supervise_change: `bool`
    :ivar error: Validation errors.
    :type error: `list`
    :ivar status: Parameter change status when supervision is active.
    :type status: `dict`
    :ivar valid_params: Validated INI file parameters.
    :type valid_params: ``pydantic.BaseModel``
    :ivar file_timestamp: Timestamp for last ini file modification.
    :type file_timestamp: `float`
    """

    # ----------------------------------------------------------
    #
    def __init__(self, name: Path, default_paths: list,
                 default_params: list, supervise_change: bool = True):
        """ Initialize IniFileHandler class attributes.

        :param name: Name of ini file.
        :param default_paths: Default paths.
        :param default_params: Default parameters.
        :param supervise_change: Supervise parameter change (default is True).
        """
        super().__init__(interpolation=ExtendedInterpolation())

        # Input parameters.
        self.filename = f'{name}'
        self.default_paths = default_paths
        self.default_params = default_params
        self.supervise_change = supervise_change

        # Unique parameters.
        self.error = []
        self.status = {}
        self.valid_params = None
        self.file_timestamp = os.path.getmtime(self.filename)

    # ---------------------------------------------------------
    #
    def __getattr__(self, name: str) -> Any:
        """ Dynamically return the value for the specified parameter.

        :param name: Parameter name.
        :return: Parameter value.
        """
        return getattr(self.valid_params, name)

    # ----------------------------------------------------------
    #
    def _handle_status(self, params: dict):
        """ Update parameter change status.

        params structure:
          {'<param name>':[<value>, False], ...}

        :param params: All ini parameters
        """

        for key in params:
            self.status[key] = [
                params[key], params[key] != self.status[key][0]
            ] if self.status.get(key) else [params[key], False]

    # ----------------------------------------------------------
    #
    def _add_default_parameters(self) -> dict:
        """ Return DEFAULT parameters.

        This method exists, so it can be overridden in derived
        classes when the need arises.

        :return: DEFAULT parameters.
        """
        return dict(self.items(DEFAULT))

    # ----------------------------------------------------------
    #
    def _add_platform_parameters(self) -> dict:
        """ Return DEFAULT and platform parameters.

        This method exists, so it can be overridden in derived
        classes when the need arises.

        :return: DEFAULT and platform parameters.
        """
        return dict(self.items(PLATFORM))

    # ----------------------------------------------------------
    #
    def _add_section_parameters(self, section: str) -> dict:
        """ Return specified section parameters.

        This method exists, so it can be overridden in derived
        classes when the need arises.

        :param section: Section name.
        :return: Section parameters.
        """
        return dict(self.items(section))

    # ----------------------------------------------------------
    #
    def _validate_params(self, validation_model: Callable, sections: list):
        """ Validate parameters against pydantic validation model.

        :param validation_model: Pydantic BaseModel.
        :param sections: List of used section names (except DEFAULT).
        """
        try:
            # Append specific platform parameters into the DEFAULT section.
            if self.has_section(PLATFORM):
                params = self._add_platform_parameters()

            # Add DEFAULT parameters since no platform parameters exist.
            else:
                params = self._add_default_parameters()

            # Add specified section parameters (using section structure).
            if sections:
                for section in sections:
                    params[section] = self._add_section_parameters(section)

            # Do a pydantic parameter type and structure validation.
            self.valid_params = validation_model(**params)

        except ValueError as why:
            self.error.append(f'{why}')
            raise RuntimeError(self.error)

    # ----------------------------------------------------------
    #
    async def read_ini_file(self):
        """ Read the ini file and parse existing parameters.

        Expand environment variables and secrets before parsing starts.
        """

        # Extract ini file content.
        async with aiofiles.open(self.filename, mode='r', encoding='utf-8') as hdl:
            cfg_txt = await hdl.read()

        # Expand all environment variables and retrieve possible expanding errors.
        obj = EnvironmentManager()
        cfg_txt = obj.expand_environment_variables(cfg_txt)
        self.error.extend(obj.get_expand_errors())

        # Expand secrets and retrieve possible expanding errors.
        secrets = SecretsManager()
        cfg_txt = secrets.expand_secrets(cfg_txt)
        self.error.extend(secrets.get_expand_errors())

        # Parse the expanded ini file string.
        self.read_string(cfg_txt)

    # ----------------------------------------------------------
    #
    async def validate_ini_file(self, validation_model: Callable,
                                sections: Optional[list] = None,
                                read_ini: bool = True):
        """
        Read ini file, check existence of required parameters and validate
        their types.

        Update parameter change status when the value is changed, if required.


        :param validation_model: Pydantic BaseModel.
        :param sections: List of used section names (except DEFAULT).
        :param read_ini: Read ini file (default is True).
        :raise RuntimeError: When missing or corrupt, Ini parameters are detected.
        """

        if read_ini:
            await self.read_ini_file()

        required_params = self.default_params + self.default_paths

        # Make sure that all required default parameters exist.
        for key in required_params:

            if not self.has_option(DEFAULT, key):
                self.error.append(f"Missing option '{key}' in section: '{DEFAULT}'")

        # Make sure that all required default path parameters exist in the file system.
        for key in self.default_paths:
            try:
                path = self.get(DEFAULT, key)

                if not os.path.isdir(path):
                    self.error.append(f"Section [{DEFAULT}] path '{key}' does not exist")

            except (ValueError, SyntaxError, EnvironmentError, NoOptionError):
                self.error.append(f'<{key}> parameter is incorrect')

        if self.error:
            raise RuntimeError(self.error)

        self._validate_params(validation_model, sections)

        # Add parameters to change supervision, if needed.
        if self.supervise_change:
            items = self._add_default_parameters()
            self._handle_status(items)

    # ----------------------------------------------------------
    #
    @property
    def file_is_changed(self) -> bool:
        """ Return INI file change status since the last check.

        :return: Ini file change status.
        """
        check_time = os.path.getmtime(self.filename)

        if changed := check_time > self.file_timestamp:
            self.file_timestamp = check_time

        return changed
