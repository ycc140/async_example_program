# -*- coding: utf-8 -*-
"""
License: Apache 2.0

VERSION INFO::

      $Repo: async_example_program
    $Author: Anders Wiklund
      $Date: 2023-10-09 18:52:05
       $Rev: 24
"""

# BUILTIN modules
import os
import sys
import shutil
from pathlib import Path
from typing import Optional, Callable, Any
from configparser import ConfigParser, ExtendedInterpolation, NoOptionError

# Third party modules
import aiofiles
from pydantic import ValidationError

# Local modules
from .secrets_expander import SecretsExpander
from .environment_expander import EnvironmentExpander

# Constants
DEFAULT = 'DEFAULT'
""" Defaults section name. """
PLATFORM = sys.platform
""" Current platform. """


# -----------------------------------------------------------------------------
#
class IniValidationError(Exception):
    """
    This exception will be raised when an error occurs
    during validation of the INI file.
    """
    pass


# -----------------------------------------------------------------------------
#
class AsyncIniFileParser(ConfigParser):
    """ This class handles a program ini file.

    It keeps track of changed parameter values during program execution,
    if needed. Environment variables and secrets are expanded. Parameter
    values are accessed using dot notation.

    If INI file validation fails during program startup, the program stops.

    If an INI file change is detected when the program is running and the
    validation fails, the content is rejected and the old file content is
    restored, whilst the program keeps running.

    .. Note::
        You should be aware of the consequences of changing values while the
        program is running. For example, changing path parameters is mostly
        a bad idea since the input path of this program might be the output
        path of another program. So changing file paths usually require a
        coordinated effort that results in changes to several INI files,
        while the programs are offline.

        This goes for connection parameters as well. The program must have been
        programmed to handle disconnecting from the current connection and create
        a new connection using the new parameters, otherwise the change will have
        no effect.

    Expanded parameter ini file syntax:
      - @{<secrets-name>}
      - &{<environment-variable-name>}
      - ${<extended-interpolation-reference>}


    :ivar filename: Name if ini file.
    :type filename: `str`
    :ivar orig_file_copied: Is original ini file copied status.
    :type orig_file_copied: `bool`
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
        self.orig_file_copied = False
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
    async def _restore_orig_file(self):
        """ Restore original ini file since validation failed.

        Save faulty file with a *.err* extension.
        """
        shutil.copyfile(self.filename, f'{self.filename}.err')
        shutil.copyfile(f'{self.filename}.orig', self.filename)

        # Update timestamp so that the error is only detected once.
        self.file_timestamp = os.path.getmtime(self.filename)

    # ----------------------------------------------------------
    #
    def _add_default_parameters(self) -> dict:
        """ Return DEFAULT parameters.

        This method exists, so it can be overridden in derived
        classes when needed (for example, when using vars).

        :return: DEFAULT parameters.
        """
        return self.defaults()

    # ----------------------------------------------------------
    #
    def _add_platform_parameters(self) -> dict:
        """ Return DEFAULT and platform parameters.

        This method exists, so it can be overridden in derived
        classes when needed (for example, when using vars).

        :return: DEFAULT and platform parameters.
        """
        return dict(self.items(PLATFORM))

    # ----------------------------------------------------------
    #
    def _add_section_parameters(self, section: str) -> dict:
        """ Return specified section parameters.

        This method exists, so it can be overridden in derived
        classes when needed (for example, when using vars).

        :param section: Section name.
        :return: Section parameters.
        """
        # noinspection PyProtectedMember
        return self.items()._mapping._sections[section]

    # ----------------------------------------------------------
    #
    def _update_change_status(self, sections: Optional[list] = None) -> dict:
        """ Update parameter supervision change status.

        :param sections: List of section names.
        """
        items = {}
        params = self.valid_params.model_dump()

        # The status structure is flat, so section parameters need
        # an added section head.
        if sections:
            for section in sections:
                items = {f'{section}.{key}': value
                         for key, value in params[section].items()}
                del params[section]

        items |= params
        self._handle_status(items)

    # ----------------------------------------------------------
    #
    async def read_ini_file(self):
        """ Read the ini file and parse existing parameters.

        Expand environment variables and secrets before parsing starts.
        """

        # Make a copy of the original file first so that it's
        # possible to recover from a bad change later on.
        if not self.orig_file_copied:
            shutil.copyfile(self.filename, f'{self.filename}.orig')
            self.orig_file_copied = True

        # Extract ini file content.
        async with aiofiles.open(self.filename, mode='r', encoding='utf-8') as hdl:
            cfg_txt = await hdl.read()

        # Expand all environment variables and retrieve possible expanding errors.
        obj = EnvironmentExpander()
        cfg_txt = obj.expand_environment_variables(cfg_txt)
        self.error.extend(obj.get_expand_errors())

        # Expand secrets and retrieve possible expanding errors.
        obj = SecretsExpander()
        cfg_txt = obj.expand_secrets(cfg_txt)
        self.error.extend(obj.get_expand_errors())

        # Parse the expanded ini file string.
        self.read_string(cfg_txt)

    # ----------------------------------------------------------
    #
    async def _handle_error_severity(self):
        """ Handle validation error severity.

        When you get a validation error during program startup, there's no
        valid original file that you can restore, so the consequence is to
        trigger a fatal stop by raising a *IniValidationError* exception.

        When the program is running (after starting with a valid INI file), and
        a faulty INI file change happens, we can restore the valid original INI
        file and keep the program running after reporting the error by raising
        a *RuntimeError* exception.

        :raise IniValidationError:
            When invalid ini file detected during startup.
        :raise RuntimeError:
            When invalid ini file detected during running program.
        """
        await self._restore_orig_file()

        if not self.valid_params:
            raise IniValidationError(self.error)
        else:
            raise RuntimeError(self.error)

    # ----------------------------------------------------------
    #
    async def _validate_params(self, validation_model: Callable, sections: list):
        """ Validate parameters against a pydantic validation model.

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
            pending_params = validation_model(**params)

            # When we end up here, file content was ok, so let's use it.
            self.valid_params = pending_params
            self.file_timestamp = os.path.getmtime(self.filename)

        except (ValueError, ValidationError) as why:
            self.error.append(f'{why}')
            await self._handle_error_severity()

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
            await self._handle_error_severity()

        await self._validate_params(validation_model, sections)

        # Add all parameters to change supervision, if needed.
        if self.supervise_change:
            self._update_change_status(sections)

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

    # ---------------------------------------------------------
    #
    async def start(self):
        """ Start the used resources in a controlled way. """
        await self.validate_ini_file_parameters()

    # ---------------------------------------------------------
    #
    async def stop(self):
        """ Stop the used resources in a controlled way. """
        removable = Path(f'{self.filename}.orig')

        if removable.exists():
            removable.unlink()
