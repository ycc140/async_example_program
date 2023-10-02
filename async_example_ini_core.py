# -*- coding: utf-8 -*-
"""
License: Apache 2.0

VERSION INFO::

      $Repo: async_example_program
    $Author: Anders Wiklund
      $Date: 2023-10-02 10:33:57
       $Rev: 18
"""

# BUILTIN modules
import os
from typing import Callable, Optional, List

# Tools modules
from tools.async_ini_file_parser import (PLATFORM,
                                         AsyncIniFileParser,
                                         IniValidationError)

# Local program modules
from async_example_ini_models import ConfigModel, Win32Model, LinuxModel


# -----------------------------------------------------------------------------
#
class AsyncExampleProgIni(AsyncIniFileParser):
    """ This class handles the ini file parameters for the ExampleProgram. """

    # ---------------------------------------------------------
    #
    def __init__(self, name: str):
        """ The class constructor.

        :param name: Name of ini file.
        """
        default_paths = []
        default_params = ['log_level', 'document_types', 'schedules']

        # Initiate the mother object.
        super().__init__(name, default_paths, default_params)

    # ---------------------------------------------------------
    # Optional, needed when you have platform parameters.
    #
    def _verify_platform_parameters(self):
        """ Verify platform-specific section parameters.

        The verification checks that all parameters exist
        and that specified path exists.
        """
        required_params = ['offline_path', 'in_path', 'out_path', 'error_path']

        for key in required_params:
            try:
                if not self.has_option(PLATFORM, key):
                    self.error.append(f"Missing param '{key}' "
                                      f"in section: '{PLATFORM}'")

                else:
                    path = self.get(PLATFORM, key)

                    if not os.path.isdir(path):
                        self.error.append(f"Section [{PLATFORM}] path "
                                          f"'{key}' does not exist")

            except (ValueError, SyntaxError, OSError):
                self.error.append(f'<{key}> parameter is incorrect')

        # Add parameters to change supervision, if needed.
        if not self.error and self.supervise_change:
            items = self._add_platform_parameters()
            self._handle_status(items)

    # ---------------------------------------------------------
    # Optional, needed when you have section parameters.
    #
    def _verify_config_section_parameters(self) -> List[str]:
        """ Verify that all config section parameters exist.

        :return: Verified section(s).
        """

        section = 'config'
        required_params = list(ConfigModel.model_fields.keys())

        # Make sure that all required default parameters exist.
        for key in required_params:

            if not self.has_option(section, key):
                self.error.append(f"No option '{key}' in section: '{section}'")

        # Add parameters to change supervision, if needed.
        if not self.error and self.supervise_change:
            items = self._add_section_parameters(section)
            self._handle_status(items)

        return [section]

    # ---------------------------------------------------------
    # Optional, needed when you have platform or section parameters.
    #
    async def validate_ini_file(self, validation_model: Callable,
                                sections: Optional[list] = None,
                                read_ini: bool = False):
        """
        Read ini file and check the existence of required parameters and validate
        their types, and somtimes their content.

        Specifically, handle validation of non-DEFAULT section(s).

        :param validation_model: Pydantic BaseModel.
        :param sections: List of used section names (except DEFAULT).
        :param read_ini: Read ini file status.
        """

        # This needs to be the first statement.
        await self.read_ini_file()

        # Do a basic check that required parameters exist, and that a
        # directory exists when specified for all current platform parameters.
        self._verify_platform_parameters()
        sect = self._verify_config_section_parameters()

        # Do the same as above for DEFAULT parameters, validate
        # all parameters with pydantic and log validation errors.
        all_sections = sect
        await super().validate_ini_file(validation_model=validation_model,
                                        sections=all_sections, read_ini=read_ini)

        # Do specific semantic validation for the parameter (after super() validation).
        for path in self.document_types:

            if path.find('\\') != -1:
                self.error.append(f"<routines> key [{path}] is not a posix path")

            elif not os.path.isdir(path):
                self.error.append(f"<routines> key [{path}] does not exist")

        # This needs to be the last block (log semantic validation errors).
        if self.error:
            raise IniValidationError(self.error)

    # ---------------------------------------------------------
    # Required by every program.
    #
    async def validate_ini_file_parameters(self):
        """ Extract and validate ini file parameters. """
        context_model = (Win32Model if PLATFORM == 'win32' else LinuxModel)
        await self.validate_ini_file(validation_model=context_model)

    # ---------------------------------------------------------
    # Optional, but used by most programs.
    #
    @property
    def changed_log_level(self) -> bool:
        """ Return log_level change status.

        :return: Ini file value change status.
        """
        return self.status['log_level'][1]

    # ---------------------------------------------------------
    # Unique for this program.
    #
    @property
    def changed_document_types(self) -> bool:
        """ Return document_types change status.

        :return: Ini file value change status.
        """
        return self.status['document_types'][1]

    # ---------------------------------------------------------
    # Unique for this program.
    #
    @property
    def changed_schedules(self) -> bool:
        """ Return schedules change status.

        :return: Ini file value change status.
        """
        return self.status['schedules'][1]

    # ----------------------------------------------------------
    # Unique for this program.
    #
    @property
    def changed_worker_parameters(self) -> bool:
        """  Return changed worker parameter status.

        These parameters are monitored:
          - schedules
          - document_types

        :return: Ini file value change status.
        """
        return (
                self.status['schedules'][1] or
                self.status['document_types'][1]
        )
