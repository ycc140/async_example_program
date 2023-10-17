# -*- coding: utf-8 -*-
"""
License: Apache 2.0

VERSION INFO::

      $Repo: async_example_program
    $Author: Anders Wiklund
      $Date: 2023-10-17 02:25:21
       $Rev: 40
"""

# BUILTIN modules
import os
from typing import Callable, Optional, List

# Tools modules
from tools.async_ini_file_parser import (PLATFORM, IniValidationError,
                                         AsyncIniFileParser)

# Local program modules
from async_example_ini_models import ConfigModel, Win32Model, LinuxModel


# -----------------------------------------------------------------------------
#
class AsyncExampleProgIni(AsyncIniFileParser):
    """ This class handles the ini file parameters for the AsyncExampleProgram.


    :ivar initial_validation:
        Need to handle initial validation correctly when post-validation is done.
    """

    # ---------------------------------------------------------
    #
    def __init__(self, name: str):
        """ The class constructor.

        :param name: Name of ini file.
        """
        default_paths = []
        default_params = ['log_level', 'document_types']

        # Initiate the mother object.
        super().__init__(name, default_paths, default_params)

        self.initial_validation = True

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

        return [section]

    # ---------------------------------------------------------
    # Optional, needed when you have platform or section parameters.
    #
    async def validate_ini_file(self, validation_model: Callable,
                                sections: Optional[list] = None,
                                read_ini: bool = False):
        """
        Read ini file and check the existence of required parameters and
        sometimes validate content of complex types.

        Specifically, handle verification of non-DEFAULT section(s).

        The superclass handles verification of DEFAULT parameters and validation
        of all parameters (except special cases for complex types).

        :param validation_model: The validation model that pydantic should use.
        :param sections: List of used section names (except DEFAULT).
        :param read_ini: Read ini file status.
        """

        # This needs to be the first statement (the read_ini
        # input parameter is for the superclass only).
        await self.read_ini_file()

        # Do a basic check that required parameters exist, and that a
        # directory exists when specified for all current platform parameters.
        self._verify_platform_parameters()
        sect = self._verify_config_section_parameters()

        # The superclass verifies the DEFAULT parameters and validates
        # all parameters using pydantic models.
        all_sections = sect
        await super().validate_ini_file(validation_model=validation_model,
                                        sections=all_sections, read_ini=read_ini)

        # Do specific semantic validation for the parameter (after super() validation).
        for path in self.document_types:

            if path.find('\\') != -1:
                self.error.append(f"<routines> key [{path}] is not a posix path")

            elif not os.path.isdir(path):
                self.error.append(f"<routines> key [{path}] does not exist")

        # This needs to be the last block (log semantic post-validation errors).
        if self.error:
            if self.initial_validation:
                raise IniValidationError()
            else:
                await self._handle_error_severity()

        self.initial_validation = False

    # ---------------------------------------------------------
    # Required by every program (but content changes).
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
