# -*- coding: utf-8 -*-
"""
License: Apache 2.0

VERSION INFO::

      $Repo: async_example_program
    $Author: Anders Wiklund
      $Date: 2023-10-08 16:03:57
       $Rev: 23
"""

# BUILTIN modules
import os
import re

# Constants
PATTERN = "&{(.+?)}"
""" Environment variable pattern. """


# -----------------------------------------------------------------------------
#
class EnvironmentExpander:
    """ This class expands environment variables referenced in ini files.

    An environment_variable is declared as &{<environment-variable-name>}.

    All environment errors found in the INI file are collected and can be
    retrieved by calling the I{get_expand_errors()} method.

    :ivar error: Validation errors.
    """

    # ----------------------------------------------------------
    #
    def __init__(self):
        """ Initialize class attributes. """
        self.error = []

    # ---------------------------------------------------------
    #
    def _environment_variable_of(self, match_obj: re.Match) -> str:
        """ Return specified environment variable for a matching object.

        Keep track of all expand errors, so they can be retrieved later.
        When an error occurs, the unexpanded value is returned.

        :param match_obj: Matched pattern object.
        :return: Value of environment variable.
        """
        name = match_obj[1]

        if result := os.getenv(name):
            return result

        self.error.append(f'Missing environment variable: {name}')
        return match_obj[0]

    # ---------------------------------------------------------
    #
    def expand_environment_variables(self, text: str) -> str:
        """ Return the text with environment_variables expanded.

        :param text: Expanded text.
        :return: Expanded text.
        """
        return re.sub(PATTERN, self._environment_variable_of, text, re.MULTILINE)

    # ---------------------------------------------------------
    #
    def get_expand_errors(self) -> list:
        """ Return all expanding errors (undefined environment_variables).

        :return: List of expanding errors.
        """
        return self.error
