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
import re
import site
from pathlib import Path

# Constants
PATTERN = "@{(.+?)}"
""" Secrets pattern. """


# -----------------------------------------------------------------------------
#
class SecretsManager:
    """ This class expands secrets referenced in ini files.

    A secret is declared as @{<secrets-filename>}.

    All secret errors found in the INI file are collected and can be
    retrieved by calling the I{get_expand_errors()} method.

    The location of the secrets directory varies depending on a platform.

    The following python code snippet will show you where it's located:

        >>> import site
        >>> print(f'{site.USER_BASE}/secrets')


    :ivar error: Validation errors.
    """

    # ----------------------------------------------------------
    #
    def __init__(self):
        """ Initialize class attributes. """
        self.error = []

    # ---------------------------------------------------------
    #
    def _secret_of(self, match_obj: re.Match) -> str:
        """ Return content from in the specified secrets file for matching objects.

        Keep track of all expand errors, so they can be retrieved later. When an
        error occurs, the unexpanded value is returned.

        :param match_obj: Matched pattern object.
        :return: Content of specified secrets file.
        """
        name = match_obj[1]
        secrets_file = Path(f'{site.getuserbase()}/secrets/{name}')

        if secrets_file.exists():
            with secrets_file.open() as hdl:
                return hdl.read().rstrip()

        self.error.append(f'Missing secrets file: {secrets_file}')
        return match_obj[0]

    # ---------------------------------------------------------
    #
    def expand_secrets(self, text: str) -> str:
        """ Return the text with secrets expanded.

        :param text: Expanded text.
        :return: Expanded text.
        """
        return re.sub(PATTERN, self._secret_of, text, re.MULTILINE)

    # ---------------------------------------------------------
    #
    def get_expand_errors(self) -> list:
        """ Return all expanding errors (missing secrets file).

        :return: List of expanding errors
        """
        return self.error
