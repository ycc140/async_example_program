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
import sys
import subprocess
from pathlib import Path
from ctypes import windll

# Constants
OPEN_EXISTING = 3
""" Open existing flag. """
GENERIC_WRITE = 1 << 30
""" Generic write flag. """
FILE_SHARE_READ = 0x00000001
""" File share read flag. """
FILE_ATTRIBUTE_NORMAL = 0x80
""" File attribute normal flag. """


# ---------------------------------------------------------
#
def check_for_command(command: str) -> bool:
    """ Return command file existence in the current program path.

    If the command file is found it's deleted.

    :param command: Command to check for.
    :return: Result of command check.
    """
    filename = Path(sys.argv[0]).parent / command

    if result := filename.is_file():
        filename.unlink()

    return result


# ---------------------------------------------------------
#
def is_file_available(filename: str) -> bool:
    """ Return file availability status.

    When the file does not exist, it is opened or used by another process,
    it's not available. The detection is handled using the local operating
    system primitives.

    It is currently implemented for the following platforms:
      - Linux (using lsof) platforms.
      - Windows (using Kernel32 CreateFileW) platforms.

    :param filename: Current file (with a path).
    :return: File availability status.
    """
    result = False

    try:
        filename = Path(filename)

        if not filename.exists():
            result = False

        elif sys.platform == 'win32':
            hdl = windll.Kernel32.CreateFileW(str(filename), GENERIC_WRITE, FILE_SHARE_READ,
                                              None, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, None)

            if hdl != -1:
                windll.Kernel32.CloseHandle(hdl)
                result = True

        elif sys.platform == 'linux':

            # Find open files in specified directory.
            output = subprocess.Popen(f"lsof -w +d '{filename.parent}'",
                                      stdout=subprocess.PIPE, shell=True)

            # Raises a CalledProcessError exception when
            # the file is NOT found among the opened files.
            subprocess.check_output(["grep", f'{filename}'],
                                    stdin=output.stdout, shell=False)

    except subprocess.CalledProcessError:
        result = True

    return result
