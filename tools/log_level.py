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
from enum import Enum


# -----------------------------------------------------------------------------
#
class LogLevel(str, Enum):
    """ Representation of valid log levels.

    Severity level order (lowest to highest):
      - trace
      - debug
      - info
      - success
      - warning
      - error
      - critical


    :ivar info: Log *message.format(*args, **kwargs)* with severity (20) 'INFO'.
    :ivar trace: Log *message.format(*args, **kwargs)* with severity (05) 'TRACE'.
    :ivar debug: Log *message.format(*args, **kwargs)* with severity (10) 'DEBUG'.
    :ivar error: Log *message.format(*args, **kwargs)* with severity (40) 'ERROR'.
    :ivar success: Log *message.format(*args, **kwargs)* with severity (25) 'SUCCESS'.
    :ivar warning: Log *message.format(*args, **kwargs)* with severity (30) 'WARNING'.
    :ivar critical: Log *message.format(*args, **kwargs)* with severity (50) 'CRITICAL'.
    """

    info = 'info'
    trace = 'trace'
    debug = 'debug'
    error = 'error'
    success = 'success'
    warning = 'warning'
    critical = 'critical'
