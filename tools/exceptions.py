# -*- coding: utf-8 -*-
"""
License: Apache 2.0

VERSION INFO::

      $Repo: async_example_program
    $Author: Anders Wiklund
      $Date: 2023-10-06 09:57:16
       $Rev: 22
"""

# BUILTIN modules
import sys
import time
import inspect
from pathlib import Path
from typing import Optional

# Third party modules
# noinspection PyProtectedMember
from loguru._better_exceptions import ExceptionFormatter

# Tools modules
from tools.configurator import config

# Constants
SUBJECT = {'PROG': 'Program {0} processing failure on {1}',
           'FATAL': 'Program {0} crashed unexpectedly on {1}',
           'DUMP': 'Program {0} processing failed unexpectedly on {1}'}
""" Subject template text based on error state. """
EXTERNAL = {sys.prefix, sys.base_prefix}
""" Root paths to third party and standard python modules (virtual or not). """


# ---------------------------------------------------------
#
def get_exception_context() -> str:
    """ Return Exception context (filename, location and line number).

    Traverse the traceback stack from the last frame backwards to the
    first since that starts closest to the raised exception, and find
    the first frame where the filename isn't found in the standard
    python library or from any of the imported site-packages.

    Then you have found what triggered the exception in our own code.

    :return: Error context ("<filename>[<location>(<lineno>)]").
    """
    # Extract exception traceback frames.
    frames = inspect.getinnerframes(sys.exc_info()[2])

    # Find where the exception occurred in our own code.
    data = next(
        [Path(frame.filename).name, frame.function, frame.lineno]
        for frame in reversed(frames)
        if not any(frame.filename.startswith(path) for path in EXTERNAL)
    )

    return '{}[{}({})]'.format(*data)


# ---------------------------------------------------------
#
def traceback_text_of(error: Exception) -> str:
    """ Return exception traceback as a string.

    :param error: Current exception.
    :return: Exception traceback as a string.
    """
    exc = (type(error), error, error.__traceback__)
    return ''.join(ExceptionFormatter(encoding='utf8').format_exception(*exc))


# ---------------------------------------------------------
#
def has_traceback_frames(error: Exception) -> bool:
    """ Return exception traceback frame status.

    :param error: Current exception.
    :return: Does exception contain traceback frames?.
    """
    return bool(error.__traceback__)


# ---------------------------------------------------------
#
def error_text_of(error: Exception, include_traceback: bool = False,
                  extra: Optional[str] = None) -> str:
    """ Return exception context and error text.

    When *include_traceback=True*, an exception traceback is appended to
    the text.

    When the *extra* parameter is specified, that text is inserted after
    the location and before the reason.

    The logical structure of the error test is::

        <location> [<extra>] <reason> [<BR><traceback>]

    :param error: Current exception.
    :param include_traceback: Include traceback status.
    :param extra: Additional error text.
    :return: Exception context and error text (+ eventual traceback).
    """
    reason = f'{error.args[1]}'
    errmsg = (f'{extra} => {reason}' if extra else f'{reason}')
    traceback = include_traceback and has_traceback_frames(error)
    suffix = (f'{errmsg}\n{traceback_text_of(error)}'
              if traceback else f'{errmsg}')

    return f'{get_exception_context()}: {suffix}'


# ---------------------------------------------------------
#
def error_message_of(error: Exception, program: str, state: str,
                     include_traceback: bool = True) -> dict:
    """ Return a populated ErrorMessage.

    When include_traceback=True, an exception traceback
    is attached to the description.

    :param error: Current exception.
    :param program: Current program.
    :param state: Current error state.
    :param include_traceback: Include traceback status (default is True).
    :return: A populated ErrorMessage.
    """
    now = time.strftime("%Y-%m-%d %X")
    item = [program, config.server, now,
            f'{error_text_of(error, include_traceback)}']
    return {'msgType': 'ErrorMessage',
            'description': "Program {0} on {1} at {2} =>>> {3}".format(*item),
            'summary': SUBJECT[state].format(*item), 'state': state}
