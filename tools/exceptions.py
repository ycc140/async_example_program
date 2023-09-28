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
import time
import inspect
import traceback
from pathlib import Path

# Tools modules
from tools.configurator import config

# Constants
SUBJECT = {'PROG': 'Program {0} processing failure on {1}',
           'FATAL': 'Program {0} crashed unexpectedly on {1}',
           'DUMP': 'Program {0} processing failed unexpectedly on {1}'}
""" Subject template text based on error state. """
EXTERNAL = {sys.prefix, sys.base_prefix}
""" Root paths to third party, and standard python modules (virtual or not). """


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
    return ''.join(traceback.format_exception(error))


# ---------------------------------------------------------
#
def error_text_of(error: Exception, include_traceback: bool) -> str:
    """ Return exception context and error text.

    When include_traceback=True, an exception traceback is attached to the text.

    :param error: Current exception.
    :param include_traceback: Include traceback status.
    :return: Exception context and error text (+ eventual traceback).
    """
    errmsg = (f'{traceback_text_of(error)}' if include_traceback else f'{error}')
    return f'{get_exception_context()}: {errmsg}'


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
    item = [program, config.server, now, f'{error_text_of(error, include_traceback)}']
    return {'msgType': 'ErrorMessage',
            'description': "Program {0} on {1} at {2} =>>> {3}".format(*item),
            'summary': SUBJECT[state].format(*item), 'state': state}
