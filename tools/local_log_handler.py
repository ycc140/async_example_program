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
import site
import logging
import inspect
from os import getenv
from pathlib import Path
from typing import Optional

# Third party modules
from loguru import logger

# Constants.
LEVELS = {'trace': '###', 'debug': '%%%', 'info': '---',
          'success': '+++', 'warning': '++*', 'error': '***', 'critical': '!!!'}
""" Log level convert mappings for a more compact log line. """
ENV: str = getenv('ENVIRONMENT', 'dev').lower()
""" Get server defined environment. """


# -------------------------------------------------------------
#
def path_of(program: Optional[Path] = None) -> Path:
    """ Return a log path depending on a static path or current_path.

    If *program* parameter is not supplied, then the log file will
    be created in the current program execution context.

    Otherwise, the location is platform dependent.

    The following python code snippet will show you where it's located:

        >>> import site
        >>> print(f'{site.USER_BASE}/log')

    :param program: Program name
    :return: Logging path.
    """

    if program:
        log_path = Path(f'{site.getuserbase()}') / 'log' / f'{program.stem}'

        # Create a logging path if needed.
        log_path.mkdir(parents=True, exist_ok=True)

    else:
        log_path = Path.cwd()

    return log_path


# ---------------------------------------------------------
#
def lean_color_formatter(record: dict) -> str:
    # noinspection StructuralWrap
    """ Return lean colored logging format string with special log level.

        The following log levels (in prio order) are used::

          - '###' => trace    (cyan text)
          - '%%%' => debug    (blue text)
          - '---' => info     (white text)
          - '+++' => success  (green text)
          - '++*' => warning  (yellow text)
          - '***' => error    (red text)
          - '!!!' => critical (white text on a red background).

        :param record: Logging record.
        :return: Lean and colored logging format.
        """
    level = record["level"].name.lower()
    return ('<level>{time:YYYY-MM-DD HH:mm:ss.SSS} ' + LEVELS[level] +
            ' [{extra[program]}] {message}</level>\n{exception}')


# -----------------------------------------------------------------------------
#
class InterceptHandler(logging.Handler):
    """ Send logs to loguru logger from Python logging module. """

    def emit(self, record: logging.LogRecord) -> None:
        """  Move the specified logging record to loguru.

        :param record: Original python log record.
        """
        # Get corresponding Loguru level if it exists.
        level: str | int
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        # Find caller from where originated the logged message.
        frame, depth = inspect.currentframe(), 0
        while frame and (depth == 0 or frame.f_code.co_filename == logging.__file__):
            frame = frame.f_back
            depth += 1

        logger.opt(depth=depth, exception=record.exc_info).log(level, record.getMessage())


# -----------------------------------------------------------------------------
#
class DynamicLogFilter:
    """ This class handles dynamic log level filtering.

    The initial level is set to ERROR so that it suppresses all "informational"
    log level entries that arrives before our own application is stated by
    calling the *LogHandler start()* method.

    :ivar level: Current logging level.
    """
    level = 'ERROR'

    # ---------------------------------------------------------
    #
    def __call__(self, record: dict) -> bool:
        """ Return current log level display status.

        :param record: Logger record.
        :return: log level display status.
        """
        level_no = logger.level(self.level).no
        return record["level"].no >= level_no


# -----------------------------------------------------------------------------
#
class LogHandler:
    """ This class handles enhanced console and rotating file in loguru logging.

    The following functionality is implemented:
      - thread and multiprocessing safe.
      - Rotating log files are based on date.
      - Console output is colored based on log level.


    :ivar log_filter: DynamicLogFilter object.
    :type log_filter: `DynamicLogFilter`
    """

    # ---------------------------------------------------------
    #
    def __init__(
            self, log_path: Path, program: str, lean_format: bool = True,
            include_external: bool = False, supress_console: bool = False
    ):
        """ Add console and rotating log file handler into Loguru logging.

        Setting *supress_console=True* is used when building a Windows
        service or when a Linux program is managed by systemctl.

        :param log_path: Logfile path.
        :param program: Name of logging program.
        :param lean_format: Use a lean log format (default is True).
        :param include_external: Include python standard logging into Loguru
            (default is False).
        :param supress_console: Suppress console logging (default is False).
        """
        self.log_filter = DynamicLogFilter()

        # Remove default console logger.
        logger.remove()

        # Caution, "diagnose=True" is the default and may leak sensitive data in prod
        diag = ENV != 'prod'

        # Always create a new and enhanced rotating file logger.
        conf = {"extra": {"program": program},
                "handlers": [{'backtrace': True, "enqueue": True,
                              "filter": self.log_filter, "level": 0,
                              "rotation": '00:00:00', 'diagnose': diag,
                              "sink": f'{log_path}/' + '{time:YYYY-MM-DD}.log'}]}

        # Define and create an enhanced console logger, when needed.
        if not supress_console:
            console_item = {'backtrace': True, "enqueue": True,
                            "sink": sys.stderr, 'diagnose': diag,
                            "filter": self.log_filter, "level": 0}
            conf['handlers'].append(console_item)

        if lean_format:
            for item in conf['handlers']:
                item['format'] = lean_color_formatter

        # Configure all selected handlers.
        logger.configure(**conf)

        # Incorporate python standard logging into Loguru.
        if include_external:
            logging.basicConfig(handlers=[InterceptHandler()], level=0, force=True)

            # Dampen chatty python standard logging module.
            logging.getLogger('apscheduler').setLevel('ERROR')

    # ---------------------------------------------------------
    #
    def update_loglevel(self, log_level: str):
        """ Update logging level filter.

        Possible log_level values are:
          - *trace*,
          - *debug*,
          - *info*,
          - *success*,
          - *warning*,
          - *error*,
          - *critical*.

        :param log_level: Name of applied log level.
        """
        self.log_filter.level = log_level.upper()
        logger.success('Updated log level is [{name}]', name=log_level)

    # ---------------------------------------------------------
    #
    def start(self, log_level: str, suppress: bool = False):
        """ Set initial log level filter.

        Possible log_level values are:
          - *trace*,
          - *debug*,
          - *info*,
          - *success*,
          - *warning*,
          - *error*,
          - *critical*.

        :param log_level: Name of log level.
        :param suppress: Suppress an initial log message (default id False).
        """
        self.log_filter.level = log_level.upper()

        if not suppress:
            logger.success('Initial log level is [{name}]', name=log_level)
