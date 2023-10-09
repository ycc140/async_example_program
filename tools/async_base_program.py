#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
License: Apache 2.0

VERSION INFO::

      $Repo: async_example_program
    $Author: Anders Wiklund
      $Date: 2023-10-09 19:14:55
       $Rev: 26
"""

# BUILTIN modules
import sys
import json
import asyncio
from pathlib import Path
from abc import abstractmethod

# Third party modules
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# Tools modules
from tools import exceptions
from tools.configurator import config
from tools.async_ini_file_parser import IniValidationError
from tools.local_log_handler import path_of, LogHandler, logger


# ---------------------------------------------------------------------
#
class AsyncBaseProgram:
    """
    This generic base program class is intended to be used as a building block
    through inheritance when you are creating a server program.

    Any methods that you need to change, you override it using polymorphism.

    RabbitMQ is used for sending messages to external programs. Messages that
    are to be sent to the RabbitMQ will be stored offline if the communication
    goes down and will be re-sent when the communication is re-established.

    Subscribe temporarily for the following RabbitMQ message topic(s):
      - Health.Request

    Sends RabbitMQ messages with the following topic(s):
      - Error.Message.AsyncExampleProgram.<server>
      - Health.Response.AsyncExampleProgram.<server>


    :ivar ini: Handles INI file parameters for this program.
    :type ini: `tools.async_ini_file_parser.AsyncIniFileParser`
    :ivar error: Program exit error status.
    :type error: `bool`
    :ivar future:
        Asyncio Future handle (used for handling fatal error program exit).
    :type future: `asyncio.Future`
    :ivar worker:  Handles the bulk of the work for this program.
    :type worker: `tools.async_base_worker.AsyncBaseWorker`
    :ivar program: Current program name, used for logging.
    :type program: `str`
    :ivar location: Current program name, including the path.
    :type location: L{Path}
    :ivar scheduler: Handles periodic ini file change checks.
    :type scheduler: ``apscheduler.schedulers.asyncio.AsyncIOScheduler``
    :ivar log: Handle colored and dynamic filter logging.
    :type log: `LogHandler`
    """

    # ---------------------------------------------------------
    #
    def __init__(self):
        """ The class constructor. """

        # Root parameters (used in all programs).
        context = Path(sys.argv[0])
        program = self.__class__.__name__

        # General parameters (used in all programs).
        self.ini = None
        self.error = False
        self.future = None
        self.worker = None
        self.program = program
        self.location = context
        self.scheduler = AsyncIOScheduler()
        self.log = LogHandler(program=program,
                              log_path=path_of(context))

    # ---------------------------------------------------------
    # Required in every program.
    #
    async def _fatal_error_dump(self, error: Exception,
                                suppress: bool = False):
        """Report error and trigger a fatal program exit.

        When Suppress=True, no ErrorMessage is sent to RabbitMQ
        and an exception traceback is not logged.

        :param error: Current exception.
        :param suppress: Suppress traceback dump and sending ErrorMessage.
        """
        self.error = True
        errmsg = exceptions.error_text_of(error)

        if suppress:
            logger.error('{err}', err=errmsg)

        else:
            logger.opt(exception=error).error(f'{errmsg} => ')

            # Trigger sending error message.
            msg = exceptions.error_message_of(error, self.program, 'FATAL')
            await self.worker.notify(msg)

    # ---------------------------------------------------------
    #
    async def _handle_ini_error(self, fatal: bool):
        """ Trigger a fatal program exit when errors found and *fatal==True*.

        :param fatal: Is the error fatal or not?
        """

        logger.error('Ini file configuration errors:')

        for errmsg in self.ini.error:
            logger.error('  - {err}', err=errmsg)

        if fatal and self.future:
            self.error = True
            self.future.cancel()

    # ---------------------------------------------------------
    # Optional, only needed if the worker needs to be notified
    # of INI parameter changes.
    #
    async def _schedule_unique_ini_check(self):
        """ Send INI file update notification to the worker.

        If you need it, override it using normal polymorphism.
        """
        pass

    # ---------------------------------------------------------
    # required in every program (content changes depending on
    # if worker needs to be notified of INI parameter changes).
    #
    async def _schedule_ini_check(self):
        """ Extract and validate INI file content if the Ini file is updated.

        This method is called by the AsyncIOScheduler every 5 seconds.

        A RuntimeError is raised if the validation of the changed INI file
        content fails, otherwise the worker is notified if the relevant
        parameter has changed.

        :raise RuntimeError: When INI file validation fails.
        """

        if self.ini.file_is_changed:
            try:
                await self.ini.validate_ini_file_parameters()

                if self.ini.changed_log_level:
                    level_name = self.ini.log_level
                    self.log.update_loglevel(level_name)

                await self._schedule_unique_ini_check()

            # Handle INI file errors during execution (not fatal).
            except RuntimeError:
                await self._handle_ini_error(fatal=False)

    # ---------------------------------------------------------
    # Required in every program.
    #
    @abstractmethod
    async def _initiate_unique_resources(self):
        """ Initiate unique resources used by the program.

        Abstract method that needs to be overridden.

        The following actions are performed:
          - Validate INI file content.
          - Initiate current worker.
        """

    # ---------------------------------------------------------
    # Required in every program.
    #
    async def _initiate_resources(self):
        """ Initiate resources used by the program.

        The following actions are performed:
          - Validate INI file content.
          - Initiate and start worker processing.
          - Start Ini file change supervision (every 5 seconds).
          - Setup waits for program termination.
        """
        await self._initiate_unique_resources()

        self.log.start(self.ini.log_level)
        logger.success('Starting server on {name}...', name=config.server)

        # Start looking for INI file changes.
        self.scheduler.add_job(func=self._schedule_ini_check,
                               trigger='interval', seconds=5)
        self.scheduler.start()

        # Setup wait for program termination, that is triggered
        # either by the operator or by a fatal error occurs.
        event_loop = asyncio.get_event_loop()
        self.future = event_loop.create_future()

    # ---------------------------------------------------------
    # Demo purposes only.
    #
    def _demo_purposes_only(self):
        """ This method is only used for demo.

        It should be removed in normal code usage.
        """

        # Shows valid content of INI file.
        if self.log.log_filter.level == 'TRACE':
            data = json.dumps(indent=4, sort_keys=True,
                              obj=self.ini.valid_params.model_dump())
            logger.trace('INI file content:\n{show}', show=data)
            dta = json.dumps(indent=4, sort_keys=True,
                             obj=config.model_dump())
            logger.trace('config content:\n{show}', show=dta)

        # Shows how to reference a INI file config section parameter.
        logger.debug('user: {show}', show=self.ini.config.user)

        # This is for testing a fatal exception.
        # _ = 5 / 0

    # ---------------------------------------------------------
    # Required in every program.
    #
    async def exit_prog(self):
        """ Stop the server program. """
        logger.info('Terminal EXIT signal received')

        if self.worker:
            await self.worker.stop()

        if self.scheduler.running:
            self.scheduler.shutdown()

        await self.ini.stop()

        if self.error:
            logger.critical('Server halted')
            sys.exit(1)

        else:
            logger.success('Server ended OK')

    # ---------------------------------------------------------
    # Required in every program.
    #
    async def run(self):
        """ Start the server program.

        The following actions are performed:
          - Initiate used resources.
          - Start dynamic log level handling.
          - Wait for program termination.
        """

        try:
            await self._initiate_resources()

            # Please remove in normal code.
            self._demo_purposes_only()

            # Wait until termination, either by operator or by fatal error
            # (this statement needs to be placed last in the method).
            await self.future

        # Ignore the consequences of an operator stopping the program.
        except asyncio.CancelledError:
            pass

        # Handle INI file errors during program startup.
        except IniValidationError:
            await self._handle_ini_error(fatal=True)

        # The initial RabbitMQ connection failed, so no point
        # in generating a traceback and sending an ErrorMessage.
        #
        # When restore state is out-of-sync, you'll get a ValueError.
        except (ConnectionError, ValueError) as why:
            await self._fatal_error_dump(error=why, suppress=True)

        # Handle all unhandled exceptions that reach here as fatal.
        except BaseException as why:
            await self._fatal_error_dump(error=why)

        # Make sure we get a graceful program exit.
        finally:
            await asyncio.sleep(0.5)
            await self.exit_prog()
