#!/usr/bin/env python
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
import sys
import json
import contextlib

# Tools modules
from tools.configurator import config
from tools.async_ini_file_parser import IniValidationError
from tools.local_log_handler import path_of, LogHandler, logger

# Local program modules
from async_example_worker import (asyncio, Path, AsyncIOScheduler,
                                  exceptions, AsyncExampleProgIni,
                                  AsyncExampleWorker)


# ---------------------------------------------------------------------
#
class AsyncExampleProgram:
    """
    This asynchronous server program demonstrates how to use ini files that
    contain both environment variables and secrets that are expanded as well
    as dynamically handles changing parameters in a running program.

    The program also shows how to monitor a directory for arriving files as
    well as how to send topic queue messages and subscribe for topic queue
    messages using RabbitMQ.

    There's also a scheduling mechanism that handles periodic events. The
    program is stopped gracefully by pressing Ctrl-C or pressing the 'X'
    in the upper right corner of the terminal window. The program handles
    both Windows and Linux platforms.

    **macOS caveat**:
      1. You have to add some code in *tools/file_utilities.is_file_available()*
         that handles file availability if not **lsof** is available.
      2. You also have to add a **[darwin]** platform block in the INI file.

    The following environment variable dependencies exist:
      - ENVIRONMENT (on all servers)
      - HOSTNAME (on Linux servers only - set by OS)
      - COMPUTERNAME (on Windows servers only - set by OS)

    The following secret dependencies exist:
      - mongo_pwd

    The following jobs are scheduled:
      - *_schedule_ini_check()*: runs every five seconds.
      - *_schedule_dump_check()*: runs every five seconds.
      - *_schedule_state_pruning()*: runs at midnight every day, and at startup.

    RabbitMQ is used for sending messages to external programs. Messages that
    are to be sent to the RabbitMQ will be stored offline if the communication
    goes down and will be re-sent when the communication is re-established.

    Subscribe temporarily for the following RabbitMQ message topic(s):
      - Health.Request

    Subscribe permanently for the following RabbitMQ message topic(s):
      - File.ReportRequest.<SERVER>

    Sends RabbitMQ messages with the following topic(s):
      - File.Report.<server>
      - File.Detected.<server>
      - Error.Message.AsyncExampleProgram.<server>
      - Health.Response.AsyncExampleProgram.<server>


    :ivar error: Program exit error status.
    :type error: `bool`
    :ivar future:
        Asyncio Future handle (used for handling fatal error program exit).
    :type future: `asyncio.Future`
    :ivar program: Current program name, used for logging.
    :type program: `str`
    :ivar scheduler: Handles periodic ini file change checks.
    :type scheduler: ``apscheduler.schedulers.asyncio.AsyncIOScheduler``
    :ivar log: Handle colored and dynamic filter logging.
    :type log: `LogHandler`
    :ivar worker:  Handles the bulk of the work for this program.
    :type worker: `AsyncExampleWorker`
    :ivar ini: Handles INI file parameters for this program.
    :type ini: `AsyncExampleProgIni`
    """

    # ---------------------------------------------------------
    #
    def __init__(self):
        """ The class constructor. """

        # Root parameters (used in all programs).
        context = Path(sys.argv[0])
        program = self.__class__.__name__

        # General parameters (used in all programs).
        self.error = False
        self.future = None
        self.program: str = program
        self.scheduler = AsyncIOScheduler()
        self.log = LogHandler(program=program,
                              lean_format=True,
                              include_external=True,
                              log_path=path_of(context))

        # Unique parameters (attributes remain but their content change).
        self.worker = None
        self.ini = AsyncExampleProgIni(context.with_suffix('.ini'))

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
    # Required in every program.
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

                if self.ini.changed_document_types:
                    await self.worker.notify({'msgType': 'ChangedIniParams'})

            # Handle INI file errors during execution (not fatal).
            except RuntimeError:
                await self._handle_ini_error(fatal=False)

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
        # Validate the current INI file content.
        await self.ini.start()

        self.log.start(self.ini.log_level)
        logger.success('Starting server on {name}...', name=config.server)

        # Initialize and start the worker processing.
        self.worker = AsyncExampleWorker(self.ini, self.program)
        await self.worker.start()

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


# --------------------------------------------------------------------

if __name__ == "__main__":
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(AsyncExampleProgram().run())
