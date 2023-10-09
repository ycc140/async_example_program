#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
License: Apache 2.0

VERSION INFO::

      $Repo: async_example_program
    $Author: Anders Wiklund
      $Date: 2023-10-09 18:52:05
       $Rev: 24
"""

# BUILTIN modules
import contextlib

# Tools modules
from tools.async_base_program import AsyncBaseProgram

# Local program modules
from async_example_ini_core import AsyncExampleProgIni
from async_example_worker import asyncio, AsyncExampleWorker


# ---------------------------------------------------------------------
#
class AsyncExampleProgram(AsyncBaseProgram):
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


    :ivar ini: Handles INI file parameters for this program.
    :type ini: `AsyncExampleProgIni`
    :ivar worker:  Handles the bulk of the work for this program.
    :type worker: `AsyncExampleWorker`
    :ivar scheduler: Handles periodic ini file change checks.
    :type scheduler: ``apscheduler.schedulers.asyncio.AsyncIOScheduler``
    """

    # ---------------------------------------------------------
    # Optional, only needed if the worker needs to be notified
    # of INI parameter changes.
    #
    async def _schedule_unique_ini_check(self):
        """ Send INI file update notification to the worker. """

        if self.ini.changed_document_types:
            await self.worker.notify({'msgType': 'ChangedIniParams'})

    # ---------------------------------------------------------
    # Unique in every program.
    #
    async def _initiate_unique_resources(self):
        """ Initiate unique resources used by the program.

        The following actions are performed:
          - Validate INI file content.
          - Initiate worker.
        """
        # Validate the current INI file content.
        self.ini = AsyncExampleProgIni(self.location.with_suffix('.ini'))
        await self.ini.start()

        # Initialize and start the worker processing.
        self.worker = AsyncExampleWorker(self.ini, self.program)
        await self.worker.start()


# --------------------------------------------------------------------

if __name__ == "__main__":
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(AsyncExampleProgram().run())
