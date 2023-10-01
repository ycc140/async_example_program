# -*- coding: utf-8 -*-
"""
License: Apache 2.0

VERSION INFO::

      $Repo: async_example_program
    $Author: Anders Wiklund
      $Date: 2023-10-01 06:15:18
       $Rev: 14
"""

# BUILTIN modules
import os
import time
from pathlib import Path
from typing import Optional
from asyncio import (CancelledError, Queue,
                     create_task, get_event_loop, Lock)

# Third party modules
from watchdog.observers import Observer
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# Local program modules
from tools.local_log_handler import logger
from tools.file_modified_event_handler import FileModifiedEventHandler


# -----------------------------------------------------------------------------
#
class AsyncFileSearcher:
    """ This class detects files in one, or several specified directories.

    Directories that are to be excluded from the search can be specified.
    A specific file pattern can be supplied, default is all files. File
    patterns that should be ignored can be supplied. A recursive search
    can also be activated.

    The file is only reported when it's available (not when it's
    initially detected since it might be in a copying or moving phase).

    Due to antivirus programs are scanning new files as well, there's sometimes
    a conflict of interest. The consequences when this happens are that when
    the file is detected, it's not available, and therefore it will be ignored.
    Another related issue is that files might already exist in the monitored
    path(s) when the program starts, and they will not be detected either.
    This problem is handled when you let the *find_undetected_files* input
    parameter use its default value. When `True` A check runs every two minutes
    that touches files that weren't detected earlier.

    The workflow is as follows:
      1. Start the file detection by calling method start().
      2. Receive detection messages on supplied out_queue.
      3. Stop the detection by calling method stop().

    Example data:
      {"msgType": "FileFound", "file": "D:/Prod/Pre/Incoming/AKFAIN12.DAT"}


    :ivar touch_files:
        Touch undetected files in search path(s) (default is True).
    :type touch_files: `dict` or `None`
    :ivar out_queue: File detection report queue.
    :type out_queue: `asyncio.Queue`
    :ivar suppress: Suppress reporting of already detected files.
    :type suppress: `dict`
    :ivar lock: A locking mechanism that protects the suppressed cache object.
    :type lock: `asyncio.Lock`
    :ivar work_queue: Internal message broker queue.
    :type work_queue: `asyncio.Queue`
    :ivar observer:
        Observer thread that schedules watching directories
        and dispatches calls to event handlers.
    :type observer: ``watchdog.observers.Observer``
    :ivar file_mgr:
        Put a JSON message on the report queue when a file
        is detected and available.
    :type file_mgr: `FileModifiedEventHandler`
    """

    # ---------------------------------------------------------
    #
    def __init__(
            self, paths: list, out_queue: Queue,
            recursive: bool = False, find_undetected_files: bool = True,
            case_sensitive: bool = False, excluded_paths: Optional[list] = None,
            patterns: Optional[list] = None, ignored_patterns: Optional[list] = None
    ):
        """ The class constructor.

        :param paths: List of path(s) to search.
        :param out_queue: Search response queue.
        :param recursive: Search path(s) recursively (default is False).
        :param find_undetected_files: Touch undetected files in search
            path(s) (default is True).
        :param case_sensitive: Use case-sensitive file matching (default is False).
        :param excluded_paths: List of path(s) to exclude in search.
        :param patterns: A list of file search patterns to use
            (implicitly ['*'] is used if None is specified).
        :param ignored_patterns: A list of file search patterns to ignore.
        :raise RuntimeError: When an invalid path is specified.
        :raise AssertionError: When path parameter is not a list.
        :raise AssertionError: When pattern parameter is not None or not a list.
        :raise AssertionError: When excluded_paths parameter is not None or not a list.
        :raise AssertionError: When ignored_patterns parameter is not None or not a list.
        """

        # Make sure the correct parameter type exists.
        assert isinstance(paths, list)
        assert patterns is None or isinstance(patterns, list)
        assert excluded_paths is None or isinstance(excluded_paths, list)
        assert ignored_patterns is None or isinstance(ignored_patterns, list)

        # Make sure the list is in lower case.
        excl_paths = (
            [path.lower() for path in excluded_paths] if excluded_paths else []
        )

        self.touch_files = ({'recursive': recursive, 'paths': paths}
                            if find_undetected_files else None)

        # Input parameters.
        self.out_queue: Queue = out_queue

        # Unique parameters.
        self.suppress = {}

        # Initialize objects.
        self.lock = Lock()
        self.work_queue = Queue()
        self.observer = Observer()
        self.file_mgr = FileModifiedEventHandler(
            self.work_queue, case_sensitive, patterns, excl_paths, ignored_patterns)

        # Local parameters.
        invalid_paths = []

        for path in paths:

            # Make sure the path exists before starting observation.
            if os.path.isdir(path):
                self.observer.schedule(self.file_mgr, path, recursive)

            else:
                invalid_paths.append(path)

        if invalid_paths:
            raise RuntimeError(f'Found invalid path(s): {invalid_paths}')

    # ---------------------------------------------------------
    #
    async def _handle_report_suppression(self):
        """ Handle multiple detections of "small" files (only report once).

        The duplicate detections only occur for "small" files, mostly within
        a few milliseconds, so a 2-second prune timeout should be ok...

        This method is run by the scheduler every 5 seconds.
        """

        if not self.suppress:
            return

        async with self.lock:
            pending = self.suppress.copy()

        deletable = []
        now = time.time()

        for key in pending:
            age = now - pending[key]['detected']

            # This should be enough time...
            if age > 2.0:
                deletable.append(key)

        if deletable:
            async with self.lock:
                for key in deletable:
                    del self.suppress[key]

            logger.debug('Pruned {cnt} suppressed files', cnt=len(deletable))

    # ---------------------------------------------------------
    #
    async def _process_file_found(self, msg: dict):
        """ Process received FileFound message.

        :param msg: A FileFound msgType message.
        """
        item = msg['file']

        # Only report the first detection of an available file.
        if item not in self.suppress:
            async with self.lock:
                self.suppress[item] = {'detected': time.time()}

            self.out_queue.put_nowait(msg)

    # ---------------------------------------------------------
    #
    async def _handle_undetected_files(self):
        """ Touch undetected files to make them detectable again. """

        for directory in self.touch_files['paths']:

            if self.touch_files['recursive']:
                for item in Path(directory).rglob('*'):
                    item.touch(exist_ok=True)

            else:
                for item in Path(directory).glob('*'):
                    item.touch(exist_ok=True)

    # ---------------------------------------------------------
    #
    async def _process_status_request(self):
        """ Process health status request. """

        msg = {'msgType': 'StatusResponse',
               'resources': {'AsyncFileSearcher._message_broker': True,
                             'AsyncFileSearcher.observer': self.observer.is_alive()}}
        self.out_queue.put_nowait(msg)

    # ---------------------------------------------------------
    #
    async def _message_broker(self):
        """ Broker messages between interested parties using a queue.

        Handled message types are:
          - Stop
          - FileFound
          - StatusRequest
        """

        try:
            logger.trace('Starting searcher BROKER task...')

            while True:
                msg = await self.work_queue.get()

                # Exit the loop when the program termination is triggered internally.
                if msg['msgType'] == 'Stop':
                    break

                # Health status request.
                elif msg['msgType'] == 'StatusRequest':
                    await self._process_status_request()

                # A new file is detected in the supervised context.
                elif msg['msgType'] == 'FileFound':
                    await self._process_file_found(msg)

            logger.trace('Stopped searcher BROKER task')

        except CancelledError:
            logger.trace('Operator cancelled searcher BROKER task')

    # ----------------------------------------------------------
    #
    async def notify(self, msg: dict):
        """ Send msgType messages to the broker.

        :param msg: A msgType message.
        """
        self.work_queue.put_nowait(msg)

    # ---------------------------------------------------------
    #
    async def start(self):
        """ Start the used resources in a controlled way. """

        # Start asyncio tasks.
        _ = create_task(self._message_broker())

        # Start a normal python thread enveloped in an asyncio task.
        event_loop = get_event_loop()
        event_loop.call_soon_threadsafe(self.observer.start)

        # Start a scheduling prune suppression cache job.
        scheduler = AsyncIOScheduler()
        scheduler.add_job(self._handle_report_suppression,
                          trigger='interval', seconds=10)

        if self.touch_files:
            scheduler.add_job(self._handle_undetected_files,
                              trigger='interval', minutes=2)

        scheduler.start()

    # ---------------------------------------------------------
    #
    async def stop(self):
        """ Stop the used resources in a controlled way. """

        # Wait for threads to stop.
        if self.observer.is_alive():
            self.observer.stop()
            self.observer.join()

        # Stop queue blocking tasks.
        stop = {'msgType': 'Stop'}
        await self.work_queue.put(stop)
