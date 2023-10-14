# -*- coding: utf-8 -*-
"""
License: Apache 2.0

VERSION INFO::

      $Repo: async_example_program
    $Author: Anders Wiklund
      $Date: 2023-10-14 22:11:56
       $Rev: 39
"""

# BUILTIN modules
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
# noinspection StructuralWrap
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

    .. Python::
          {"msgType": "FileFound",
           "file": "D:\\prod\\kundin\\cust3\\DDDD.231008.txt"}


    :ivar recursive: Search path(s) recursively (default is False).
    :type recursive: `bool`
    :ivar root_paths: List of path(s) to search.
    :type root_paths: `set`
    :ivar out_queue: File detection report queue.
    :type out_queue: `asyncio.Queue`
    :ivar find_undetected_files:
        Touch undetected files in search path(s) (default is True).
    :type find_undetected_files: `bool`
    :ivar watchers: Keep track of which paths(s) that is observed.
    :type watchers: `dict`
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

        # Input parameters.
        self.recursive = recursive
        self.root_paths = set(paths)
        self.out_queue: Queue = out_queue
        self.find_undetected_files = find_undetected_files

        # Unique parameters.
        self.watchers = {}
        self.suppress = {}

        # Initialize objects.
        self.lock = Lock()
        self.work_queue = Queue()
        self.observer = Observer()
        self.file_mgr = FileModifiedEventHandler(
            self.work_queue, case_sensitive, patterns,
            excl_paths, ignored_patterns)

    # ---------------------------------------------------------
    #
    async def _handle_report_suppression(self):
        """ Handle multiple detections of "small" files (only report once).

        The duplicate detections only occur for "small" files, mostly within
        a few milliseconds, so a 2-second prune timeout should be ok...

        This method is run by the scheduler every 10 seconds.
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
    async def _process_new_search_paths(self, msg: dict):
        """ Update the list of paths used in the search.

        Example data:

        .. Python::
            {'msgType': 'UpdateSearchPaths', 'data': [<root Paths>]}

        :param msg: A UpdateSearchPaths message.
        """

        # Find changes in which paths to observe.
        existing = self.root_paths
        new_paths = set(msg['data'])
        remove = existing - new_paths
        insert = new_paths - existing

        self.root_paths = set(msg['data'])

        # Remove obsolete paths to observe.
        for path in remove:
            self.observer.unschedule(self.watchers[path])
            del self.watchers[path]

        # Insert new paths to observe.
        for path in insert:
            self.watchers[path] = self.observer.schedule(
                self.file_mgr, path, self.recursive)

        logger.info('Monitoring paths: {data}...', data=self.root_paths)

    # ---------------------------------------------------------
    #
    async def _handle_undetected_files(self):
        """ Touch undetected files to make them detectable again. """

        for directory in self.root_paths:

            if self.recursive:
                for item in Path(directory).rglob('*'):
                    item.touch(exist_ok=True)

            else:
                for item in Path(directory).glob('*'):
                    item.touch(exist_ok=True)

    # ---------------------------------------------------------
    #
    async def _process_status_request(self):
        """ Process health status request. """
        name = self.__class__.__name__
        msg = {'msgType': 'StatusResponse',
               'resources': {f'{name}._message_broker': True,
                             f'{name}.observer': self.observer.is_alive()}}
        self.out_queue.put_nowait(msg)

    # ---------------------------------------------------------
    #
    async def _message_broker(self):
        """ Broker messages between interested parties using a queue.

        Handled message types are:
          - Stop
          - FileFound
          - StatusRequest
          - UpdateSearchPaths
        """

        try:
            logger.trace('Starting searcher BROKER task...')

            while True:
                msg = await self.work_queue.get()

                # Exit the loop when the program termination is triggered internally.
                if msg['msgType'] == 'Stop':
                    break

                elif msg['msgType'] == 'StatusRequest':
                    await self._process_status_request()

                elif msg['msgType'] == 'FileFound':
                    await self._process_file_found(msg)

                elif msg['msgType'] == 'UpdateSearchPaths':
                    await self._process_new_search_paths(msg)

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

        # Add paths that the observer should monitor.
        for path in self.root_paths:
            self.watchers[path] = self.observer.schedule(
                self.file_mgr, path, self.recursive)

        logger.info('Monitoring paths: {what}...', what=self.root_paths)

        # Start a normal python thread enveloped in an asyncio task.
        event_loop = get_event_loop()
        event_loop.call_soon_threadsafe(self.observer.start)

        # Start a scheduling prune suppression cache job.
        scheduler = AsyncIOScheduler()
        scheduler.add_job(self._handle_report_suppression,
                          trigger='interval', seconds=10)

        if self.find_undetected_files:
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
