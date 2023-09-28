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
import os
from asyncio import Queue
from typing import Optional

# Third party modules
from watchdog.events import PatternMatchingEventHandler, FileModifiedEvent

# Local program modules
from .file_utilities import is_file_available


# -----------------------------------------------------------------------------
#
class FileModifiedEventHandler(PatternMatchingEventHandler):
    """ Put a JSON message on the out_queue when a file is detected and available.

    A list of normal search file pattern(s) can be supplied (when no pattern
    is given, the Default value is ['*'], meaning all files).

    A list of ignored search file pattern(s) can be supplied.

    A list of excluded search path(s) in basename form can be supplied (note
    that this only makes sense when the search is recursive, and this flag
    is set in the Observer scheduling part of the job, in another module).

    Structure of a reported message:
      {"msgType": "FileFound", "file": "<filename-with-path>"}


    :ivar out_queue: File detection report queue.
    :type out_queue: `asyncio.Queue`
    :ivar excluded_paths: Excluded paths in basename form.
    :type excluded_paths: `set`
    """

    # ---------------------------------------------------------
    #
    def __init__(self, out_queue: Queue,
                 case_sensitive: bool = False,
                 patterns: Optional[list] = None,
                 excluded_paths: Optional[list] = None,
                 ignore_patterns: Optional[list] = None):
        """ The class constructor.

        :param out_queue: Search response queue.
        :param case_sensitive: Use case-sensitive file matching (default is False).
        :param patterns: A list of file patterns to search for.
        :param excluded_paths: A list of excluded paths (in basename form).
        :param ignore_patterns: A list of file patterns to ignore.
        """
        super().__init__(patterns=patterns, ignore_patterns=ignore_patterns,
                         ignore_directories=True, case_sensitive=case_sensitive)

        self.out_queue = out_queue
        self.excluded_paths = set(
            excluded_paths if isinstance(excluded_paths, list) else []
        )

    # ---------------------------------------------------------
    #
    def is_excluded(self, current_file: str) -> bool:
        """ Return exclude status for specified file.

        :param current_file: Current file.
        :return: Exclude status.
        """
        path_items = list(map(type(current_file).lower, current_file.split(os.sep)))
        return bool(self.excluded_paths.intersection(path_items))

    # ---------------------------------------------------------
    #
    def on_modified(self, event: FileModifiedEvent):
        """ Report found and available file on the outgoing queue.

        Note that a non-excluded file directory is a reporting prerequisite.

        :param event: Received event.
        """

        # Make sure the file is not found in an excluded path.
        if self.excluded_paths and self.is_excluded(event.src_path):
            return

        if is_file_available(event.src_path):
            msg = {'msgType': 'FileFound', 'file': event.src_path}
            self.out_queue.put_nowait(msg)
