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
import json
from pathlib import Path
from typing import Optional

# Third party modules
import aiofiles

# Constants
PEND_MQ_NAME = 'PendingMqBuffer.json'
""" Name of Pending messages offline buffer file. """


# -----------------------------------------------------------------------------
#
class AsyncOfflineBuffer:
    """ This class handles an offline message buffer.

    Each message that should be stored needs to be a dict before it can be
    appended to the offline buffer file. The buffer will grow indefinitely
    until it is emptied.

    The buffer can be retrieved as a list of dict messages, and the buffer
    is regarded as empty after that.

    Buffer extension will always be .json, regardless of the initial buffer name.


    :ivar count: Keep track on number of stored messages.
    :ivar filename: Name of offline file.
    :ivar buffer_empty: Keeps track of buffer empty status.
    """

    # ---------------------------------------------------------
    #
    def __init__(self, offline_path: str, name: Optional[str] = None):
        """ The class constructor.

        :param offline_path: Offline path used for storing failed messages.
        :param name: Filename used for storing failed messages.
        """
        self.count = 0

        if name:
            name = Path(name).with_suffix('.json')

        self.filename: Path = (Path(offline_path, name) if name
                               else Path(offline_path, PEND_MQ_NAME))

        # Set buffer status depending on file existence.
        self.buffer_empty = not self.filename.is_file()

    # ---------------------------------------------------------
    #
    def __len__(self) -> int:
        """ Return number of buffered messages.

        :return: Number of buffered messages.
        """
        return self.count

    # ---------------------------------------------------------
    #
    async def append(self, msg: dict):
        """ Append messages to an existing buffer with 'utf-8' encoding.

        If the buffer is regarded as empty, create a new one.

        :param msg: Message to buffer.
        """

        params = ('w' if self.buffer_empty else 'a')
        line = json.dumps(msg, ensure_ascii=False)

        async with aiofiles.open(self.filename, params, encoding='utf8') as hdl:
            await hdl.write(f'{line}\n')

        self.buffer_empty = False
        self.count += 1

    # ---------------------------------------------------------
    #
    async def retrieve(self) -> list:
        """ Return the content of the current buffer and regard the buffer empty.

        :return: Buffer messages.
        """
        result = []

        if not self.buffer_empty:

            async with aiofiles.open(self.filename, 'r', encoding='utf8') as hdl:

                for line in await hdl.readlines():
                    if line:
                        msg = json.loads(line)
                        result.append(msg)

            self.filename.unlink()
            self.buffer_empty = True
            self.count = 0

        return result

    # ---------------------------------------------------------
    #
    @property
    def is_empty(self) -> bool:
        """ Return buffer empty status.

        :return: Buffer empty status.
        """
        return self.buffer_empty
