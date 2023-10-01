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
import asyncio
from typing import Coroutine


# ----------------------------------------------------------
#
async def delay(callback: Coroutine, seconds: int):
    """ Start another coroutine after a delay in seconds.

    :param callback: Coroutine to call after the delay.
    :param seconds: Delay time.
    """
    await asyncio.sleep(seconds)
    await callback
