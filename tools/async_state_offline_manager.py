# -*- coding: utf-8 -*-
"""
License: Apache 2.0

VERSION INFO::

      $Repo: async_example_program
    $Author: Anders Wiklund
      $Date: 2023-10-11 19:59:12
       $Rev: 31
"""

# BUILTIN modules
from pathlib import Path
from typing import Union

# Third party modules
import yaml
import aiofiles

# Tools modules
from tools.local_log_handler import logger


# -----------------------------------------------------------------------------
#
class AsyncStateOfflineManager:
    """ Class that handles archiving and restoring of supplied active states.

    After you have called the *restore_state()* method, you have to call the
    *clear_restored_state()* method, so that the state files are deleted.

    The reason for splitting it into two methods is that something might go
    wrong in your application, at least in the early stages and then the
    data would be gone forever (sometimes safety is better in foresight
    than hindsight).

    :ivar offline_path: Path where files are archived and restored.
    """

    # ---------------------------------------------------------
    #
    def __init__(self, offline_path: str):
        """ The class constructor.

        :param offline_path: Path where files are archived and restored.
        """
        self.offline_path: str = offline_path

    # ----------------------------------------------------------
    #
    async def archive_state(self, active_states: dict, dump: bool = False):
        """ Archive supplied state(s).

        :param active_states: Name of state, and its data.
        :param dump: Dump status (default is False).
        """

        for name, state in active_states.items():
            state_name = ' '.join(name.split('_'))
            dump_prefix = ('dump_' if dump else '')
            log_prefix = ('Dumping' if dump else 'Archiving')
            archive_file = (Path(self.offline_path) /
                            f'{dump_prefix}state.{name}.yaml')
            logger.info(f'{log_prefix} {len(state)} {state_name}...')

            async with aiofiles.open(archive_file, 'w', encoding='utf8') as hdl:
                await hdl.write(yaml.dump(state, allow_unicode=True))

    # ----------------------------------------------------------
    #
    async def restore_state(self) -> Union[dict, list]:
        """ Restore previously archived state(s).

        :return: Restored state(s).
        """

        active_states = {}

        for archive_file in Path(self.offline_path).glob('state.*.yaml'):
            name = archive_file.stem.split('.')[1]
            state_name = ' '.join(name.split('_'))

            async with aiofiles.open(archive_file, 'r', encoding='utf8') as hdl:
                active_states[name] = yaml.safe_load(await hdl.read())

            logger.info(f'Restored {len(active_states[name])} {state_name}...')

        return active_states

    # ----------------------------------------------------------
    #
    async def clear_restored_state(self):
        """ Remove archived state(s) after the restore was successful. """

        for archive_file in Path(self.offline_path).glob('state.*.yaml'):
            archive_file.unlink()
