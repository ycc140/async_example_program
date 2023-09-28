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
from pathlib import Path
from typing import Union

# Third party modules
import yaml
import aiofiles

# Tools modules
from tools.local_log_handler import logger


# -----------------------------------------------------------------------------
#
class AsyncCacheManager:
    """ Class that handles archiving and restoring of supplied active caches.

    After you have called the *restore_cache()* method, you have to call the
    *clear_restored_cache()* method, so that the cache files are deleted.

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
    async def archive_cache(self, active_caches: dict, dump: bool = False):
        """ Archive supplied cache(s).

        :param active_caches: Name of cache, and its data.
        :param dump: Dump status (default is False).
        """

        for name, cache in active_caches.items():
            cache_name = ' '.join(name.split('_'))
            dump_prefix = ('dump_' if dump else '')
            log_prefix = ('Dumping' if dump else 'Archiving')
            archive_file = (Path(self.offline_path) /
                            f'{dump_prefix}cache.{name}.yaml')
            logger.info(f'{log_prefix} {len(cache)} {cache_name}(s)...')

            async with aiofiles.open(archive_file, 'w', encoding='utf8') as hdl:
                await hdl.write(yaml.dump(cache, allow_unicode=True))

    # ----------------------------------------------------------
    #
    async def restore_cache(self) -> Union[dict, list]:
        """ Restore previously archived cache(s).

        :return: Restored cache(s).
        """

        active_caches = {}

        for archive_file in Path(self.offline_path).glob('cache.*.yaml'):
            name = archive_file.stem.split('.')[1]
            cache_name = ' '.join(name.split('_'))

            async with aiofiles.open(archive_file, 'r', encoding='utf8') as hdl:
                active_caches[name] = yaml.safe_load(await hdl.read())

            logger.info(f'Restored {len(active_caches[name])} {cache_name}(s)...')

        return active_caches

    # ----------------------------------------------------------
    #
    async def clear_restored_cache(self):
        """ Remove archived cache(s) after the restore was successful. """

        for archive_file in Path(self.offline_path).glob('cache.*.yaml'):
            archive_file.unlink()
