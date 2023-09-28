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
import sys
import site
from typing import Union, Type, Tuple

# Third party modules
from pydantic import Field, computed_field
from pydantic_settings import (BaseSettings, SettingsConfigDict,
                               PydanticBaseSettingsSource)

# Constants
USER_BASE: str = site.getuserbase()
""" This is required in some environments."""
MISSING_SECRET = '>>> missing secrets file <<<'
""" Error message for missing secrets file. """
MISSING_ENV = '>>> undefined environment parameter <<<'
""" Error message for missing environment parameter. """
ENVIRONMENT: str = os.getenv('ENVIRONMENT', MISSING_ENV)
""" Declared local server environment. """
SECRETS_DIR: str = ('/run/secrets'
                    if os.path.exists('/.dockerenv')
                    else f'{site.getuserbase()}/secrets')
""" This is where your secrets are stored (in Docker or locally). """

# --------------------------------------------------------------
# This needs to be done before the Base class gets evaluated, and
# to avoid getting UserWarnings that the path does not exist.
#
# Create the directory if it does not already exist. When running
# inside Docker, skip it (Docker handles that just fine on its own).
#
if not os.path.exists('/.dockerenv'):
    os.makedirs(SECRETS_DIR, exist_ok=True)


# ------------------------------------------------------------------------
#
# noinspection PyUnresolvedReferences
class Common(BaseSettings):
    """ Common configuration parameters shared between all environments.

    Read configuration parameters defined in this class, and from
    ENVIRONMENT variables and from the .env file.

    The source priority is changed to the following order:
      1. init_settings
      2. dotenv_settings
      3. env_settings
      4. file_secret_settings

    The following environment variables should already be defined:
      - ENVIRONMENT (on all servers)
      - HOSTNAME (on Linux servers only - set by OS)
      - COMPUTERNAME (on Windows servers only - set by OS)

    The following secrets should already be defined:
      - service_api_key
      - mongo_url_{environment}
      - rabbit_url_{environment}

    Path where your .env and <environment>.env file should be placed:
      - linux: /home/<user>/.local
      - darwin: /home/<user>/.local
      - win32: C:\\Users\\<user>\\AppData\\Roaming\\Python'

    Path where your secret files should be placed:
      - linux: /home/<user>/.local/secrets
      - darwin: /home/<user>/.local/secrets
      - win32: C:\\Users\\<user>\\AppData\\Roaming\\Python\\secrets'

    You know you are running in Docker when the "/.dockerenv" file exists.


    :ivar model_config: Fixed name used by the BaseSettings class.
    :ivar exchange: RabbitMQ topic exchange name.
    :ivar logSeverity: RabbitMQ link status log level mappings.
    :ivar serviceApiKey: Microservices API key.
    :ivar env: Defined environment variable ENVIRONMENT.
    :ivar server: Local server name without possible domain part.
    """
    model_config = SettingsConfigDict(extra='ignore',
                                      secrets_dir=SECRETS_DIR,
                                      env_file_encoding='utf-8',
                                      env_file=f'{site.USER_BASE}/.env')

    # Constants.
    exchange: str = 'topic_routing'
    logSeverity: dict = {'LinkUp': 'info', 'LinkDown': 'warning'}

    # Secrets depending parameters.
    serviceApiKey: str = Field(MISSING_SECRET, alias='service_api_key')

    # Environment depending parameters.
    env: str = ENVIRONMENT

    @computed_field
    @property
    def server(self) -> str:
        """ Return local server name stripped of possible domain part.

        :return: Server name in upper case.
        """
        name = ('COMPUTERNAME' if sys.platform == 'win32' else 'HOSTNAME')
        return os.getenv(name, MISSING_ENV).upper().split('.')[0]

    @classmethod
    def settings_customise_sources(
            cls,
            settings_cls: Type[BaseSettings],
            init_settings: PydanticBaseSettingsSource,
            env_settings: PydanticBaseSettingsSource,
            dotenv_settings: PydanticBaseSettingsSource,
            file_secret_settings: PydanticBaseSettingsSource,
    ) -> Tuple[PydanticBaseSettingsSource, ...]:
        """
        Moved dotenv_settings before env_settings and removed settings_cls
        since it causes recursion max depth error in this configuration.
        """
        return (init_settings, dotenv_settings,
                env_settings, file_secret_settings)


# ------------------------------------------------------------------------
#
class Dev(Common):
    """ Configuration parameters for DEV environment.

    Values from dev.env supersede previous values when the file exists.


    :ivar model_config: Fixed name used by the BaseSettings class.
    :ivar dbServer: The MySQL development server.
    :ivar mongoUrl: The MongoDB development URL.
    :ivar rabbitUrl: The RabbitMQ development URL.
    """
    model_config = SettingsConfigDict(env_file=f'{USER_BASE}/{Common().env}.env')

    dbServer: str = 'localhost:3306'
    mongoUrl: str = Field(MISSING_SECRET, alias=f'mongo_url_{Common().env}')
    rabbitUrl: str = Field(MISSING_SECRET, alias=f'rabbit_url_{Common().env}')


# ------------------------------------------------------------------------
#
class Prod(Common):
    """ Configuration parameters for PROD environment.

    Values from prod.env supersede previous values when the file exists.


    :ivar model_config: Fixed name used by the BaseSettings class.
    :ivar dbServer: The MySQL production server.
    :ivar mongoUrl: The MongoDB production URL.
    :ivar rabbitUrl: The RabbitMQ production URL.
    """
    model_config = SettingsConfigDict(env_file=f'{USER_BASE}/{Common().env}.env')

    dbServer: str = 'remote-db-server.com:3306'
    mongoUrl: str = Field(MISSING_SECRET, alias=f'mongo_url_{Common().env}')
    rabbitUrl: str = Field(MISSING_SECRET, alias=f'rabbit_url_{Common().env}')


# ------------------------------------------------------------------------

# Translation table between ENVIRONMENT value and their classes.
_setup = dict(
    dev=Dev,
    prod=Prod,
)

# Validate and instantiate specified environment configuration.
config: Union[Dev, Prod] = _setup[ENVIRONMENT]()
""" Environment specific configuration parameters. """
