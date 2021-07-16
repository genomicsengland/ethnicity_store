"""
provides a config factory to generate a environment-specific config object
based on environmental variables provided in a .env file or already laoded
"""
import os
from dotenv import load_dotenv

# loads the contents of .env file at runtime
# if environmental variables have been set already then load_dotenv will not
# overwrite them
basedir = os.path.abspath(os.path.dirname(__file__))
load_dotenv(os.path.join(basedir, '.env'))

class Config:
    
    def __init__(self, env):

        self.environment = env

        # diversity db connection configuration
        self._div_db_name = os.getenv('DIV_DB_NAME')
        self._div_db_host = os.getenv('DIV_DB_HOST')
        self._div_db_port = os.getenv('DIV_DB_PORT') or 5432  # how we can set defaults
        self._div_db_user = os.getenv('DIV_DB_USER')
        self._div_db_password = os.getenv('DIV_DB_PASSWORD')

        # dams db connection configuration
        self._dams_db_name = os.getenv('DAMS_DB_NAME')
        self._dams_db_host = os.getenv('DAMS_DB_HOST')
        self._dams_db_port = os.getenv('DAMS_DB_PORT') or 5432  # how we can set defaults
        self._dams_db_user = os.getenv('DAMS_DB_USER')
        self._dams_db_password = os.getenv('DAMS_DB_PASSWORD')

        # dams db connection configuration
        self._hes_db_name = os.getenv('HES_DB_NAME')
        self._hes_db_host = os.getenv('HES_DB_HOST')
        self._hes_db_port = os.getenv('HES_DB_PORT') or 5432  # how we can set defaults
        self._hes_db_user = os.getenv('HES_DB_USER')
        self._hes_db_password = os.getenv('HES_DB_PASSWORD')

        # log config
        self.log_folder = os.getenv('LOG_FOLDER') or \
            os.path.join(basedir, 'logs')
        self.log_filename = os.getenv('LOG_FILENAME') or \
            'diversity_logs'

    @property
    def div_db_conn_str(self):

        return (f'postgresql+psycopg2://{self._div_db_user}:'
            f'{self._div_db_password}@{self._div_db_host}:'
            f'{self._div_db_port}/{self._div_db_name}')

    @property
    def dams_db_conn_str(self):

        return (f'postgresql+psycopg2://{self._dams_db_user}:'
            f'{self._dams_db_password}@{self._dams_db_host}:'
            f'{self._dams_db_port}/{self._dams_db_name}')

    @property
    def hes_db_conn_str(self):

        return (f'postgresql+psycopg2://{self._hes_db_user}:'
            f'{self._hes_db_password}@{self._hes_db_host}:'
            f'{self._hes_db_port}/{self._hes_db_name}')

    def __repr__(self):

        return f'config: {self.environment}'


class TestingConfig(Config):

    # environment-specific config options
    testing = True
    debug = True
    console_logging = False
    file_logging = True


class ProductionConfig(Config):

    testing = False
    debug = False
    console_logging = False
    file_logging = True


class DevelopmentConfig(Config):

    testing = False
    debug = True
    console_logging = True
    file_logging = True


class ConfigFactory:

    def factory():

        env = os.getenv('ENV') or 'development'

        if env == 'testing':
            return TestingConfig(env)

        elif env == 'production':
            return ProductionConfig(env)

        elif env == 'development':
            return DevelopmentConfig(env)

        else:
            raise RuntimeError('unrecognised environment')
