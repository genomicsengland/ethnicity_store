"""
creates the console and file handlers for logging
file handler is a timed rotating handler (rotating each day) and
keeping backups for a week
alternative would be rotatingfilehandler with some maxBytes so that we only
keep a certain size of log, not certain time
"""
import os
import logging
from logging.config import dictConfig

def setup_logger(c):
    """
    set up logging handlers and formatters
    :params c: a config object that contains something to say whether we are
    in debug mode (consoldHandler level will be set to debug), and log_dir and
    log_filenames
    """

    # set up log directory if it doesn't exist
    log_dir = c.log_folder
    if not os.path.exists(log_dir):
        os.mkdir(log_dir)

    # log config
    log_config = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'fFormatter': {
                'class': 'logging.Formatter',
                'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                'datefmt': '%Y-%m-%d %H:%M:%S'
            },
            'cFormatter': {
                'class': 'logging.Formatter',
                'format': '%(name)s - %(levelname)s - %(message)s'
            }
        },
        'handlers': {
            'consoleHandler': {
                'class': 'logging.StreamHandler',
                'level': 'DEBUG' if c.debug else 'INFO',
                'formatter': 'cFormatter'
            },
            'fileHandler': {
                'class': 'logging.handlers.TimedRotatingFileHandler',
                'filename': os.path.join(log_dir, c.log_filename),
                'formatter': 'fFormatter',
                'when': 'd',
                'interval': 1,
                'backupCount': 7
            }
        },
        'root': {
            'level': 'DEBUG',
            'handlers': list(h for (h, b) in zip(
                ['consoleHandler', 'fileHandler'],
                [c.console_logging, c.file_logging]
            ) if b)
        }
    }

    # configure logging
    dictConfig(log_config)
