import logging
import sys
from portability import colored


loggers = {}

#LOGGING_LEVEL = logging.DEBUG
LOGGING_LEVEL = logging.WARNING


# Logging
def get_logger(name='parcel'):
    """Create or return an existing logger with given name
    """

    if name in loggers:
        return loggers[name]
    log = logging.getLogger(name)
    log.propagate = False
    log.setLevel(LOGGING_LEVEL)
    if sys.stdout.isatty():
        formatter = logging.Formatter(
            colored('%(asctime)s - %(name)s - %(levelname)s: ', 'blue')+'%(message)s')
    else:
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s: %(message)s')
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    log.addHandler(handler)
    loggers[name] = log
    return log
