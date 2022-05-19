# vim: ai:sw=4:ts=4:sta:et:fo=croql
# pylint: disable=line-too-long
"""
GCP CloudFunction mandatory entry point:
* https://cloud.google.com/functions/docs/writing#functions-writing-file-structuring-python
* https://cloud.google.com/functions/docs/writing/http
* https://cloud.google.com/functions/docs/tutorials/pubsub
"""
# pylint: enable=line-too-long
import logging
import os
from typing import Optional, Union

LOG_LEVEL_ENV_VAR_NAME: str = 'LOG_LEVEL'  # DEBUG or WARNING, etc
_DEFAULT_LOG_LEVEL: int = logging.INFO
_LOGGER_FORMAT: str = '[%(asctime)s %(filename)s.%(funcName)s:%(lineno)s]%(levelname)s: %(message)s'


def get(name: str, *, level: Optional[Union[str, int]] = None) -> logging.Logger:
    """
    Creates a :py:class:`logging.Logger` setting the log level based on the following priority:
    - argument `level`;
    - environment variable :py:data:`LOG_LEVEL_ENV_VAR_NAME`;
    - default value in: :py:data:`_DEFAULT_LOG_LEVEL`.

    :param name:
    :param level:
    :return:
    """
    if not isinstance(name, str) or not name.strip():
        raise ValueError(f'Name must be a non-empty string. Got: <{name}>({type(name)})')
    name = name.strip()
    level = _log_level(level)
    logging.basicConfig(level=level, format=_LOGGER_FORMAT)
    logging.debug('Set log level for %s to %d', name, level)
    return _create_logger(name, level)


def _log_level(level: Optional[Union[str, int]]) -> int:
    result = None
    # argument
    if isinstance(level, int):
        result = level
    if isinstance(level, str):
        result = getattr(logging, level)
    # env var
    if result is None:
        level_str = os.environ.get(LOG_LEVEL_ENV_VAR_NAME)
        if level_str:
            result = getattr(logging, level_str)
    # default
    if result is None:
        result = _DEFAULT_LOG_LEVEL
    return result


def _create_logger(name: str, level: int) -> logging.Logger:
    result = logging.getLogger(name.strip())
    result.setLevel(level)
    if level == logging.DEBUG:
        formatter = logging.Formatter(_LOGGER_FORMAT)
        handler = logging.StreamHandler()
        handler.setLevel(level)
        handler.setFormatter(formatter)
        result.addHandler(handler)
    return result
