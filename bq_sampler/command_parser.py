# vim: ai:sw=4:ts=4:sta:et:fo=croql
"""
Parses a dictionary into an instance of :py:class:`command: command.CommandBase`.
"""
from typing import Any, Dict

from bq_sampler.entity import command
from bq_sampler import logger

_LOGGER = logger.get(__name__)


def to_command(value: Dict[str, Any], timestamp: int) -> command.CommandBase:
    """
    Converts a dictionary to the corresponding
        :py:class:`command.CommandBase` subclass.

    :param value:
    :param timestamp:
    :return:
    """
    # validate input
    if not isinstance(value, dict):
        raise TypeError(f'Expecting a {dict.__name__} as argument. Got: <{value}>({type(value)})')
    req_type = command.CommandType.from_str(value.get(command.CommandBase.type.__name__))
    if not req_type:
        raise ValueError(f'Cannot create command of type <{req_type}> in argument <{value}>')
    if not isinstance(timestamp, int) or timestamp <= 0:
        raise ValueError(
            f'Timestamp must be a positive {int.__name__}. Got <{timestamp}>({type(timestamp)})'
        )
    # logic
    value[command.CommandBase.timestamp.__name__] = timestamp
    if req_type == command.CommandType.START:
        result = command.CommandStart.from_dict(value)
    elif req_type == command.CommandType.SAMPLE_START:
        result = command.CommandSampleStart.from_dict(value)
    elif req_type == command.CommandType.SAMPLE_DONE:
        result = command.CommandSampleDone.from_dict(value)
    else:
        raise ValueError(f'Command type <{req_type}> is not supported. Argument: <{value}>')
    return result
