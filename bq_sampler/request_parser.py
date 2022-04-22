# vim: ai:sw=4:ts=4:sta:et:fo=croql
"""
Parses a dictionary into an instance of :py:class:`event_request: request.EventRequest`.
"""
from typing import Any, Dict

import logging

from bq_sampler.dto import request

_LOGGER = logging.getLogger(__name__)


def to_event_request(value: Dict[str, Any]) -> request.EventRequest:
    """
    Converts a dictionary to the corresponding
        :py:class:`request.EventRequest` subclass.

    :param value:
    :return:
    """
    # validate input
    if not isinstance(value, dict):
        raise TypeError(f'Expecting a {dict.__name__} as argument. Got: <{value}>({type(value)})')
    req_type = request.RequestType.from_str(value.get('type'))
    if not req_type:
        raise ValueError(f'Cannot create request of type <{req_type}> in argument <{value}>')
    request_timestamp = None  # TODO
    # logic
    value['request_timestamp'] = request_timestamp
    if req_type == request.RequestType.START:
        result = request.EventRequestStart.from_dict(value)
    elif req_type == request.RequestType.SAMPLE_START:
        result = request.EventRequestSampleStart.from_dict(value)
    elif req_type == request.RequestType.SAMPLE_DONE:
        result = request.EventRequestSampleDone.from_dict(value)
    else:
        raise ValueError(f'Request type <{req_type}> is not supported. Argument: <{value}>')
    return result
