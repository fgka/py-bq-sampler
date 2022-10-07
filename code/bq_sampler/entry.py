# vim: ai:sw=4:ts=4:sta:et:fo=croql
# pylint: disable=line-too-long
"""
GCP `CloudFunction`_ entry point
.. _CloudFunction: https://cloud.google.com/functions/docs/quickstart-python
"""
# pylint: enable=line-too-long
from datetime import datetime
import calendar
import os
import re
from typing import Any, Dict, Optional

from bq_sampler import process_request
from bq_sampler import command_parser
from bq_sampler.gcp import pubsub
from bq_sampler import logger

_LOGGER = logger.get(__name__)


def handler(  # pylint: disable=unused-argument
    event: Optional[Dict[str, Any]] = None,
    context: Optional[Any] = None,
) -> None:
    """Responds to any HTTP request.
    Args:
        event (dict): Event payload.
        context (google.cloud.functions.Context): Event context.
    Returns:
        The response text or any set of values that can be turned into a
        Response object using
        `make_response <https://flask.palletsprojects.com/en/1.1.x/api/#flask.Flask.make_response>`.
    """
    # validate input
    if not isinstance(event, dict):
        raise TypeError(f'Event must be of type {dict.__name__}. Got <{event}>({type(event)})')
    if context is None:
        raise TypeError(f'Context must be a valid object. Got <{context}>({type(context)})')
    # logic
    _LOGGER.debug(
        'Processing event ID <%s> from <%s> type <%s>',
        context.event_id,
        context.resource.get('name'),
        context.resource.get('type'),
    )
    try:
        _handler(event, context)
    except Exception as err:
        raise RuntimeError(
            f'Could not process event: <{event}>,'
            f' context: <{context}>,'
            f' and environment: <{os.environ}>. '
            f'Error: {err}'
        ) from err


def _handler(
    cmd: Dict[str, Any],
    context: Any,
) -> None:
    cmd = _from_pubsub_to_cmd(cmd, context)
    process_request.process(cmd)


def _from_pubsub_to_cmd(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    cmd_dict = pubsub.parse_json_data(event.get('data'))
    timestamp = _extract_timestamp_from_iso_str(context.timestamp)
    return command_parser.to_command(cmd_dict, timestamp)


def _extract_timestamp_from_iso_str(value: str) -> int:
    # Zulu timezone = UTC
    # https://en.wikipedia.org/wiki/List_of_military_time_zones
    plain_iso_dateime = re.sub(r'(\.[0-9]\+)?Z', '+00:00', value)
    try:
        result: int = calendar.timegm(datetime.fromisoformat(plain_iso_dateime).utctimetuple())
    except Exception as err:
        raise RuntimeError(
            f'Could not extract timestamp from <{value}>({type(value)}).' f' Error: {err}'
        ) from err
    return result
