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
from typing import Any, Dict, Optional

from bq_sampler import command_parser, logger, process_request
from bq_sampler.gcp import pubsub
from bq_sampler.entity import command

_LOGGER = logger.get(__name__)


def handler(  # pylint: disable=unused-argument
    event: Optional[Dict[str, Any]] = None,
    context: Optional[Any] = None,
) -> str:
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
        _LOGGER.error('Event must be of type %s. Got <%s>(%s)', dict.__name__, event, type(event))
    if context is None:
        _LOGGER.warning('Context is None. Got <%s>(%s)', context, type(context))
    else:
        _LOGGER.debug(
            'Processing event ID <%s> from <%s> type <%s>',
            context.event_id,
            context.resource.get('name'),
            context.resource.get('type'),
        )
    # logic
    try:
        response = _handler(event, context)
    except Exception as err:  # pylint: disable=broad-except
        response = (
            f'Could not process event: <{event}>, '
            f'context: <{context}>, '
            f'and environment: <{os.environ}>. '
            f'Error: {err}'
        )
        _LOGGER.critical(response)
    return response


def _handler(
    event: Dict[str, Any],
    context: Any,
) -> str:
    try:
        cmd = _from_pubsub_to_cmd(event, context)
    except Exception as err:  # pylint: disable=broad-except
        raise RuntimeError(
            f'Could not parse event <{event}> '
            f'into a {command.CommandBase.__name__} instance. '
            f'Error: {err}',
        ) from err
    return process_request.process(cmd)


def _from_pubsub_to_cmd(event: Dict[str, Any], context: Any) -> command.CommandBase:
    message = event.get('message')
    timestamp = datetime.utcnow().timestamp()
    # is it an HTTP triggered function?
    if isinstance(message, dict):
        data = message.get('data')
        timestamp = _extract_timestamp_from_iso_str(message.get('publish_time'))
    else:
        data = event.get('data')
        if context is not None:
            timestamp = _extract_timestamp_from_iso_str(context.timestamp)
    cmd_dict = pubsub.parse_json_data(data)
    return command_parser.to_command(cmd_dict, timestamp)


def _extract_timestamp_from_iso_str(value: str) -> int:
    # Zulu timezone = UTC
    # https://en.wikipedia.org/wiki/List_of_military_time_zones
    plain_iso_dateime = value.removesuffix('Z').split('.')[0] + '+00:00'
    try:
        result: int = calendar.timegm(datetime.fromisoformat(plain_iso_dateime).utctimetuple())
    except Exception as err:  # pylint: disable=broad-except
        raise RuntimeError(
            f'Could not extract timestamp from <{value}>({type(value)}). Error: {err}'
        ) from err
    return result
