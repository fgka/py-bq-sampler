# vim: ai:sw=4:ts=4:sta:et:fo=croql
# pylint: disable=line-too-long
"""
GCP `CloudFunction`_ entry point
.. _CloudFunction: https://cloud.google.com/functions/docs/quickstart-python
"""
# pylint: enable=line-too-long
from datetime import datetime
import calendar
import logging
import os
from typing import Any, Dict, Optional

from bq_sampler import process_request
from bq_sampler import request_parser
from bq_sampler.gcp import pubsub

_LOGGER = logging.getLogger(__name__)


def handler(  # pylint: disable=unused-argument
    event: Optional[Dict[str, Any]] = None,
    context: Optional[Any] = None,
) -> None:
    """Responds to any HTTP request.
    Args:
        event (dict): Event payload.
        context (google.cloud.functions.Context): Event context.
        use_env_var_gcs_path (bool): if py:obj:`True` will overwrite object name resolution in GCS
            to use environment variable defined path.
        add_env_var_moveit_server (bool): if py:obj:`True` will force inclusion of
            environment variable defined MOVEit server.
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
        msg = (
            f'Could not process event: <{event}>,'
            f' context: <{context}>,'
            f' and environment: <{os.environ}>'
        )
        _LOGGER.critical(msg)
        raise RuntimeError(msg) from err


def _handler(
    event: Dict[str, Any],
    context: Any,
) -> None:
    event_request = _from_pubsub_to_event_request(event, context)
    process_request.process(event_request)


def _from_pubsub_to_event_request(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    request_dict = pubsub.parse_json_data(event.get('data'))
    timestamp: int = calendar.timegm(datetime.utcfromtimestamp(context.timestamp).utctimetuple())
    return request_parser.to_event_request(request_dict, timestamp)
