# vim: ai:sw=4:ts=4:sta:et:fo=croql
# pylint: disable=line-too-long
"""
GCP `CloudFunction`_ entry point
.. _CloudFunction: https://cloud.google.com/functions/docs/quickstart-python
"""
# pylint: enable=line-too-long
import base64
from datetime import datetime
import calendar
import json
from typing import Any, Dict, Optional
import logging
import os

from bq_sampler import process_request
from bq_sampler import request_parser

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
    _LOGGER.debug(
        'Processing event ID <%s> from <%s> type <%s>',
        context.event_id,
        context.resource.get('name'),
        context.resource.get('type'),
    )
    try:
        _handler(event, context)
    except Exception as err:
        msg = f'Could not process event: <{event}>, context: <{context}>, and environment: <{os.environ}>'
        _LOGGER.critical(msg)
        raise RuntimeError(msg) from err


def _handler(
    event: Optional[Dict[str, Any]] = None,
    context: Optional[Any] = None,
) -> None:
    event_data = _data_from_pubsub_event(event)
    _LOGGER.debug('Processing event data: <%s>', event_data)
    event_request = request_parser.to_event_request(event_data)
    project_id = _project_id_from_context(context)
    process_request.process(event_request, project_id)
    _LOGGER.info('Processed event request: <%s>', event_request)


def _data_from_pubsub_event(event: Dict[str, Any]) -> Dict[str, Any]:
    # parse PubSub payload
    if not isinstance(event, dict):
        raise TypeError(f'Event must be of type {dict.__name__}. Got <{event}>({type(event)})')
    event_data = event.get('data')
    if event_data is None:
        raise ValueError(f'Event data is None. Event: <{event}>')
    try:
        decoded_event_data = base64.b64decode(event_data).decode('utf-8')
        result = json.loads(decoded_event_data)
    except Exception as err:
        msg = f'Cloud not parse PubSub event data. Event: <{event_data}>. Error: {err}'
        _LOGGER.critical(msg)
        raise RuntimeError(msg) from err
    # adds timestamp
    utc_ts: int = calendar.timegm(datetime.utcfromtimestamp(event.timestamp).utctimetuple())
    # field name from bq_sampler.request.EventRequest
    result['request_timestamp'] = utc_ts
    return result


def _project_id_from_context(context: Optional[Any] = None) -> Optional[str]:
    result = None
    if context is not None:
        resource_name = context.resource.get('name') if context.resource else None
        # something like: "projects/lhg-csv-moveit-upload/topics/cron-lhg-csv-moveit-upload"
        if resource_name:
            result = resource_name.split('/')[1]  # lhg-csv-moveit-upload
            _LOGGER.info('Got project ID <%s> from resource name <%s>', result, resource_name)
        else:
            _LOGGER.error('Context resource has no entry "name": %s', context)
    else:
        _LOGGER.error('There is no context object from which to extract the project ID')
    return result
