# vim: ai:sw=4:ts=4:sta:et:fo=croql
# pylint: disable=line-too-long
"""
GCP `CloudFunction`_ entry point
.. _CloudFunction: https://cloud.google.com/functions/docs/quickstart-python
"""
# pylint: enable=line-too-long
from typing import Any, Dict, Optional
import logging
import os

import flask

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
    _LOGGER.debug('Event: <%s>', event)
    _LOGGER.debug('Context: <%s>', context)
    _LOGGER.debug('Environment: %s', str(os.environ))
    _LOGGER.info('Starting CSV upload')
    event_request = request_parser.to_event_request(event)
    project_id = _project_id_from_context(context)
    process_request.process(event_request, project_id)
    _LOGGER.info('Finished CSV upload')
    flask.jsonify(success=True)


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
