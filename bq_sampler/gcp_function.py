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

from bq_sampler.dto import request
from bq_sampler import process_request

_LOGGER = logging.getLogger(__name__)


def handler(  # pylint: disable=unused-argument
    event: Optional[Dict[str, Any]] = None,
    context: Optional[Any] = None,
    *,
    use_env_var_gcs_path: Optional[bool] = False,
    add_env_var_moveit_server: Optional[bool] = False,
) -> Any:
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
    event_request = _parse_request(event, context)
    process_request.process(event_request)
    _LOGGER.info('Finished CSV upload')
    return flask.jsonify(success=True)


def _parse_request(
    event: Optional[Dict[str, Any]] = None, context: Optional[Any] = None
) -> request.EventRequest:
    # TODO
    pass
