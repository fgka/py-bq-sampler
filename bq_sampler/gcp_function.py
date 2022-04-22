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
    logging.debug('Event: <%s>', event)
    logging.debug('Context: <%s>', context)
    logging.debug('Environment: %s', str(os.environ))
    logging.info('Starting CSV upload')
    # TODO
    logging.info('Finished CSV upload')
    return flask.jsonify(success=True)
