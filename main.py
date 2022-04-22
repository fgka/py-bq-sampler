# vim: ai:sw=4:ts=4:sta:et:fo=croql
# pylint: disable=line-too-long
"""
GCP CloudFunction mandatory entry point:
* https://cloud.google.com/functions/docs/writing#functions-writing-file-structuring-python
* https://cloud.google.com/functions/docs/writing/http
* https://cloud.google.com/functions/docs/tutorials/pubsub
"""
# pylint: enable=line-too-long
import base64
import logging
import os
from typing import Any, Optional

# From: https://cloud.google.com/logging/docs/setup/python
import google.cloud.logging

client = google.cloud.logging.Client()
client.get_default_handler()
client.setup_logging()

import flask  # pylint: disable=wrong-import-position

from bq_sampler import gcp_function  # pylint: disable=wrong-import-position

_LOGGER = logging.getLogger(__name__)


def handler(  # pylint: disable=unused-argument,keyword-arg-before-vararg
    event: Optional[dict] = None, context: Optional[Any] = None, *args, **kwargs
) -> Any:
    """Entry-point for GCP CloudFunction.
    This is just a proxy to keep the code organized in a pythonic way.

    Args:
        event (dict): The dictionary with data specific to this type of
                        event. The `@type` field maps to
                         `type.googleapis.com/google.pubsub.v1.PubsubMessage`.
                        The `data` field maps to the PubsubMessage data
                        in a base64-encoded string. The `attributes` field maps
                        to the PubsubMessage attributes if any is present.
        context (google.cloud.functions.Context): Metadata of triggering event
                        including `event_id` which maps to the PubsubMessage
                        messageId, `timestamp` which maps to the PubsubMessage
                        publishTime, `event_type` which maps to
                        `google.pubsub.topic.publish`, and `resource` which is
                        a dictionary that describes the service API endpoint
                        pubsub.googleapis.com, the triggering topic's name, and
                        the triggering event type
                        `type.googleapis.com/google.pubsub.v1.PubsubMessage`.
    Returns:
        Proxied response from the actual implementation.
    """
    _LOGGER.info('Processing event <%s> and context <%s>', event, context)
    event_data = event.get('data') if event is not None else None
    if event_data is not None:
        data = base64.b64decode(event_data).decode('utf-8')
        _LOGGER.info('Event data: <%s>', data)
    try:
        result = gcp_function.handler(event, context)
    except Exception as err:  # pylint: disable=broad-except
        msg = f'Error processing event <{event}> and context <{context}>. Error: {err}'
        _LOGGER.critical(msg)
        _LOGGER.warning('Environment: %s', str(os.environ))
        flask.abort(500, {'message': msg})
    return result
