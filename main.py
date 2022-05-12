# vim: ai:sw=4:ts=4:sta:et:fo=croql
# pylint: disable=line-too-long
"""
GCP CloudFunction mandatory entry point:
* https://cloud.google.com/functions/docs/writing#functions-writing-file-structuring-python
* https://cloud.google.com/functions/docs/writing/http
* https://cloud.google.com/functions/docs/tutorials/pubsub
"""
# pylint: enable=line-too-long
import os
from typing import Any, Callable, Dict, Optional

# From: https://cloud.google.com/logging/docs/setup/python
import google.cloud.logging

client = google.cloud.logging.Client()
client.get_default_handler()
client.setup_logging()

import flask  # pylint: disable=wrong-import-position

from bq_sampler import entry  # pylint: disable=wrong-import-position
from bq_sampler.notification import email  # pylint: disable=wrong-import-position
from bq_sampler.notification import sendgrid  # pylint: disable=wrong-import-position
from bq_sampler.notification import smtp  # pylint: disable=wrong-import-position
from bq_sampler import logger  # pylint: disable=wrong-import-position

_LOGGER = logger.get(__name__)


def handler(event: Optional[Dict[str, Any]] = None, context: Optional[Any] = None) -> None:
    """
    Entry-point for GCP CloudFunction.
    This is just a proxy to keep the code organized in a pythonic way.

    :param event: The dictionary with data specific to this type of
        event. The `@type` field maps to `type.googleapis.com/google.pubsub.v1.PubsubMessage`.
        The `data` field maps to the PubsubMessage data in a base64-encoded string.
        The `attributes` field maps to the PubsubMessage attributes if any is present.
    :param context: Metadata of triggering event including `event_id`
        which maps to the PubsubMessage messageId, `timestamp` which maps to the PubsubMessage
        publishTime, `event_type` which maps to `google.pubsub.topic.publish`,
        and `resource` which is a dictionary that describes the service API endpoint
        pubsub.googleapis.com, the triggering topic's name, and the triggering event type
        `type.googleapis.com/google.pubsub.v1.PubsubMessage`.
    :return:
    """
    _handler(entry.handler, event, context)


def _handler(
    handler_fn: Callable[[Dict[str, Any], Any], None] = None,
    event: Optional[Dict[str, Any]] = None,
    context: Optional[Any] = None,
) -> None:
    _LOGGER.debug(
        'Processing event <%s> and context <%s>. Environment: %s', event, context, str(os.environ)
    )
    try:
        handler_fn(event, context)
        _LOGGER.debug(
            'Finished processing event <%s> and context <%s>. Environment: %s',
            event,
            context,
            str(os.environ),
        )
        flask.jsonify(success=True)
    except Exception as err:  # pylint: disable=broad-except
        msg = (
            f'Could not process event: <{event}>,'
            f' context: <{context}>,'
            f' and environment: <{os.environ}>.'
            f' Error: {err}'
        )
        _LOGGER.critical(msg)
        flask.abort(500, {'message': msg})


def handler_sendgrid(event: Optional[Dict[str, Any]] = None, context: Optional[Any] = None) -> None:
    """
    Triggered from a message on a Pub/Sub topic to send through Sendgrid.

    :param event: Event payload.
    :param context: Metadata for the event.
    :return:
    """
    _handler(_sendgrid_handler_fn, event, context)


def _sendgrid_handler_fn(event: Optional[Dict[str, Any]] = None, _: Any = None) -> None:
    email.email_handler(
        event=event, event_to_msg_fn=sendgrid.event_to_msg_fn, sender_fn=sendgrid.sender_fn
    )


def handler_smtp(event: Optional[Dict[str, Any]] = None, context: Optional[Any] = None) -> None:
    """
    Triggered from a message on a Pub/Sub topic to send as email.

    :param event: Event payload.
    :param context: Metadata for the event.
    :return:
    """
    _handler(_smtp_handler_fn, event, context)


def _smtp_handler_fn(event: Optional[Dict[str, Any]] = None, _: Any = None) -> None:
    email.email_handler(event=event, event_to_msg_fn=smtp.event_to_msg_fn, sender_fn=smtp.sender_fn)
