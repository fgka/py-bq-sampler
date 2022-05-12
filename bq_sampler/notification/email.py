# vim: ai:sw=4:ts=4:sta:et:fo=croql
"""
Wrapper for sending emails. This is a partial implementation.
"""
import os
from typing import Any, Callable, Dict

from bq_sampler import logger

_LOGGER = logger.get(__name__)


def email_handler(
    *,
    event: Dict[str, Any],
    event_to_msg_fn: Callable[[Dict[str, Any]], Any],
    sender_fn: Callable[[Any], None],
) -> None:
    """
    Triggered from a message on a Pub/Sub topic.

    :param event: Event payload.
    :param event_to_msg_fn:
    :param sender_fn:
    """
    _LOGGER.debug('Handling event <%s> to send as email', event)
    # input validation
    if not isinstance(event, dict):
        raise TypeError(f'Event must be of type {dict.__name__}. Got: <{event}>({type(event)})')
    if not callable(event_to_msg_fn):
        raise TypeError(
            'Event to message function must be a callable.'
            f' Got: <{event_to_msg_fn}>({type(event_to_msg_fn)})'
        )
    if not callable(sender_fn):
        raise TypeError(f'Sender must be a callable. Got: <{sender_fn}>({type(sender_fn)})')
    # logic
    message = None
    try:
        message: Any = event_to_msg_fn(event.get('data'))
        _LOGGER.debug('Message to be sent: <%s>', message)
        sender_fn(message)
    except Exception as err:
        raise RuntimeError(
            f'Failed to send email. Message: {message}.'
            f' Environment {os.environ}.'
            f' Event: {event}, error: {err}'
        ) from err
    _LOGGER.debug('Message sent for event: %s', event)
