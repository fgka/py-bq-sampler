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
import os
from typing import Any, Callable, Dict, Optional, Union

# From: https://cloud.google.com/logging/docs/setup/python
import google.cloud.logging

try:
    client = google.cloud.logging.Client()
    client.get_default_handler()
    client.setup_logging()
except Exception as log_err:  # pylint: disable=broad-except
    print(f'Could not start Google Client logging. Ignoring. Error: {log_err}')

# pylint: disable=wrong-import-position
import click
import flask
import functions_framework

from bq_sampler import entry, logger
from bq_sampler.notification import email, sendgrid, smtp

# pylint: enable=wrong-import-position

_LOGGER = logger.get(__name__)


@functions_framework.http
def handler_http(event: Optional[flask.Request] = None) -> str:
    # pylint: disable=line-too-long
    """
    Entry-point for GCP CloudFunction V2.
    This is just a proxy to keep the code organized in a pythonic way.
    Name is derived from Terraform module `cloud-foundation-fabric/cloud-function`_.
    This means that in the Terraform code you need to declare the ``handler``
        without the ``_http`` suffix, i.e., ``handler`` only to trigger this code-path.

    :param event: a :py:class:`flask.Request` with data specific to this type of
        event. The `@type` field maps to `type.googleapis.com/google.pubsub.v1.PubsubMessage`.
        The `data` field maps to the PubsubMessage data in a base64-encoded string.
        The `attributes` field maps to the PubsubMessage attributes if any is present.
    :return:

    .. _cloud-foundation-fabric/cloud-function:https://github.com/GoogleCloudPlatform/cloud-foundation-fabric/blob/master/modules/cloud-function/main.tf#L139
    """
    # pylint: enable=line-too-long
    return handler(event)


def handler(
    event: Optional[Union[flask.Request, Dict[str, Any]]] = None, context: Optional[Any] = None
) -> str:
    """
    Entry-point for GCP CloudFunction.
    This is just a proxy to keep the code organized in a pythonic way.

    :param event: The dictionary or :py:class:`flask.Request` with data specific to this type of
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
    return _handler(entry.handler, event, context)


def _handler(
    handler_fn: Callable[[Dict[str, Any], Any], None] = None,
    event: Optional[Union[flask.Request, Dict[str, Any]]] = None,
    context: Optional[Any] = None,
) -> str:
    _LOGGER.debug(
        'Processing event <%s> and context <%s>. Environment: %s', event, context, str(os.environ)
    )
    try:
        if isinstance(event, flask.Request):
            _LOGGER.debug('Event is of type %s. Extracting JSON payload.', type(event))
            event = event.get_json()
        result = handler_fn(event, context)
        _LOGGER.debug(
            'Finished processing event <%s> and context <%s>. Environment: %s',
            event,
            context,
            str(os.environ),
        )
    except Exception as err:  # pylint: disable=broad-except
        msg = (
            f'Could not process event: <{event}>({type(event)}),'
            f' context: <{context}>({type(context)}),'
            f' and environment: <{os.environ}>.'
            f' Error: {err}'
        )
        _LOGGER.exception(msg)
        raise RuntimeError(msg) from err
    return result


def handler_sendgrid(event: Optional[Dict[str, Any]] = None, context: Optional[Any] = None) -> str:
    """
    Triggered from a message on a Pub/Sub topic to send through Sendgrid.

    :param event: Event payload.
    :param context: Metadata for the event.
    :return:
    """
    return _handler(_sendgrid_handler_fn, event, context)


def _sendgrid_handler_fn(event: Optional[Dict[str, Any]] = None, _: Any = None) -> str:
    return email.email_handler(
        event=event, event_to_msg_fn=sendgrid.event_to_msg_fn, sender_fn=sendgrid.sender_fn
    )


def handler_smtp(event: Optional[Dict[str, Any]] = None, context: Optional[Any] = None) -> str:
    """
    Triggered from a message on a Pub/Sub topic to send as email.

    :param event: Event payload.
    :param context: Metadata for the event.
    :return:
    """
    return _handler(_smtp_handler_fn, event, context)


def _smtp_handler_fn(event: Optional[Dict[str, Any]] = None, _: Any = None) -> str:
    return email.email_handler(
        event=event, event_to_msg_fn=smtp.event_to_msg_fn, sender_fn=smtp.sender_fn
    )


####################
# Terminal Testing #
####################

_DEFAULT_EMAIL_JSON_PAYLOAD: str = '{"type": "TEST"}'


@click.group(help='Use this to test your Cloud Function handlers locally.')
def cli() -> None:
    """
    Click entry-point
    :return:
    """


@cli.command(help='SMTP email sender')
@click.option('--config-uri', '-c', required=True, type=str, help='SMTP Cloud Storage config URI')
@click.option(
    '--json-payload',
    '-d',
    default=_DEFAULT_EMAIL_JSON_PAYLOAD,
    required=False,
    type=str,
    help='What to send as an event.',
)
def smtp_sender(config_uri: str, json_payload: str) -> None:
    """
    CLI wrapper to :py:func:`handler_smtp`.

    :param config_uri:
    :param json_payload:
    :return:
    """
    os.environ[smtp.SMTP_CONFIG_URI_ENV_VAR] = config_uri
    handler_smtp(_create_event_str(json_payload))


@cli.command(help='SendGrid email sender')
@click.option(
    '--config-uri', '-c', required=True, type=str, help='SendGrid Cloud Storage config URI'
)
@click.option(
    '--json-payload',
    '-d',
    default=_DEFAULT_EMAIL_JSON_PAYLOAD,
    required=False,
    type=str,
    help='What to send as an event.',
)
def sendgrid_sender(config_uri: str, json_payload: str) -> None:
    """
    CLI wrapper to :py:func:`handler_sendgrid`.

    :param config_uri:
    :param json_payload:
    :return:
    """
    os.environ[sendgrid.SENDGRID_CONFIG_URI_ENV_VAR] = config_uri
    handler_sendgrid(_create_event_str(json_payload))


def _create_event_str(data: str) -> str:
    data_str = base64.b64encode(bytes(data.encode('utf-8')))
    return dict(data=data_str)


if __name__ == '__main__':
    cli()
