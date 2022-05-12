# vim: ai:sw=4:ts=4:sta:et:fo=croql
# pylint: disable=line-too-long
"""
Wrapper for `SendGrid`_ based on GCP's `Enabling real-time email and chat notifications`_ documentation.

.. _SendGrid: https://app.sendgrid.com/
.. _Enabling real-time email and chat notifications: https://cloud.google.com/security-command-center/docs/how-to-enable-real-time-notifications#setting_up_a_messaging_app
"""
# pylint: enable=line-too-long
# pylint: disable=duplicate-code
import os
from typing import Union

import cachetools

import sendgrid
from sendgrid.helpers import mail

from bq_sampler import logger
from bq_sampler import const
from bq_sampler.entity import config
from bq_sampler.gcp import secrets
from bq_sampler.gcp import gcs
from bq_sampler.gcp import pubsub

_LOGGER = logger.get(__name__)
# pylint: enable=duplicate-code

_SENDGRID_CONFIG_URI_ENV_VAR: str = 'SENDGRID_CONFIG_URI'


class _GeneralConfig:  # pylint: disable=too-few-public-methods
    def __init__(self):
        self.sendgrid_config_uri = os.environ.get(_SENDGRID_CONFIG_URI_ENV_VAR)


def event_to_msg_fn(event_data: Union[str, bytes]) -> mail.Mail:
    """
    Will extract the message from `event` to be sent as an email message body.

    :param event_data:
    :return:
    """
    body = const.NOTIFICATION_PUBSUB_CONTENT_MESSAGE_TMPL.format(pubsub.parse_json_data(event_data))
    return mail.Mail(
        from_email=_config().sender,
        to_emails=_config().recipients,
        subject=_config().subject,
        html_content=body,
    )


@cachetools.cached(cache=cachetools.LRUCache(maxsize=1))
def _config() -> config.SendGrid:
    bucket_name, object_path = gcs.bucket_path_from_uri(_general_config().sendgrid_config_uri)
    content = gcs.read_object(bucket_name, object_path)
    return config.SendGrid.from_json(content)


@cachetools.cached(cache=cachetools.LRUCache(maxsize=1))
def _general_config() -> _GeneralConfig:
    return _GeneralConfig()


def sender_fn(message: mail.Mail) -> None:
    """
    Will send the `message` as an email.

    :param message:
    :return:
    """
    response = _client().send(message)
    _LOGGER.info(
        'Email sent with status %s and response %s. Response headers: %s',
        response.status_code,
        response.body,
        response.headers,
    )


@cachetools.cached(cache=cachetools.LRUCache(maxsize=1))
def _client() -> sendgrid.SendGridAPIClient:
    api_key = secrets.secret(_config().api_key_secret_name)
    return sendgrid.SendGridAPIClient(api_key)
