# vim: ai:sw=4:ts=4:sta:et:fo=croql
# pylint: disable=line-too-long
"""
Wrapper for `SendGrid`_ based on GCP's `Enabling real-time email and chat notifications`_ documentation.

.. _SendGrid: https://app.sendgrid.com/
.. _Enabling real-time email and chat notifications: https://cloud.google.com/security-command-center/docs/how-to-enable-real-time-notifications#setting_up_a_messaging_app
"""
# pylint: enable=line-too-long
# pylint: disable=duplicate-code
from email import message
import os
import smtplib
from typing import Union

import cachetools

from bq_sampler import const, logger
from bq_sampler.entity import config
from bq_sampler.gcp import gcs, pubsub, secrets

_LOGGER = logger.get(__name__)
# pylint: enable=duplicate-code

SMTP_CONFIG_URI_ENV_VAR: str = 'SMTP_CONFIG_URI'


class _GeneralConfig:  # pylint: disable=too-few-public-methods
    def __init__(self):
        self.smtp_config_uri = os.environ.get(SMTP_CONFIG_URI_ENV_VAR)


def event_to_msg_fn(event: Union[str, bytes]) -> message.EmailMessage:
    """
    Will extract the message from `event` to be sent as an email message body.

    :param event:
    :return:
    """
    # pylint: disable=line-too-long
    result = message.EmailMessage()
    result.set_content(
        const.NOTIFICATION_PUBSUB_CONTENT_MESSAGE_TMPL.format(pubsub.parse_json_data(event))
    )
    result['Subject'] = _config().subject
    result['From'] = _config().sender
    result['To'] = ','.join(_config().recipients)
    return result


def sender_fn(content: message.EmailMessage) -> None:
    """
    Will send the `message` as an email.

    :param content:
    :return:
    """
    server = _server()
    server.ehlo()
    if _config().use_tls:
        server.starttls()
    password = secrets.secret(_config().password_secret_name)
    server.login(_config().username, password)
    server.send_message(content)
    server.close()


@cachetools.cached(cache=cachetools.LRUCache(maxsize=1))
def _server() -> smtplib.SMTP:
    return smtplib.SMTP(_config().smtp_server, _config().smtp_port)


@cachetools.cached(cache=cachetools.LRUCache(maxsize=1))
def _config() -> config.Smtp:
    bucket_name, object_path = gcs.bucket_path_from_uri(_general_config().smtp_config_uri)
    content = gcs.read_object(bucket_name, object_path)
    return config.Smtp.from_json(content, f'gs://{bucket_name}/{object_path}')


@cachetools.cached(cache=cachetools.LRUCache(maxsize=1))
def _general_config() -> _GeneralConfig:
    return _GeneralConfig()
