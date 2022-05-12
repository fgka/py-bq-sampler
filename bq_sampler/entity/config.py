# vim: ai:sw=4:ts=4:sta:et:fo=croql
"""
DTOs for notification methods.
"""
from typing import List

import attrs

from bq_sampler import const
from bq_sampler.entity import attrs_defaults


@attrs.define(**const.ATTRS_DEFAULTS)
class Email(attrs_defaults.HasFromJsonString):  # pylint: disable=too-few-public-methods
    """
    Email general config object, e.g.::
        config = Email(
            subject='[Email] This is coming from an email sender',
            sender='me@example.com',
            recipients=[
                'you@example.com',
            ],
        )
    """

    subject: str = attrs.field(validator=attrs.validators.instance_of(str))
    sender: str = attrs.field(validator=attrs.validators.instance_of(str))
    recipients: List[str] = attrs.field(
        validator=attrs.validators.deep_iterable(
            member_validator=attrs.validators.instance_of(str),
            iterable_validator=attrs.validators.instance_of(list),
        )
    )


@attrs.define(**const.ATTRS_DEFAULTS)
class SendGrid(Email):  # pylint: disable=too-few-public-methods
    """
    SendGrid config object, e.g.::
        config = SendGrid(
            api_key_secret_name='my_sendgrid_api_key_secret_name',
            subject='[SendGrid] This is coming from SendGrid',
            sender='me@example.com',
            recipients=[
                'you@example.com',
            ],
        )

    """

    api_key_secret_name: str = attrs.field(validator=attrs.validators.instance_of(str))


@attrs.define(**const.ATTRS_DEFAULTS)
class Smtp(Email):  # pylint: disable=too-few-public-methods
    """
    SMTP configuration, e.g.::
        config = Smtp(
            username='my_smtp_user',
            password_secret_name='my_smtp_pass_secret_name',
            smtp_server='smtp.gmail.com',
            smtp_port=587,
            use_tls=True,
            subject='[SMTP] This is sent using SMTP',
            sender='me@example.com',
            recipients=[
                'you@example.com',
            ],
        )
    """

    username: str = attrs.field(validator=attrs.validators.instance_of(str))
    password_secret_name: str = attrs.field(validator=attrs.validators.instance_of(str))
    smtp_server: str = attrs.field(validator=attrs.validators.instance_of(str))
    smtp_port: int = attrs.field(validator=attrs.validators.instance_of(int))
    use_tls: bool = attrs.field(default=True, validator=attrs.validators.instance_of(bool))
