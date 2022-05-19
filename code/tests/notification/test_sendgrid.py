# vim: ai:sw=4:ts=4:sta:et:fo=croql
# pylint: disable=missing-module-docstring,missing-class-docstring,missing-function-docstring
# pylint: disable=missing-function-docstring,assignment-from-no-return,c-extension-no-member
# pylint: disable=protected-access,redefined-outer-name,no-self-use,using-constant-test
# pylint: disable=invalid-name,attribute-defined-outside-init,too-few-public-methods, redefined-builtin
# type: ignore
import base64
import json
from typing import Any, Dict

import sendgrid
from sendgrid.helpers import mail

from bq_sampler.notification import sendgrid
from bq_sampler.entity import config


_TEST_EVENT: Dict[str, Any] = {'key_a': 'value_a', 'key_b': 123}
_TEST_SENDGRID_DTO: config.SendGrid = config.SendGrid(
    subject='TEST_SUBJECT',
    sender='sender@example.com',
    recipients=['recipient_a@example.com', 'recipient_b@example.com'],
    api_key_secret_name='TEST_API_KEY_NAME',
)


def test_event_to_msg_fn_ok(monkeypatch):
    # Given
    cfg = _TEST_SENDGRID_DTO
    event = _TEST_EVENT
    _mock_config(monkeypatch, cfg)
    # When
    result = sendgrid.event_to_msg_fn(event_data=_create_event_str(event))
    # Then
    assert isinstance(result, mail.Mail)
    res_dict = result.get()
    assert res_dict.get('from', {}).get('email') == cfg.sender
    for pers in res_dict.get('personalizations', []):
        for to in pers.get('to', []):
            assert to.get('email') in cfg.recipients
    assert res_dict.get('subject') == cfg.subject
    assert str(event) in res_dict.get('content', [{}])[0].get('value')


def _create_event_str(data: Any) -> str:
    return base64.b64encode(bytes(json.dumps(data).encode('utf-8')))


def _mock_config(monkeypatch, cfg: config.SendGrid) -> None:
    def mocked_config() -> config.SendGrid:
        return cfg

    monkeypatch.setattr(sendgrid, '_config', mocked_config)


def test_sender_fn_ok(monkeypatch):
    # Given
    client = _mock_client(monkeypatch)
    message = mail.Mail()
    # When
    sendgrid.sender_fn(message)
    # Then
    assert client.called


class _StubbedResponse:
    def __init__(self):
        self.status_code = 200
        self.body = "TEST_BODY"
        self.headers = {}


class _Object(object):  # pylint: disable=useless-object-inheritance
    """
    This is necessary since `obj = object()` will not add the `__dict__` attribute
        which is necessary to be able to dynamically add attributes to the instance.
    """


class _StubbedClient:
    def __init__(self):
        self.called = False
        self.client = _Object()
        # pylint: disable=no-member
        setattr(self.client, 'mail', _Object())
        setattr(self.client.mail, 'send', _Object())
        setattr(self.client.mail.send, 'post', self.post)

    def post(self, request_body: Dict[str, Any]) -> _StubbedResponse:
        assert isinstance(request_body, dict)
        self.called = True
        return _StubbedResponse()

    def send(self, message: mail.Mail) -> _StubbedResponse:
        assert isinstance(message, mail.Mail)
        return self.post(message.get())


def _mock_client(monkeypatch) -> _StubbedClient:
    result = _StubbedClient()
    monkeypatch.setattr(sendgrid, '_client', lambda: result)
    return result
