# vim: ai:sw=4:ts=4:sta:et:fo=croql
# pylint: disable=missing-module-docstring,missing-class-docstring,missing-function-docstring
# pylint: disable=missing-function-docstring,assignment-from-no-return,c-extension-no-member
# pylint: disable=protected-access,redefined-outer-name,no-self-use,using-constant-test
# pylint: disable=invalid-name,attribute-defined-outside-init,too-few-public-methods, redefined-builtin
# type: ignore
import base64
from email import message
import json
from typing import Any, Dict

from bq_sampler.entity import config
from bq_sampler.notification import smtp


_TEST_EVENT: Dict[str, Any] = {'key_a': 'value_a', 'key_b': 123}
_TEST_SMTP_DTO: config.Smtp = config.Smtp(
    subject='TEST_SUBJECT',
    sender='sender@example.com',
    recipients=['recipient_a@example.com', 'recipient_b@example.com'],
    smtp_server='smtp.example.com',
    smtp_port=587,
    use_tls=True,
    username='test.username',
    password_secret_name='test.password.secret.name',
)


def test_event_to_msg_fn_ok(monkeypatch):
    # Given
    cfg = _TEST_SMTP_DTO
    event = _TEST_EVENT
    _mock_config(monkeypatch, cfg)
    # When
    result = smtp.event_to_msg_fn(_create_event_str(event))
    # Then
    assert isinstance(result, message.EmailMessage)
    assert result.get('From') == cfg.sender
    assert result.get('Subject') == cfg.subject
    for email in cfg.recipients:
        assert email in result.get('To')
    assert str(event) in result.as_string()


def _create_event_str(data: Any) -> str:
    return base64.b64encode(bytes(json.dumps(data).encode('utf-8')))


class _StubbedServer:
    def __init__(self, cfg: config.Smtp):
        self._config = cfg
        self._called = {
            _StubbedServer.ehlo.__name__: False,
            _StubbedServer.login.__name__: False,
            _StubbedServer.send_message.__name__: False,
            _StubbedServer.close.__name__: False,
            _StubbedServer.starttls.__name__: False,
        }
        self.password = None
        self.msg = None

    def ehlo(self) -> None:
        self._called[_StubbedServer.ehlo.__name__] = True

    def starttls(self) -> None:
        self._called[_StubbedServer.starttls.__name__] = True

    def login(self, username, password) -> None:
        assert username == self._config.username
        self.password = password
        self._called[_StubbedServer.login.__name__] = True

    def send_message(self, msg: message.EmailMessage) -> None:
        self.msg = msg
        self._called[_StubbedServer.send_message.__name__] = True

    def close(self) -> None:
        self._called[_StubbedServer.close.__name__] = True

    def are_all_called(self) -> bool:
        return all(self._called.values())


def test_sender_fn_ok(monkeypatch):
    # Given
    cfg = _TEST_SMTP_DTO
    password = 'TEST_PASSWORD'
    server = _StubbedServer(cfg)
    _mock_server(monkeypatch, server)
    _mock_secrets(monkeypatch, cfg.password_secret_name, password)
    _mock_config(monkeypatch, cfg)
    msg = str(_TEST_EVENT)
    # When
    smtp.sender_fn(msg)
    # Then
    assert server.are_all_called()
    assert server.password == password
    assert server.msg == msg


def _mock_server(monkeypatch, server: _StubbedServer) -> None:
    monkeypatch.setattr(smtp, '_server', lambda: server)


def _mock_secrets(monkeypatch, name: str, secret: str) -> None:
    def mocked_secret(value: str) -> str:
        assert name == value
        return secret

    monkeypatch.setattr(smtp.secrets, 'secret', mocked_secret)


def _mock_config(monkeypatch, cfg: config.Smtp) -> None:
    def mocked_config() -> config.Smtp:
        return cfg

    monkeypatch.setattr(smtp, '_config', mocked_config)
