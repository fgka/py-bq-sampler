# vim: ai:sw=4:ts=4:sta:et:fo=croql
# pylint: disable=missing-module-docstring,missing-class-docstring,missing-function-docstring
# pylint: disable=missing-function-docstring,assignment-from-no-return,c-extension-no-member
# pylint: disable=protected-access,redefined-outer-name,no-self-use,using-constant-test
# pylint: disable=invalid-name,attribute-defined-outside-init,too-few-public-methods, redefined-builtin
# type: ignore
import json
from typing import Any, Callable, Dict

import pytest

from bq_sampler.notification import email


_TEST_EVENT: Dict[str, Any] = {'data': {'key_a': 'value_a', 'key_b': 123}}
_TEST_EVENT_TO_MSG_FN: Callable[[Dict[str, Any]], Any] = json.dumps
_TEST_SENDER_FN: Callable[[Any], None] = lambda _: None


def test_email_handler_ok():
    # Given
    event = _TEST_EVENT
    called_event_to_msg_fn = False
    called_sender_fn = False

    def event_to_msg_fn(value: Dict[str, Any]) -> str:
        nonlocal called_event_to_msg_fn
        assert value == event.get('data')
        called_event_to_msg_fn = True
        return json.dumps(value)

    def sender_fn(message: Any) -> None:
        nonlocal called_sender_fn
        assert isinstance(message, str)
        called_sender_fn = True

    # When
    email.email_handler(event=event, event_to_msg_fn=event_to_msg_fn, sender_fn=sender_fn)
    # Then
    assert called_event_to_msg_fn
    assert called_sender_fn


@pytest.mark.parametrize(
    'event,event_to_msg_fn,sender_fn',
    [
        (None, _TEST_EVENT_TO_MSG_FN, _TEST_SENDER_FN),
        ('', _TEST_EVENT_TO_MSG_FN, _TEST_SENDER_FN),
        (_TEST_EVENT, None, _TEST_SENDER_FN),
        (_TEST_EVENT, _TEST_EVENT_TO_MSG_FN, None),
    ],
)
def test_email_handler_nok_wrong_types(
    event: Dict[str, Any],
    event_to_msg_fn: Callable[[Dict[str, Any]], Any],
    sender_fn: Callable[[Any], None],
):
    # Given/When/Then
    with pytest.raises(TypeError):
        email.email_handler(event=event, event_to_msg_fn=event_to_msg_fn, sender_fn=sender_fn)


def test_email_handler_nok_event_to_msg_fn():
    # Given/When/Then
    with pytest.raises(RuntimeError):
        email.email_handler(event=_TEST_EVENT, event_to_msg_fn=_raise_fn, sender_fn=_TEST_SENDER_FN)


def _raise_fn(*args, **kwarg) -> None:
    raise ValueError()


def test_email_handler_nok_sender_fn():
    # Given/When/Then
    with pytest.raises(RuntimeError):
        email.email_handler(
            event=_TEST_EVENT, event_to_msg_fn=_TEST_EVENT_TO_MSG_FN, sender_fn=_raise_fn
        )
