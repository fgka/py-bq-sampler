# vim: ai:sw=4:ts=4:sta:et:fo=croql
# pylint: disable=missing-module-docstring,missing-class-docstring,missing-function-docstring
# pylint: disable=missing-function-docstring,assignment-from-no-return,c-extension-no-member
# pylint: disable=protected-access,redefined-outer-name,no-self-use,using-constant-test
# pylint: disable=invalid-name,attribute-defined-outside-init,too-few-public-methods, redefined-builtin
# type: ignore
import base64
import json
from typing import Any

from bq_sampler import entry

from tests.entity import command_test_data


class _StubContext:
    def __init__(self, *, timestamp: int = 0):
        self.timestamp = timestamp


def test__from_pubsub_to_cmd_ok():
    # Given
    cmd = command_test_data.TEST_COMMAND_START
    timestamp = 123
    context = _StubContext(timestamp=timestamp)
    event = _create_event_str(cmd.as_dict())
    # When
    result = entry._from_pubsub_to_cmd(event, context)
    # Then
    assert isinstance(result, cmd.__class__)
    assert result.type == cmd.type
    assert result.timestamp == timestamp


def _create_event_str(data: Any) -> str:
    data_str = base64.b64encode(bytes(json.dumps(data).encode('utf-8')))
    return dict(data=data_str)
