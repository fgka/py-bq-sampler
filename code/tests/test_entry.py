# vim: ai:sw=4:ts=4:sta:et:fo=croql
# pylint: disable=missing-module-docstring,missing-class-docstring,missing-function-docstring
# pylint: disable=missing-function-docstring,assignment-from-no-return,c-extension-no-member
# pylint: disable=protected-access,redefined-outer-name,no-self-use,using-constant-test
# pylint: disable=invalid-name,attribute-defined-outside-init,too-few-public-methods, redefined-builtin
# type: ignore
import base64
from datetime import datetime
import json
from typing import Any

import pytest

from bq_sampler import entry

from tests.entity import command_test_data


class _StubContext:
    def __init__(self, *, timestamp: str = ''):
        self.timestamp = timestamp


def test__from_pubsub_to_cmd_ok():
    # Given
    cmd = command_test_data.TEST_COMMAND_START
    timestamp = 123
    zulu_timestamp = datetime.utcfromtimestamp(timestamp).isoformat().replace('+00:00', '.35Z')
    context = _StubContext(timestamp=zulu_timestamp)
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


_ISO_DATE_STR_PREFIX: str = '2022-10-07T11:01:38'
_ISO_DATE_TO_TS: str = 1665140498


@pytest.mark.parametrize(
    'value,expected',
    [
        (_ISO_DATE_STR_PREFIX, _ISO_DATE_TO_TS),
        (_ISO_DATE_STR_PREFIX + '.1', _ISO_DATE_TO_TS),
        (_ISO_DATE_STR_PREFIX + '.12', _ISO_DATE_TO_TS),
        (_ISO_DATE_STR_PREFIX + '.123', _ISO_DATE_TO_TS),
        (_ISO_DATE_STR_PREFIX + 'Z', _ISO_DATE_TO_TS),
        (_ISO_DATE_STR_PREFIX + '.1Z', _ISO_DATE_TO_TS),
        (_ISO_DATE_STR_PREFIX + '.12Z', _ISO_DATE_TO_TS),
        (_ISO_DATE_STR_PREFIX + '.123Z', _ISO_DATE_TO_TS),
    ],
)
def test__extract_timestamp_from_iso_str_ok(value: str, expected: int):
    # Given/When
    result = entry._extract_timestamp_from_iso_str(value)
    # Then
    assert result == expected


def test_handler_nok_empty_args():
    # Given
    event = {}
    context = None
    # When
    response = entry.handler(event, context)
    # Then
    assert 'Could not process event:' in response
