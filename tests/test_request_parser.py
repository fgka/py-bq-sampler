# vim: ai:sw=4:ts=4:sta:et:fo=croql
# pylint: disable=missing-module-docstring,missing-class-docstring,missing-function-docstring
# pylint: disable=missing-function-docstring,assignment-from-no-return,c-extension-no-member
# pylint: disable=protected-access,redefined-outer-name,no-self-use,using-constant-test
# pylint: disable=invalid-name,attribute-defined-outside-init,too-few-public-methods, redefined-builtin
# type: ignore
import pytest

from bq_sampler import request_parser
from bq_sampler.entity import request

from tests import request_test_data


@pytest.mark.parametrize(
    'value',
    [
        request_test_data.TEST_EVENT_REQUEST_START,
        request_test_data.TEST_EVENT_REQUEST_SAMPLE_START,
        request_test_data.TEST_EVENT_REQUEST_SAMPLE_DONE,
    ],
)
def test_to_event_request_ok(value: request.EventRequest):
    # Given
    request_timestamp = 31
    # When
    result = request_parser.to_event_request(value.as_dict(), request_timestamp)
    # Then
    for key in result.as_dict():
        if key == 'request_timestamp':
            assert result.request_timestamp == request_timestamp
        else:
            assert getattr(value, key) == getattr(result, key)
