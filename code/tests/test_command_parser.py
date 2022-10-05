# vim: ai:sw=4:ts=4:sta:et:fo=croql
# pylint: disable=missing-module-docstring,missing-class-docstring,missing-function-docstring
# pylint: disable=missing-function-docstring,assignment-from-no-return,c-extension-no-member
# pylint: disable=protected-access,redefined-outer-name,no-self-use,using-constant-test
# pylint: disable=invalid-name,attribute-defined-outside-init,too-few-public-methods, redefined-builtin
# type: ignore
import pytest

from bq_sampler import command_parser
from bq_sampler.entity import command

from tests.entity import command_test_data


@pytest.mark.parametrize(
    'value',
    [
        command_test_data.TEST_COMMAND_START,
        command_test_data.TEST_COMMAND_SAMPLE_POLICY_PREFIX,
        command_test_data.TEST_COMMAND_SAMPLE_START,
        command_test_data.TEST_COMMAND_SAMPLE_DONE,
    ],
)
def test_to_command_ok(value: command.CommandBase):
    # Given
    timestamp = 31
    # When
    result = command_parser.to_command(value.as_dict(), timestamp)
    # Then
    for key in result.as_dict():
        if key == command.CommandBase.timestamp.__name__:
            assert result.timestamp == timestamp
        else:
            assert getattr(value, key) == getattr(result, key)
