# vim: ai:sw=4:ts=4:sta:et:fo=croql
# pylint: disable=missing-module-docstring,missing-class-docstring,missing-function-docstring
# pylint: disable=missing-function-docstring,assignment-from-no-return,c-extension-no-member
# pylint: disable=protected-access,redefined-outer-name,no-self-use,using-constant-test
# pylint: disable=invalid-name,attribute-defined-outside-init,too-few-public-methods, redefined-builtin
# pylint: disable=unspecified-encoding
# type: ignore
import pathlib

from bq_sampler.entity import config

_TEST_DATA_DIR: pathlib.Path = pathlib.Path(__file__).parent.parent.parent.joinpath('test_data')
_TEST_CONFIG_DIR: pathlib.Path = _TEST_DATA_DIR / 'config_json'
_TEST_SMTP_JSON: pathlib.Path = _TEST_CONFIG_DIR / 'smtp_config.json'
_TEST_SENDGRID_JSON: pathlib.Path = _TEST_CONFIG_DIR / 'sendgrid_config.json'


def test_smtp_from_json_ok():
    # Given
    json_str = None
    with open(_TEST_SMTP_JSON, 'r') as in_file:
        json_str = in_file.read()
    # When
    result = config.Smtp.from_json(json_str)
    # Then
    assert isinstance(result, config.Smtp)


def test_sendgrid_from_json_ok():
    # Given
    json_str = None
    with open(_TEST_SENDGRID_JSON, 'r') as in_file:
        json_str = in_file.read()
    # When
    result = config.SendGrid.from_json(json_str)
    # Then
    assert isinstance(result, config.SendGrid)
