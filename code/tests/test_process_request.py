# vim: ai:sw=4:ts=4:sta:et:fo=croql
# pylint: disable=missing-module-docstring,missing-class-docstring,missing-function-docstring
# pylint: disable=missing-function-docstring,assignment-from-no-return,c-extension-no-member
# pylint: disable=protected-access,redefined-outer-name,no-self-use,using-constant-test
# pylint: disable=invalid-name,attribute-defined-outside-init,too-few-public-methods, redefined-builtin
# type: ignore
import types
from typing import Any, Dict, List, Optional

import pytest

from bq_sampler import const, process_request
from bq_sampler.entity import command, table

from tests.entity import sample_policy_data, command_test_data
from tests.gcp import gcs_on_disk

_GENERAL_POLICY_PATH: str = 'default_policy.json'
_SAMPLING_LOCK_PATH: str = 'lock_sampling'


class _StubGeneralConfig:  # pylint: disable=too-many-instance-attributes
    def __init__(self):
        self.target_location = None
        self.target_project_id = None
        self.policy_bucket = None
        self.default_policy_path = None
        self.request_bucket = None
        self.pubsub_request = None
        self.pubsub_error = None
        self.sampling_lock_path = None
        self.pubsub_bq_notification = None
        self.bq_transfer_sa = None


@pytest.mark.parametrize(
    'cmd,process_fn',
    [
        (command_test_data.TEST_COMMAND_START, '_process_start'),
        (command_test_data.TEST_COMMAND_SAMPLE_START, '_process_sample_start'),
        (command_test_data.TEST_COMMAND_SAMPLE_DONE, '_process_sample_done'),
    ],
)
def test_process_ok(monkeypatch, cmd: command.CommandBase, process_fn: str):
    # Given
    called = False

    def mocked_process(value: command.CommandBase) -> None:
        nonlocal called
        called = True
        assert value == cmd

    monkeypatch.setattr(process_request, process_fn, mocked_process)
    # When
    process_request.process(cmd)
    # Then
    assert called


def test_process_nok(monkeypatch):
    # Given
    cmd = command_test_data.TEST_COMMAND_START
    error = TypeError('TEST_MESSAGE')
    called = False
    config = _StubGeneralConfig()
    config.pubsub_error = 'PUBSUB_ERROR'

    def mocked_process(_) -> None:
        raise error

    def mocked_publish(value: Dict[str, Any], topic_path: str) -> str:
        nonlocal called
        called = True
        value_event = cmd.__class__.from_dict(value.get(process_request._PUBSUB_ERROR_CMD_ENTRY))
        assert value_event == cmd
        assert value.get(process_request._PUBSUB_ERROR_MSG_ENTRY) == str(error)
        assert topic_path == config.pubsub_error

    _mock_general_config(monkeypatch, config)
    monkeypatch.setattr(process_request, '_process_start', mocked_process)
    monkeypatch.setattr(process_request.pubsub, 'publish', mocked_publish)
    # When
    with pytest.raises(RuntimeError):
        process_request.process(cmd, with_retry=False)
        # Then
        assert called


def _mock_general_config(monkeypatch, config: _StubGeneralConfig) -> None:
    def mocked_config() -> Any:
        return config

    monkeypatch.setattr(process_request, '_general_config', mocked_config)


def test__create_sample_start_request_ok(monkeypatch):
    # Given
    cmd = command_test_data.TEST_COMMAND_START
    table_policy = sample_policy_data.TEST_TABLE_POLICY
    table_sample = sample_policy_data.TEST_TABLE_SAMPLE
    target_location = sample_policy_data.TEST_TGT_LOCATION
    config = _StubGeneralConfig()
    config.target_project_id = 'TARGET_PROJECT_ID'
    _mock_general_config(monkeypatch, config)
    # When
    result = process_request._create_sample_start_cmd(
        cmd, table_policy, table_sample, target_location
    )
    # Then
    assert isinstance(result, command.CommandSampleStart)
    assert result.timestamp == cmd.timestamp
    assert result.type == command.CommandType.SAMPLE_START.value
    assert result.sample_request == table_sample
    res_tgt_table = result.target_table
    assert res_tgt_table.project_id != table_sample.table_reference.project_id
    assert res_tgt_table.project_id == config.target_project_id
    assert res_tgt_table.dataset_id == table_sample.table_reference.dataset_id
    assert res_tgt_table.table_id == table_sample.table_reference.table_id
    assert res_tgt_table.location == sample_policy_data.TEST_TGT_LOCATION
    assert res_tgt_table.location != table_sample.table_reference.location


def test__process_start_ok(monkeypatch):
    # Given
    cmd = command_test_data.TEST_COMMAND_START
    config = _StubGeneralConfig()
    config.target_location = 'TEST_LOCATION'
    config.default_policy_path = _GENERAL_POLICY_PATH
    config.policy_bucket = gcs_on_disk.POLICY_BUCKET
    config.request_bucket = 'REQUEST_BUCKET'
    config.pubsub_request = 'PUBSUB_REQUEST'
    config.sampling_lock_path = _SAMPLING_LOCK_PATH
    sample_policy_prefix_req_lst: List[command.CommandSampleStart] = []
    called_drop = False
    called_remove = False
    called_removed_transfer_config = False
    called_publish = False

    def mocked_drop_all_sample_tables(  # pylint: disable=unused-argument
        *,
        project_id: str,
        labels: Optional[Dict[str, str]] = None,
    ) -> None:
        nonlocal called_drop
        assert project_id == config.target_project_id
        called_drop = True

    def mocked_publish(value: Dict[str, Any], topic_path: str) -> str:
        assert topic_path == config.pubsub_request
        nonlocal sample_policy_prefix_req_lst
        nonlocal called_publish
        sample_policy_prefix_req_lst.append(command.CommandSamplePolicyPrefix.from_dict(value))
        called_publish = True

    def mock_remove_all_empty_sample_datasets(  # pylint: disable=unused-argument
        *,
        project_id: str,
        labels: Optional[Dict[str, str]] = None,
    ) -> None:
        nonlocal called_remove
        assert project_id == config.target_project_id
        called_remove = True

    def mock_remove_all_transfer_config(*, project_id: str, location: str) -> None:
        nonlocal called_removed_transfer_config
        assert project_id == config.target_project_id
        assert location == config.target_location
        called_removed_transfer_config = True

    _mock_general_config(monkeypatch, config)
    monkeypatch.setattr(process_request.sampler_bucket.gcs, 'read_object', gcs_on_disk.read_object)
    monkeypatch.setattr(
        process_request.sampler_bucket.gcs,
        '_get_gcs_prefixes_http_iterator',
        gcs_on_disk.get_gcs_prefixes_http_iterator,
    )
    monkeypatch.setattr(
        process_request.sampler_bucket.gcs, '_list_blob_names', gcs_on_disk.list_blob_names
    )
    monkeypatch.setattr(
        process_request.sampler_query, 'drop_all_sample_tables', mocked_drop_all_sample_tables
    )
    monkeypatch.setattr(
        process_request.sampler_query,
        'remove_all_empty_sample_datasets',
        mock_remove_all_empty_sample_datasets,
    )
    monkeypatch.setattr(
        process_request.sampler_query,
        'remove_all_transfer_config',
        mock_remove_all_transfer_config,
    )
    monkeypatch.setattr(process_request.pubsub, 'publish', mocked_publish)
    # When
    process_request._process_start(cmd)
    # Then
    assert called_drop
    assert called_remove
    assert called_removed_transfer_config
    assert called_publish
    assert len(sample_policy_prefix_req_lst) == 3
    for start_prefix_req in sample_policy_prefix_req_lst:
        assert isinstance(start_prefix_req.prefix, str) and start_prefix_req.prefix.endswith(
            const.GS_PREFIX_DELIM
        )


def test__process_start_nok_sampling_lock_exists(monkeypatch):
    # Given
    cmd = command_test_data.TEST_COMMAND_START
    config = _StubGeneralConfig()
    config.location = 'TEST_LOCATION'
    config.request_bucket = 'REQUEST_BUCKET'
    config.sampling_lock_path = _SAMPLING_LOCK_PATH
    called = False

    # pylint: disable=unused-argument
    def mocked_read_object(bucket_name: str, path: str, warn_read_failure: Optional[bool]) -> bytes:
        nonlocal called
        assert bucket_name == config.request_bucket
        assert path == config.sampling_lock_path
        called = True
        return bytes(''.encode('utf-8'))

    # pylint: enable=unused-argument

    _mock_general_config(monkeypatch, config)
    monkeypatch.setattr(process_request.sampler_bucket.gcs, 'read_object', mocked_read_object)
    # When/Then
    with pytest.raises(InterruptedError):
        process_request._process_start(cmd)
    assert called


def test__create_sample_done_request_ok():
    # Given
    cmd = command_test_data.TEST_COMMAND_SAMPLE_START_RANDOM
    start_timestamp = 19
    end_timestamp = 37
    error_message = 'TEST_ERROR'
    # When
    result = process_request._create_sample_done_cmd(
        cmd, start_timestamp, end_timestamp, error_message
    )
    # Then
    assert isinstance(result, command.CommandSampleDone)
    assert result.type == command.CommandType.SAMPLE_DONE.value
    assert result.timestamp == cmd.timestamp
    assert result.sample_request == cmd.sample_request
    assert result.target_table == cmd.target_table
    assert result.start_timestamp == start_timestamp
    assert result.end_timestamp == end_timestamp
    assert result.error_message == error_message


def test__process_sample_policy_prefix_ok(monkeypatch):
    # Given
    cmd = command_test_data.TEST_COMMAND_SAMPLE_POLICY_PREFIX
    config = _StubGeneralConfig()
    config.pubsub_request = 'PUBSUB_REQUEST'
    config.target_project_id = 'TARGET_PROJECT_ID'
    config.policy_bucket = gcs_on_disk.POLICY_BUCKET
    called = {}
    _mock_general_config(monkeypatch, config)
    monkeypatch.setattr(process_request.sampler_bucket.gcs, 'read_object', gcs_on_disk.read_object)
    monkeypatch.setattr(
        process_request.sampler_bucket.gcs,
        '_get_gcs_prefixes_http_iterator',
        gcs_on_disk.get_gcs_prefixes_http_iterator,
    )
    monkeypatch.setattr(
        process_request.sampler_bucket.gcs, '_list_blob_names', gcs_on_disk.list_blob_names
    )
    monkeypatch.setattr(process_request.sampler_query, 'row_count', lambda _: 100)
    _mock_bq_base_dataset(monkeypatch)
    _mock_publish_sample_start(monkeypatch, called, 'called_publish', config.pubsub_request)
    # When
    process_request._process_sample_policy_prefix(cmd)
    # Then
    assert called.get('called_publish')


def _mock_bq_base_dataset(monkeypatch) -> None:

    dataset = types.SimpleNamespace()
    setattr(dataset, 'location', sample_policy_data.TEST_SRC_LOCATION)

    # pylint: disable=unused-argument
    def mocked_dataset(*args, **kwargs) -> object:
        return dataset

    # pylint: enable=unused-argument

    monkeypatch.setattr(process_request.sampler_bucket.bq, 'get_dataset', mocked_dataset)


def _mock_publish_sample_start(
    monkeypatch, called: Dict[str, bool], called_key: str, topic: str
) -> None:
    def mocked_publish(value: Dict[str, Any], topic_path: str) -> str:
        nonlocal called
        assert topic_path == topic
        value_event = command.CommandSampleStart.from_dict(value)
        assert value_event.type == command.CommandType.SAMPLE_START.value
        called[called_key] = True

    monkeypatch.setattr(process_request.pubsub, 'publish', mocked_publish)


def test__process_sample_start_ok_random(monkeypatch):
    # Given
    cmd = command_test_data.TEST_COMMAND_SAMPLE_START_RANDOM
    config = _StubGeneralConfig()
    config.pubsub_request = 'PUBSUB_REQUEST'
    called = {}
    _mock_general_config(monkeypatch, config)
    _mock_create_table_with_sample(
        monkeypatch, 'create_table_with_random_sample', called, 'called_create', cmd
    )
    _mock_publish_done(monkeypatch, called, 'called_publish', config.pubsub_request)
    # When
    process_request._process_sample_start(cmd)
    # Then
    assert called.get('called_create')
    assert called.get('called_publish')


def _mock_create_table_with_sample(  # pylint: disable=too-many-arguments
    monkeypatch,
    to_mock_function_name: str,
    called: Dict[str, bool],
    called_key: str,
    cmd: command.CommandSampleStart,
    kwargs_check: Optional[Dict[str, Any]] = None,
) -> None:
    def mocked_create_table_with_sample(
        *,
        source_table_ref: table.TableReference,
        target_table_ref: table.TableReference,
        amount: int,
        recreate_table: bool,
        **kwargs: Dict[str, Any],
    ) -> None:
        nonlocal called
        assert source_table_ref == cmd.sample_request.table_reference
        assert target_table_ref == cmd.target_table
        assert amount == cmd.sample_request.sample.size.count
        assert recreate_table
        if kwargs_check:
            for key, val in kwargs_check.items():
                assert kwargs.get(key) == val
        called[called_key] = True

    monkeypatch.setattr(
        process_request.sampler_query,
        to_mock_function_name,
        mocked_create_table_with_sample,
    )


def _mock_publish_done(monkeypatch, called: Dict[str, bool], called_key: str, topic: str) -> None:
    def mocked_publish(value: Dict[str, Any], topic_path: str) -> str:
        nonlocal called
        assert topic_path == topic
        value_event = command.CommandSampleDone.from_dict(value)
        assert value_event.type == command.CommandType.SAMPLE_DONE.value
        called[called_key] = True

    monkeypatch.setattr(process_request.pubsub, 'publish', mocked_publish)


def test__process_sample_start_ok_sorted(monkeypatch):
    # Given
    cmd = command_test_data.TEST_COMMAND_SAMPLE_START_SORTED
    config = _StubGeneralConfig()
    config.pubsub_request = 'PUBSUB_REQUEST'
    create_kwargs_check = {
        'column': cmd.sample_request.sample.spec.properties.by,
        'order': cmd.sample_request.sample.spec.properties.direction,
    }
    called = {}
    _mock_general_config(monkeypatch, config)
    _mock_create_table_with_sample(
        monkeypatch,
        'create_table_with_sorted_sample',
        called,
        'called_create',
        cmd,
        create_kwargs_check,
    )
    _mock_publish_done(monkeypatch, called, 'called_publish', config.pubsub_request)
    # When
    process_request._process_sample_start(cmd)
    # Then
    assert called.get('called_create')
    assert called.get('called_publish')
