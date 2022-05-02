# vim: ai:sw=4:ts=4:sta:et:fo=croql
# pylint: disable=missing-module-docstring,missing-class-docstring,missing-function-docstring
# pylint: disable=missing-function-docstring,assignment-from-no-return,c-extension-no-member
# pylint: disable=protected-access,redefined-outer-name,no-self-use,using-constant-test
# pylint: disable=invalid-name,attribute-defined-outside-init,too-few-public-methods, redefined-builtin
# type: ignore
from typing import Any, Dict, List, Optional

import pytest

from bq_sampler import process_request
from bq_sampler.entity import request, table

from tests.entity import sample_policy_data
from tests import gcs_on_disk
from tests import request_test_data

_GENERAL_POLICY_PATH: str = 'default_policy.json'


class _StubGeneralConfig:
    def __init__(self):
        self.target_project_id = None
        self.policy_bucket = None
        self.default_policy_path = None
        self.request_bucket = None
        self.pubsub_request = None
        self.pubsub_error = None


@pytest.mark.parametrize(
    'event_request,process_fn',
    [
        (request_test_data.TEST_EVENT_REQUEST_START, '_process_start'),
        (request_test_data.TEST_EVENT_REQUEST_SAMPLE_START, '_process_sample_start'),
        (request_test_data.TEST_EVENT_REQUEST_SAMPLE_DONE, '_process_sample_done'),
    ],
)
def test_process_ok(monkeypatch, event_request: request.EventRequest, process_fn: str):
    # Given
    called = False

    def mocked_process(value: request.EventRequest) -> None:
        nonlocal called
        called = True
        assert value == event_request

    monkeypatch.setattr(process_request, process_fn, mocked_process)
    # When
    process_request.process(event_request)
    # Then
    assert called


def test_process_nok(monkeypatch):
    # Given
    event_request = request_test_data.TEST_EVENT_REQUEST_START
    error = TypeError('TEST_MESSAGE')
    called = False
    config = _StubGeneralConfig()
    config.pubsub_error = 'PUBSUB_ERROR'

    def mocked_process(_) -> None:
        raise error

    def mocked_publish(value: Dict[str, Any], topic_path: str) -> str:
        nonlocal called
        called = True
        value_event = event_request.__class__.from_dict(value.get('event'))
        assert value_event == event_request
        assert value.get('error') == str(error)
        assert topic_path == config.pubsub_error

    _mock_general_config(monkeypatch, config)
    monkeypatch.setattr(process_request, '_process_start', mocked_process)
    monkeypatch.setattr(process_request.pubsub, 'publish', mocked_publish)
    # When
    with pytest.raises(RuntimeError):
        process_request.process(event_request)
        # Then
        assert called


def _mock_general_config(monkeypatch, config: _StubGeneralConfig) -> None:
    def mocked_config() -> Any:
        return config

    monkeypatch.setattr(process_request, '_general_config', mocked_config)


def test__create_sample_start_request_ok(monkeypatch):
    # Given
    event_request = request_test_data.TEST_EVENT_REQUEST_START
    table_policy = sample_policy_data.TEST_TABLE_POLICY
    table_sample = sample_policy_data.TEST_TABLE_SAMPLE
    config = _StubGeneralConfig()
    config.target_project_id = 'TARGET_PROJECT_ID'
    _mock_general_config(monkeypatch, config)
    # When
    result = process_request._create_sample_start_request(event_request, table_policy, table_sample)
    # Then
    assert isinstance(result, request.EventRequestSampleStart)
    assert result.request_timestamp == event_request.request_timestamp
    assert result.type == request.RequestType.SAMPLE_START.value
    assert result.sample_request == table_sample
    res_tgt_table = result.target_table
    assert res_tgt_table.project_id != table_sample.table_reference.project_id
    assert res_tgt_table.project_id == config.target_project_id
    assert res_tgt_table.dataset_id == table_sample.table_reference.dataset_id
    assert res_tgt_table.table_id == table_sample.table_reference.table_id
    assert res_tgt_table.location == table_sample.table_reference.location


def test__process_start_ok(monkeypatch):
    # Given
    event_request = request_test_data.TEST_EVENT_REQUEST_START
    config = _StubGeneralConfig()
    config.default_policy_path = 'DEFAULT_POLICY_PATH'
    config.policy_bucket = 'POLICY_BUCKET'
    config.request_bucket = 'REQUEST_BUCKET'
    config.target_project_id = 'TARGET_PROJECT_ID'
    config.pubsub_request = 'PUBSUB_REQUEST'
    sample_start_req_lst: List[request.EventRequestSampleStart] = []

    def mocked_publish(value: Dict[str, Any], topic_path: str) -> str:
        assert topic_path == config.pubsub_request
        nonlocal sample_start_req_lst
        sample_start_req_lst.append(request.EventRequestSampleStart.from_dict(value))

    _mock_general_config(monkeypatch, config)
    monkeypatch.setattr(process_request.sampler_bucket.gcs, 'read_object', gcs_on_disk.read_object)
    monkeypatch.setattr(
        process_request.sampler_bucket.gcs, '_list_blob_names', gcs_on_disk.list_blob_names
    )
    monkeypatch.setattr(process_request.sampler_query, 'row_count', lambda _: 100)
    monkeypatch.setattr(process_request.pubsub, 'publish', mocked_publish)
    # When
    process_request._process_start(event_request)
    # Then
    assert len(sample_start_req_lst) == 6
    for start_req in sample_start_req_lst:
        assert start_req.target_table.project_id == config.target_project_id


def test__create_sample_done_request_ok():
    # Given
    event_request = request_test_data.TEST_EVENT_REQUEST_SAMPLE_START_RANDOM
    start_timestamp = 19
    end_timestamp = 37
    error_message = 'TEST_ERROR'
    # When
    result = process_request._create_sample_done_request(
        event_request, start_timestamp, end_timestamp, error_message
    )
    # Then
    assert isinstance(result, request.EventRequestSampleDone)
    assert result.type == request.RequestType.SAMPLE_DONE.value
    assert result.request_timestamp == event_request.request_timestamp
    assert result.sample_request == event_request.sample_request
    assert result.target_table == event_request.target_table
    assert result.start_timestamp == start_timestamp
    assert result.end_timestamp == end_timestamp
    assert result.error_message == error_message


def test__process_sample_start_ok_random(monkeypatch):
    # Given
    event_request = request_test_data.TEST_EVENT_REQUEST_SAMPLE_START_RANDOM
    config = _StubGeneralConfig()
    config.pubsub_request = 'PUBSUB_REQUEST'
    called = {}
    _mock_general_config(monkeypatch, config)
    _mock_create_table_with_sample(
        monkeypatch, 'create_table_with_random_sample', called, 'called_create', event_request
    )
    _mock_publish_done(monkeypatch, called, 'called_publish', config.pubsub_request)
    # When
    process_request._process_sample_start(event_request)
    # Then
    assert called.get('called_create')
    assert called.get('called_publish')


def _mock_create_table_with_sample(  # pylint: disable=too-many-arguments
    monkeypatch,
    to_mock_function_name: str,
    called: Dict[str, bool],
    called_key: str,
    event_request: request.EventRequestSampleStart,
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
        assert source_table_ref == event_request.sample_request.table_reference
        assert target_table_ref == event_request.target_table
        assert amount == event_request.sample_request.sample.size.count
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
        value_event = request.EventRequestSampleDone.from_dict(value)
        assert value_event.type == request.RequestType.SAMPLE_DONE.value
        called[called_key] = True

    monkeypatch.setattr(process_request.pubsub, 'publish', mocked_publish)


def test__process_sample_start_ok_sorted(monkeypatch):
    # Given
    event_request = request_test_data.TEST_EVENT_REQUEST_SAMPLE_START_SORTED
    config = _StubGeneralConfig()
    config.pubsub_request = 'PUBSUB_REQUEST'
    create_kwargs_check = {
        'column': event_request.sample_request.sample.spec.properties.by,
        'order': event_request.sample_request.sample.spec.properties.direction,
    }
    called = {}
    _mock_general_config(monkeypatch, config)
    _mock_create_table_with_sample(
        monkeypatch,
        'create_table_with_sorted_sample',
        called,
        'called_create',
        event_request,
        create_kwargs_check,
    )
    _mock_publish_done(monkeypatch, called, 'called_publish', config.pubsub_request)
    # When
    process_request._process_sample_start(event_request)
    # Then
    assert called.get('called_create')
    assert called.get('called_publish')
