# vim: ai:sw=4:ts=4:sta:et:fo=croql
# pylint: disable=missing-module-docstring,missing-class-docstring,missing-function-docstring
# pylint: disable=missing-function-docstring,assignment-from-no-return,c-extension-no-member
# pylint: disable=protected-access,redefined-outer-name,no-self-use,using-constant-test
# pylint: disable=invalid-name,attribute-defined-outside-init,too-few-public-methods, redefined-builtin
# type: ignore
import types
from typing import Set

from bq_sampler import const, sampler_bucket
from bq_sampler.entity import policy

from tests.gcp import gcs_on_disk

_GENERAL_POLICY_PATH: str = 'default_policy.json'


def test__fetch_gcs_object_as_json_string_ok(monkeypatch):
    # Given
    _patch_gcs_storage(monkeypatch)
    _mock_bq_base_dataset(monkeypatch)
    # When
    result = sampler_bucket._fetch_gcs_object_as_string(
        gcs_on_disk.POLICY_BUCKET, _GENERAL_POLICY_PATH
    )
    # Then
    assert isinstance(result, str)


def _patch_gcs_storage(monkeypatch):
    monkeypatch.setattr(sampler_bucket.gcs, 'read_object', gcs_on_disk.read_object)
    monkeypatch.setattr(
        sampler_bucket.gcs,
        '_get_gcs_prefixes_http_iterator',
        gcs_on_disk.get_gcs_prefixes_http_iterator,
    )
    monkeypatch.setattr(sampler_bucket.gcs, '_list_blob_names', gcs_on_disk.list_blob_names)


def _mock_bq_base_dataset(monkeypatch) -> None:

    dataset = types.SimpleNamespace()
    setattr(dataset, 'location', 'test_location')

    # pylint: disable=unused-argument
    def mocked_dataset(*args, **kwargs) -> object:
        return dataset

    # pylint: enable=unused-argument

    monkeypatch.setattr(sampler_bucket.bq, 'get_dataset', mocked_dataset)


def test__default_policy_ok(monkeypatch):
    # Given
    _patch_gcs_storage(monkeypatch)
    # When
    result = sampler_bucket._default_policy(gcs_on_disk.POLICY_BUCKET, _GENERAL_POLICY_PATH)
    # Then
    assert isinstance(result, policy.Policy)
    assert result is not policy.FALLBACK_GENERIC_POLICY


def test__list_all_table_references_obj_path_ok(monkeypatch):
    # Given
    _patch_gcs_storage(monkeypatch)
    _mock_bq_base_dataset(monkeypatch)
    # When
    result = list(sampler_bucket._list_all_table_references_obj_path(gcs_on_disk.POLICY_BUCKET))
    # Then
    assert len(result) > 1
    for table_ref, obj_path in result:
        assert (
            obj_path
            == '/'.join([table_ref.project_id, table_ref.dataset_id, table_ref.table_id])
            + const.JSON_EXT
        )


_MANDATORY_PRESENT_POLICIES: Set[str] = set(
    [
        'empty_policy',
        'non_json',
        'non_json_b',
        'policy_full',
        'policy_full_again',
        'policy_full_simple_default_sample',
        'policy_only_limit',
    ]
)


def test_all_policies_ok(monkeypatch):
    # Given
    _patch_gcs_storage(monkeypatch)
    _mock_bq_base_dataset(monkeypatch)
    default_policy = sampler_bucket._default_policy(gcs_on_disk.POLICY_BUCKET, _GENERAL_POLICY_PATH)
    # When
    tables = set()
    for t_pol in sampler_bucket.all_policies(gcs_on_disk.POLICY_BUCKET, _GENERAL_POLICY_PATH):
        table_id = t_pol.table_reference.table_id
        tables.add(table_id)
        # Then
        if table_id in ('empty_policy', 'non_json', 'non_json_b'):
            _is_same_as_default(table_id, t_pol.policy, default_policy, True, True)
        elif table_id in ('policy_full', 'policy_full_again', 'policy_full_simple_default_sample'):
            _is_same_as_default(table_id, t_pol.policy, default_policy, False, False)
        elif table_id == 'policy_only_limit':
            _is_same_as_default(table_id, t_pol.policy, default_policy, False, True)
        else:
            assert False, f'Table id <{table_id}> has not assert specified'
    # Then: tests all mandatory
    assert _MANDATORY_PRESENT_POLICIES.issubset(tables)


def _is_same_as_default(
    table_id: str,
    table_policy: policy.Policy,
    default_policy: policy.Policy,
    limit: bool = True,
    default_sample: bool = True,
):
    is_limit = table_policy.limit == default_policy.limit
    is_default = table_policy.default_sample == default_policy.default_sample
    assert is_limit == limit, (
        f'Table <{table_id}>: Sample size should be equal = {limit}.'
        f'\nPolicy:{table_policy.limit}'
        f'\nDefault:{default_policy.limit}'
    )
    assert is_default == default_sample, (
        f'Table <{table_id}>: Default Sample = {default_sample}.'
        f'\nPolicy:{table_policy.default_sample}'
        f'\nDefault:{default_policy.default_sample}'
    )


_MANDATORY_PRESENT_REQUESTS: Set[str] = _MANDATORY_PRESENT_POLICIES.union(
    set(
        [
            'invalid_table_id',
        ]
    )
)
_REQUEST_HAS_SAMPLE_SIZE: Set[str] = set(
    [
        'invalid_table_id',
        'empty_policy',
        'non_json',
    ]
)
_REQUEST_HAS_SORT_ALGORITHM: Set[str] = set(
    [
        'invalid_table_id',
        'empty_policy',
        'invalid_table_id',
        'policy_full',
        'policy_full_simple_default_sample',
        'policy_only_limit',
    ]
)


def test_all_sample_requests_ok(monkeypatch):
    # Given
    _patch_gcs_storage(monkeypatch)
    _mock_bq_base_dataset(monkeypatch)
    # When
    tables = set()
    for t_req in sampler_bucket.all_sample_requests(gcs_on_disk.REQUEST_BUCKET):
        table_id = t_req.table_reference.table_id
        tables.add(table_id)
        # Then
        assert t_req.sample is not None
        if table_id in _REQUEST_HAS_SAMPLE_SIZE:
            assert (
                t_req.sample.size is not None
            ), f'Table id <{table_id}> expected to have sample size, but does not.'
        if table_id in _REQUEST_HAS_SORT_ALGORITHM:
            assert (
                t_req.sample.spec is not None
            ), f'Table id <{table_id}> expected to have sort algorithm, but does not.'
    # Then
    assert _MANDATORY_PRESENT_REQUESTS.issubset(tables)
