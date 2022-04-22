# vim: ai:sw=4:ts=4:sta:et:fo=croql
# pylint: disable=missing-module-docstring,missing-class-docstring,missing-function-docstring
# pylint: disable=missing-function-docstring,assignment-from-no-return,c-extension-no-member
# pylint: disable=protected-access,redefined-outer-name,no-self-use,using-constant-test
# pylint: disable=invalid-name,attribute-defined-outside-init,too-few-public-methods
# type: ignore

from typing import Any

import pytest

import attrs

from bq_sampler.dto import policy
from bq_sampler.dto import sample


_TEST_SAMPLE_SIZE: sample.SampleSize = sample.SampleSize(count=17, percentage=11.1)
_TEST_COLUMN: str = "TEST_COLUMN"
_TEST_SORT_PROPERTIES: sample._SortProperties = sample._SortProperties(
    by=_TEST_COLUMN, direction=sample.SortDirection.ASC.value
)
_TEST_SORT_ALGORITHM: sample.SortAlgorithm = sample.SortAlgorithm(
    type=sample.SortType.SORTED.value, properties=_TEST_SORT_PROPERTIES
)
_TEST_SAMPLE: sample.Sample = sample.Sample(size=_TEST_SAMPLE_SIZE, spec=_TEST_SORT_ALGORITHM)


@pytest.mark.incremental
class TestPolicy:
    @pytest.mark.parametrize(
        'limit,default_sample',
        [
            (None, None),
            (_TEST_SAMPLE_SIZE, None),
            (None, _TEST_SAMPLE),
            (_TEST_SAMPLE_SIZE, _TEST_SAMPLE),
        ],
    )
    def test_ctor_ok(self, limit: sample.SampleSize, default_sample: sample.Sample):
        obj = policy.Policy(limit=limit, default_sample=default_sample)
        assert obj.limit == limit
        assert obj.default_sample == default_sample
        obj_dict = attrs.asdict(obj)
        assert obj == policy.Policy.from_dict(obj_dict)

    @pytest.mark.parametrize(
        'limit,default_sample',
        [
            (attrs.asdict(_TEST_SAMPLE_SIZE), None),
            (None, attrs.asdict(_TEST_SAMPLE)),
            (attrs.asdict(_TEST_SAMPLE_SIZE), attrs.asdict(_TEST_SAMPLE)),
        ],
    )
    def test_ctor_nok_type(self, limit: Any, default_sample: Any):
        with pytest.raises(TypeError):
            policy.Policy(limit=limit, default_sample=default_sample)


_TEST_PROJECT_ID: str = 'TEST_PROJECT_ID'
_TEST_DATASET_ID: str = 'TEST_DATASET_ID'
_TEST_TABLE_ID: str = 'TEST_TABLE_ID'
_TEST_LOCATION: str = 'TEST_REGION'
_TEST_TABLE_REFERENCE: sample.TableReference = sample.TableReference(
    project_id=_TEST_PROJECT_ID,
    dataset_id=_TEST_DATASET_ID,
    table_id=_TEST_TABLE_ID,
    location=_TEST_LOCATION,
)
_TEST_POLICY: policy.Policy = policy.Policy(limit=_TEST_SAMPLE_SIZE, default_sample=_TEST_SAMPLE)


@pytest.mark.incremental
class TestTablePolicy:
    def test_ctor_ok(self):
        obj = policy.TablePolicy(table_reference=_TEST_TABLE_REFERENCE, policy=_TEST_POLICY)
        assert obj.table_reference == _TEST_TABLE_REFERENCE
        assert obj.policy == _TEST_POLICY
        obj_dict = attrs.asdict(obj)
        assert obj == policy.TablePolicy.from_dict(obj_dict)

    @pytest.mark.parametrize(
        'table_reference,policy_arg',
        [
            (None, None),
            (_TEST_TABLE_REFERENCE, None),
            (None, _TEST_POLICY),
        ],
    )
    def test_ctor_nok_type(self, table_reference: Any, policy_arg: Any):
        with pytest.raises(TypeError):
            policy.TablePolicy(table_reference=table_reference, policy=policy_arg)
