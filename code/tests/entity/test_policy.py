# vim: ai:sw=4:ts=4:sta:et:fo=croql
# pylint: disable=missing-module-docstring,missing-class-docstring,missing-function-docstring
# pylint: disable=missing-function-docstring,assignment-from-no-return,c-extension-no-member
# pylint: disable=protected-access,redefined-outer-name,no-self-use,using-constant-test
# pylint: disable=invalid-name,attribute-defined-outside-init,too-few-public-methods
# type: ignore
import math

from typing import Any

import pytest

import attrs

from bq_sampler.entity import policy
from bq_sampler.entity import table

from tests.entity import sample_policy_data


@pytest.mark.incremental
class TestPolicy:
    @pytest.mark.parametrize(
        'limit,default_sample',
        [
            (None, None),
            (sample_policy_data.TEST_SAMPLE_SIZE, None),
            (None, sample_policy_data.TEST_SAMPLE),
            (sample_policy_data.TEST_SAMPLE_SIZE, sample_policy_data.TEST_SAMPLE),
        ],
    )
    def test_ctor_ok(self, limit: table.SizeSpec, default_sample: table.Sample):
        obj = policy.Policy(limit=limit, default_sample=default_sample)
        assert obj.limit == limit
        assert obj.default_sample == default_sample
        obj_dict = attrs.asdict(obj)
        assert obj == policy.Policy.from_dict(obj_dict)

    @pytest.mark.parametrize(
        'limit,default_sample',
        [
            (attrs.asdict(sample_policy_data.TEST_SAMPLE_SIZE), None),
            (None, attrs.asdict(sample_policy_data.TEST_SAMPLE)),
            (
                attrs.asdict(sample_policy_data.TEST_SAMPLE_SIZE),
                attrs.asdict(sample_policy_data.TEST_SAMPLE),
            ),
        ],
    )
    def test_ctor_nok_type(self, limit: Any, default_sample: Any):
        with pytest.raises(TypeError):
            policy.Policy(limit=limit, default_sample=default_sample)

    def test__policy_count_limit_ok_count_wins(self):
        # Given
        obj = sample_policy_data.TEST_POLICY
        row_count = math.ceil(obj.limit.count * 100 / obj.limit.percentage) + 1
        # When
        result = obj._policy_count_limit(row_count)
        # Then
        assert result == obj.limit.count

    def test__policy_count_limit_ok_percentage_wins(self):
        # Given
        obj = sample_policy_data.TEST_POLICY
        row_count = math.floor(obj.limit.count * 100 / obj.limit.percentage) - 1
        # When
        result = obj._policy_count_limit(row_count)
        # Then
        assert result < obj.limit.count

    def test__sample_count_ok_count_wins(self):
        # Given
        obj = sample_policy_data.TEST_POLICY
        sample = sample_policy_data.TEST_SAMPLE
        row_count = 0
        # When
        result = obj._sample_count(sample, row_count)
        # Then
        assert result == sample.size.count

    def test__sample_count_ok_percentage_wins(self):
        # Given
        obj = sample_policy_data.TEST_POLICY
        sample = sample_policy_data.TEST_SAMPLE
        row_count = math.ceil(obj.limit.count * 100 / obj.limit.percentage) + 100
        # When
        result = obj._sample_count(sample, row_count)
        # Then
        assert result > sample.size.count

    def test__sample_copy_with_count_ok(self):
        # Given
        obj = sample_policy_data.TEST_POLICY
        sample = sample_policy_data.TEST_SAMPLE
        sample_size = 100
        # When
        result = obj._sample_copy_with_count(sample, sample_size)
        # Then
        assert result.spec == sample.spec
        assert result.size.percentage is None
        assert result.size.count == sample_size

    def test_compliant_sample_ok_happy_path(self):
        # Given
        obj = sample_policy_data.TEST_POLICY
        sample = sample_policy_data.TEST_SAMPLE
        row_count: int = math.ceil(100 * obj.limit.count / obj.limit.percentage) + 1
        # When
        result = obj.compliant_sample(sample, row_count)
        # Then
        assert result.size.percentage is None
        assert result.size.count == sample.size.count

    def test_compliant_sample_nok_empty_table(self):
        # Given
        obj = sample_policy_data.TEST_POLICY
        sample = sample_policy_data.TEST_SAMPLE
        # When
        result = obj.compliant_sample(sample, 0)
        # Then
        assert result.size.percentage is None
        assert result.size.count == 0


@pytest.mark.incremental
class TestTablePolicy:
    def test_ctor_ok(self):
        obj = sample_policy_data.TEST_TABLE_POLICY
        assert obj.table_reference == sample_policy_data.TEST_TABLE_REFERENCE
        assert obj.policy == sample_policy_data.TEST_POLICY
        obj_dict = attrs.asdict(obj)
        assert obj == policy.TablePolicy.from_dict(obj_dict)

    @pytest.mark.parametrize(
        'table_reference,policy_arg',
        [
            (None, None),
            (sample_policy_data.TEST_TABLE_REFERENCE, None),
            (None, sample_policy_data.TEST_POLICY),
        ],
    )
    def test_ctor_nok_type(self, table_reference: Any, policy_arg: Any):
        with pytest.raises(TypeError):
            policy.TablePolicy(table_reference=table_reference, policy=policy_arg)

    def test_compliant_sample_ok_happy_path(self):
        # Given
        obj = sample_policy_data.TEST_TABLE_POLICY
        table_sample = sample_policy_data.TEST_TABLE_SAMPLE
        row_count: int = math.ceil(100 * obj.policy.limit.count / obj.policy.limit.percentage) + 1
        # When
        result = obj.compliant_sample(table_sample, row_count)
        # Then
        assert result.table_reference == table_sample.table_reference
        assert result.sample.size.percentage is None
        assert result.sample.size.count == table_sample.sample.size.count
