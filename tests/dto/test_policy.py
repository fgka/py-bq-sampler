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
    sort_by=_TEST_COLUMN, sort_direction=sample.SortDirection.ASC.value
)
_TEST_SORT_ALGORITHM: sample._SortAlgorithm = sample._SortAlgorithm(
    type=sample.SortType.RELATIONAL.value, properties=_TEST_SORT_PROPERTIES
)
_TEST_SAMPLE: sample.Sample = sample.Sample(
    sample_size=_TEST_SAMPLE_SIZE, sort_algorithm=_TEST_SORT_ALGORITHM
)


@pytest.mark.incremental
class TestPolicy:
    @pytest.mark.parametrize(
        'sample_size_limit,sample_size_default',
        [
            (None, None),
            (_TEST_SAMPLE_SIZE, None),
            (None, _TEST_SAMPLE),
            (_TEST_SAMPLE_SIZE, _TEST_SAMPLE),
        ],
    )
    def test_ctor_ok(
        self, sample_size_limit: sample.SampleSize, sample_size_default: sample.Sample
    ):
        obj = policy.Policy(
            sample_size_limit=sample_size_limit, sample_size_default=sample_size_default
        )
        assert obj.sample_size_limit == sample_size_limit
        assert obj.sample_size_default == sample_size_default
        assert obj == policy.Policy.from_dict(attrs.asdict(obj))

    @pytest.mark.parametrize(
        'sample_size_limit,sample_size_default',
        [
            (attrs.asdict(_TEST_SAMPLE_SIZE), None),
            (None, attrs.asdict(_TEST_SAMPLE)),
            (attrs.asdict(_TEST_SAMPLE_SIZE), attrs.asdict(_TEST_SAMPLE)),
        ],
    )
    def test_ctor_nok_type(self, sample_size_limit: Any, sample_size_default: Any):
        with pytest.raises(TypeError):
            policy.Policy(
                sample_size_limit=sample_size_limit, sample_size_default=sample_size_default
            )
