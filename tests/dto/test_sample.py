# vim: ai:sw=4:ts=4:sta:et:fo=croql
# pylint: disable=missing-module-docstring,missing-class-docstring,missing-function-docstring
# pylint: disable=missing-function-docstring,assignment-from-no-return,c-extension-no-member
# pylint: disable=protected-access,redefined-outer-name,no-self-use,using-constant-test
# pylint: disable=invalid-name,attribute-defined-outside-init,too-few-public-methods, redefined-builtin
# type: ignore

from typing import Any

import pytest

import attrs

from bq_sampler.dto import sample


@pytest.mark.incremental
class TestSampleSize:
    @pytest.mark.parametrize(
        'count,percentage',
        [
            (None, None),
            (1, None),
            (2000, None),
            (None, 0.001),
            (None, 23.57),
            (None, 100.0),
            (None, 29),  # percentage is int
            (17, 31.9),
        ],
    )
    def test_ctor_ok(self, count: int, percentage: float):
        obj = sample.SampleSize(count=count, percentage=percentage)
        assert obj.count == count
        assert obj.percentage == percentage
        obj_dict = attrs.asdict(obj)
        assert obj == sample.SampleSize.from_dict(obj_dict)

    @pytest.mark.parametrize(
        'count,percentage',
        [
            (0, None),
            (-0, None),
            (-1, None),  # count is negative
            (None, 0),
            (None, -0),
            (None, 0.0),
            (None, -0.0),
            (None, -0.0001),  # percentage is negative
            (None, 100.00001),  # percentage is above 100.0
        ],
    )
    def test_ctor_nok(self, count: int, percentage: float):
        with pytest.raises(ValueError):
            sample.SampleSize(count=count, percentage=percentage)


@pytest.mark.incremental
class TestSortDirection:
    def test_from_str_ok(self):
        for d in sample.SortDirection:
            assert d == sample.SortDirection.from_str(d.value.lower())

    def test_from_str_none(self):
        assert sample.SortDirection.from_str(None) is None

    def test_from_str_nok(self):
        for d in sample.SortDirection:
            assert sample.SortDirection.from_str(d.value.lower() + "_NOT") is None


@pytest.mark.incremental
class Test_SortProperties:
    @pytest.mark.parametrize(
        'sort_by,sort_direction',
        [
            ("column", sample.SortDirection.ASC.value),
            ("column", sample.SortDirection.DESC.value),
        ],
    )
    def test_ctor_ok(self, sort_by: str, sort_direction: str):
        obj = sample._SortProperties(sort_by=sort_by, sort_direction=sort_direction)
        assert obj.sort_by == sort_by
        assert obj.sort_direction == sort_direction
        obj_dict = attrs.asdict(obj)
        assert obj == sample._SortProperties.from_dict(obj_dict)

    @pytest.mark.parametrize(
        'sort_by,sort_direction',
        [
            (None, None),  # None is not str
            ("column", None),  # None is not str
            (None, sample.SortDirection.ASC.value),  # None is not str
            (123, sample.SortDirection.ASC.value),  # int is not str
            ("column", sample.SortDirection.ASC),  # enum is not str
        ],
    )
    def test_ctor_nok_type(self, sort_by: Any, sort_direction: Any):
        with pytest.raises(TypeError):
            sample._SortProperties(sort_by=sort_by, sort_direction=sort_direction)

    @pytest.mark.parametrize(
        'sort_by,sort_direction',
        [
            ("", sample.SortDirection.ASC.value),  # empty string as sort_by
            (" column", sample.SortDirection.ASC.value),  # leading space in sort_by
            ("column ", sample.SortDirection.ASC.value),  # trailing space in sort_by
            ("column", sample.SortDirection.ASC.value + "_NOT"),  # invalid sort_direction
        ],
    )
    def test_ctor_nok_value(self, sort_by: str, sort_direction: str):
        with pytest.raises(ValueError):
            sample._SortProperties(sort_by=sort_by, sort_direction=sort_direction)


@pytest.mark.incremental
class TestSortType:
    def test_from_str_ok(self):
        for d in sample.SortType:
            assert d == sample.SortType.from_str(d.value.lower())

    def test_from_str_none(self):
        assert sample.SortType.from_str(None) is None

    def test_from_str_nok(self):
        for d in sample.SortType:
            assert sample.SortType.from_str(d.value.lower() + "_NOT") is None


_TEST_COLUMN: str = "TEST_COLUMN"
_TEST_SORT_PROPERTIES: sample._SortProperties = sample._SortProperties(
    sort_by=_TEST_COLUMN, sort_direction=sample.SortDirection.ASC.value
)


@pytest.mark.incremental
class Test_SortAlgorithm:
    @pytest.mark.parametrize(
        'type,properties',
        [
            (None, None),
            (None, _TEST_SORT_PROPERTIES),
            (sample.SortType.RELATIONAL.value, None),
            (sample.SortType.RELATIONAL.value, _TEST_SORT_PROPERTIES),
            (sample.SortType.RANDOM.value, _TEST_SORT_PROPERTIES),
        ],
    )
    def test_ctor_ok(self, type: str, properties: sample._SortProperties):
        obj = sample._SortAlgorithm(type=type, properties=properties)
        assert obj.type == type
        assert obj.properties == properties
        obj_dict = attrs.asdict(obj)
        assert obj == sample._SortAlgorithm.from_dict(obj_dict)

    def test_ctor_nok(self):
        for t in sample.SortType:
            with pytest.raises(ValueError):
                sample._SortAlgorithm(type=t.value + "_NOT", properties=_TEST_SORT_PROPERTIES)


_TEST_SAMPLE_SIZE: sample.SampleSize = sample.SampleSize(count=17, percentage=11.1)
_TEST_SORT_ALGORITHM: sample._SortAlgorithm = sample._SortAlgorithm(
    type=sample.SortType.RELATIONAL.value, properties=_TEST_SORT_PROPERTIES
)


@pytest.mark.incremental
class TestSample:
    @pytest.mark.parametrize(
        'sample_size,sort_algorithm',
        [
            (None, None),
            (None, _TEST_SORT_ALGORITHM),
            (_TEST_SAMPLE_SIZE, None),
            (_TEST_SAMPLE_SIZE, _TEST_SORT_ALGORITHM),
        ],
    )
    def test_ctor_ok(self, sample_size: sample.SampleSize, sort_algorithm: sample._SortAlgorithm):
        obj = sample.Sample(sample_size=sample_size, sort_algorithm=sort_algorithm)
        assert obj.sample_size == sample_size
        assert obj.sort_algorithm == sort_algorithm
        obj_dict = attrs.asdict(obj)
        assert obj == sample.Sample.from_dict(obj_dict)

    @pytest.mark.parametrize(
        'sample_size,sort_algorithm',
        [
            (None, attrs.asdict(_TEST_SORT_ALGORITHM)),
            (attrs.asdict(_TEST_SAMPLE_SIZE), None),
            (attrs.asdict(_TEST_SAMPLE_SIZE), attrs.asdict(_TEST_SORT_ALGORITHM)),
        ],
    )
    def test_ctor_nok_type(self, sample_size: Any, sort_algorithm: Any):
        with pytest.raises(TypeError):
            sample.Sample(sample_size=sample_size, sort_algorithm=sort_algorithm)
