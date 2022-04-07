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

_TEST_SAMPLE_SIZE: sample.SampleSize = sample.SampleSize(count=31, percentage=57.986)


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

    @pytest.mark.parametrize(
        'count,percentage',
        [
            (2000, None),
            (None, 23.57),
            (17, 31.9),
        ],
    )
    def test_overwrite_with_ok_return_self(self, count: int, percentage: float):
        obj = sample.SampleSize(count=count, percentage=percentage)
        result = obj.overwrite_with(_TEST_SAMPLE_SIZE)
        assert result == obj

    def test_overwrite_with_ok_return_self_value_is_none(self):
        obj = sample.SampleSize(count=None, percentage=None)
        result = obj.overwrite_with(None)
        assert result == obj

    def test_overwrite_with_ok_return_value(self):
        obj = sample.SampleSize(count=None, percentage=None)
        result = obj.overwrite_with(_TEST_SAMPLE_SIZE)
        assert result == _TEST_SAMPLE_SIZE


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


_TEST_COLUMN: str = "TEST_COLUMN"
_TEST_SORT_PROPERTIES: sample._SortProperties = sample._SortProperties(
    sort_by=_TEST_COLUMN, sort_direction=sample.SortDirection.ASC.value
)


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

    def test_overwrite_with_ok_return_self(self):
        obj = sample._SortProperties(
            sort_by="my_column", sort_direction=sample.SortDirection.ASC.value
        )
        result = obj.overwrite_with(None)
        assert result == obj

    def test_overwrite_with_ok_return_value(self):
        obj = sample._SortProperties(
            sort_by="my_column", sort_direction=sample.SortDirection.ASC.value
        )
        result = obj.overwrite_with(_TEST_SORT_PROPERTIES)
        assert result == _TEST_SORT_PROPERTIES


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


_TEST_SORT_ALGORITHM: sample.SortAlgorithm = sample.SortAlgorithm(
    type=sample.SortType.RELATIONAL.value, properties=_TEST_SORT_PROPERTIES
)


@pytest.mark.incremental
class TestSortAlgorithm:
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
        obj = sample.SortAlgorithm(type=type, properties=properties)
        assert obj.type == type
        assert obj.properties == properties
        obj_dict = attrs.asdict(obj)
        assert obj == sample.SortAlgorithm.from_dict(obj_dict)

    def test_ctor_nok(self):
        for t in sample.SortType:
            with pytest.raises(ValueError):
                sample.SortAlgorithm(type=t.value + "_NOT", properties=_TEST_SORT_PROPERTIES)

    @pytest.mark.parametrize(
        'type,properties',
        [
            (None, _TEST_SORT_PROPERTIES),
            (sample.SortType.RANDOM.value, None),
        ],
    )
    def test_overwrite_with_ok_return_self(self, type: str, properties: sample._SortProperties):
        obj = sample.SortAlgorithm(type=type, properties=properties)
        result = obj.overwrite_with(_TEST_SORT_ALGORITHM)
        assert result == obj

    def test_overwrite_with_ok_return_self_value_is_none(self):
        obj = sample.SortAlgorithm(type=None, properties=None)
        result = obj.overwrite_with(None)
        assert result == obj

    def test_overwrite_with_ok_return_value(self):
        obj = sample.SortAlgorithm(type=None, properties=None)
        result = obj.overwrite_with(_TEST_SORT_ALGORITHM)
        assert result == _TEST_SORT_ALGORITHM


_TEST_SAMPLE_SIZE: sample.SampleSize = sample.SampleSize(count=17, percentage=11.1)
_TEST_SAMPLE: sample.Sample = sample.Sample(
    sample_size=_TEST_SAMPLE_SIZE, sort_algorithm=_TEST_SORT_ALGORITHM
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
    def test_ctor_ok(self, sample_size: sample.SampleSize, sort_algorithm: sample.SortAlgorithm):
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

    def test_overwrite_with_ok_sample_size_changed(self):
        obj = sample.Sample(sample_size=None, sort_algorithm=_TEST_SORT_ALGORITHM)
        result = obj.overwrite_with(_TEST_SAMPLE)
        assert result.sample_size is not None

    def test_overwrite_with_ok_sort_algorithm_changed(self):
        obj = sample.Sample(sample_size=_TEST_SAMPLE_SIZE, sort_algorithm=None)
        result = obj.overwrite_with(_TEST_SAMPLE)
        assert result.sort_algorithm is not None

    def test_overwrite_with_ok_return_self_value_is_none(self):
        obj = sample.Sample(sample_size=None, sort_algorithm=None)
        result = obj.overwrite_with(None)
        assert result == obj


_TEST_PROJECT_ID: str = 'TEST_PROJECT_ID'
_TEST_DATASET_ID: str = 'TEST_DATASET_ID'
_TEST_TABLE_ID: str = 'TEST_TABLE_ID'
_TEST_REGION: str = 'TEST_REGION'


@pytest.mark.incremental
class TestTableReference:
    @pytest.mark.parametrize(
        'project_id,dataset_id,table_id,region',
        [
            (_TEST_PROJECT_ID, _TEST_DATASET_ID, _TEST_TABLE_ID, None),
            (_TEST_PROJECT_ID, _TEST_DATASET_ID, _TEST_TABLE_ID, _TEST_REGION),
        ],
    )
    def test_ctor_ok(self, project_id: str, dataset_id: str, table_id: str, region: str):
        obj = sample.TableReference(
            project_id=project_id, dataset_id=dataset_id, table_id=table_id, region=region
        )
        assert obj.project_id == project_id
        assert obj.dataset_id == dataset_id
        assert obj.table_id == table_id
        assert obj.region == region
        obj_dict = attrs.asdict(obj)
        assert obj == sample.TableReference.from_dict(obj_dict)

    @pytest.mark.parametrize(
        'project_id,dataset_id,table_id,region',
        [
            (None, _TEST_DATASET_ID, _TEST_TABLE_ID, None),
            (_TEST_PROJECT_ID, None, _TEST_TABLE_ID, None),
            (_TEST_PROJECT_ID, _TEST_DATASET_ID, None, None),
        ],
    )
    def test_ctor_nok_type(self, project_id: Any, dataset_id: Any, table_id: Any, region: Any):
        with pytest.raises(TypeError):
            sample.TableReference(
                project_id=project_id, dataset_id=dataset_id, table_id=table_id, region=region
            )


_TEST_TABLE_REFERENCE: sample.TableReference = sample.TableReference(
    project_id=_TEST_PROJECT_ID,
    dataset_id=_TEST_DATASET_ID,
    table_id=_TEST_TABLE_ID,
    region=_TEST_REGION,
)


@pytest.mark.incremental
class TestTableSample:
    def test_ctor_ok(self):
        obj = sample.TableSample(table_reference=_TEST_TABLE_REFERENCE, sample=_TEST_SAMPLE)
        assert obj.table_reference == _TEST_TABLE_REFERENCE
        assert obj.sample == _TEST_SAMPLE
        obj_dict = attrs.asdict(obj)
        assert obj == sample.TableSample.from_dict(obj_dict)

    @pytest.mark.parametrize(
        'table_reference,sample_arg',
        [
            (None, None),
            (_TEST_TABLE_REFERENCE, None),
            (None, _TEST_SAMPLE),
        ],
    )
    def test_ctor_nok_type(self, table_reference: Any, sample_arg: Any):
        with pytest.raises(TypeError):
            sample.TableSample(table_reference=table_reference, sample=sample_arg)
