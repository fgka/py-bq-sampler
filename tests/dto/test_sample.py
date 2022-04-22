# vim: ai:sw=4:ts=4:sta:et:fo=croql
# pylint: disable=missing-module-docstring,missing-class-docstring,missing-function-docstring
# pylint: disable=missing-function-docstring,assignment-from-no-return,c-extension-no-member
# pylint: disable=protected-access,redefined-outer-name,no-self-use,using-constant-test,duplicate-code
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
        # Given/When
        obj = sample.SampleSize(count=count, percentage=percentage)
        # Then
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
class Test_SortProperties:
    @pytest.mark.parametrize(
        'by,direction',
        [
            ("column", sample.SortDirection.ASC.value),
            ("column", sample.SortDirection.DESC.value),
        ],
    )
    def test_ctor_ok(self, by: str, direction: str):
        # Given/When
        obj = sample._SortProperties(by=by, direction=direction)
        # Then
        assert obj.by == by
        assert obj.direction == direction
        obj_dict = attrs.asdict(obj)
        assert obj == sample._SortProperties.from_dict(obj_dict)

    @pytest.mark.parametrize(
        'by,direction',
        [
            (None, None),  # None is not str
            ("column", None),  # None is not str
            (None, sample.SortDirection.ASC.value),  # None is not str
            (123, sample.SortDirection.ASC.value),  # int is not str
            ("column", sample.SortDirection.ASC),  # enum is not str
        ],
    )
    def test_ctor_nok_type(self, by: Any, direction: Any):
        with pytest.raises(TypeError):
            sample._SortProperties(by=by, direction=direction)

    @pytest.mark.parametrize(
        'by,direction',
        [
            ("", sample.SortDirection.ASC.value),  # empty string as by
            (" column", sample.SortDirection.ASC.value),  # leading space in by
            ("column ", sample.SortDirection.ASC.value),  # trailing space in by
            ("column", sample.SortDirection.ASC.value + "_NOT"),  # invalid direction
        ],
    )
    def test_ctor_nok_value(self, by: str, direction: str):
        with pytest.raises(ValueError):
            sample._SortProperties(by=by, direction=direction)


@pytest.mark.incremental
class TestSortType:
    def test_default_ok(self):
        # Given/When
        result = sample.SortType.default()
        # Then
        assert result in sample.SortType


_TEST_size: sample.SampleSize = sample.SampleSize(count=31, percentage=57.986)
_TEST_COLUMN: str = "TEST_COLUMN"
_TEST_SORT_PROPERTIES: sample._SortProperties = sample._SortProperties(
    by=_TEST_COLUMN, direction=sample.SortDirection.ASC.value
)


@pytest.mark.incremental
class TestSortAlgorithm:
    @pytest.mark.parametrize(
        'type,properties',
        [
            (None, None),
            (None, _TEST_SORT_PROPERTIES),
            (sample.SortType.SORTED.value, None),
            (sample.SortType.SORTED.value, _TEST_SORT_PROPERTIES),
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


_TEST_spec: sample.SortAlgorithm = sample.SortAlgorithm(
    type=sample.SortType.SORTED.value, properties=_TEST_SORT_PROPERTIES
)
_TEST_SAMPLE: sample.Sample = sample.Sample(size=_TEST_size, spec=_TEST_spec)


@pytest.mark.incremental
class TestSample:
    @pytest.mark.parametrize(
        'size,spec',
        [
            (None, None),
            (None, _TEST_spec),
            (_TEST_size, None),
            (_TEST_size, _TEST_spec),
        ],
    )
    def test_ctor_ok(self, size: sample.SampleSize, spec: sample.SortAlgorithm):
        obj = sample.Sample(size=size, spec=spec)
        assert obj.size == size
        assert obj.spec == spec
        obj_dict = attrs.asdict(obj)
        assert obj == sample.Sample.from_dict(obj_dict)

    @pytest.mark.parametrize(
        'size,spec',
        [
            (None, attrs.asdict(_TEST_spec)),
            (attrs.asdict(_TEST_size), None),
            (attrs.asdict(_TEST_size), attrs.asdict(_TEST_spec)),
        ],
    )
    def test_ctor_nok_type(self, size: Any, spec: Any):
        with pytest.raises(TypeError):
            sample.Sample(size=size, spec=spec)


_TEST_PROJECT_ID: str = 'TEST_PROJECT_ID'
_TEST_DATASET_ID: str = 'TEST_DATASET_ID'
_TEST_TABLE_ID: str = 'TEST_TABLE_ID'
_TEST_LOCATION: str = 'TEST_REGION'


@pytest.mark.incremental
class TestTableReference:
    @pytest.mark.parametrize(
        'project_id,dataset_id,table_id,location',
        [
            (_TEST_PROJECT_ID, _TEST_DATASET_ID, _TEST_TABLE_ID, None),
            (_TEST_PROJECT_ID, _TEST_DATASET_ID, _TEST_TABLE_ID, _TEST_LOCATION),
        ],
    )
    def test_ctor_ok(self, project_id: str, dataset_id: str, table_id: str, location: str):
        obj = sample.TableReference(
            project_id=project_id, dataset_id=dataset_id, table_id=table_id, location=location
        )
        assert obj.project_id == project_id
        assert obj.dataset_id == dataset_id
        assert obj.table_id == table_id
        assert obj.location == location
        obj_dict = attrs.asdict(obj)
        assert obj == sample.TableReference.from_dict(obj_dict)

    @pytest.mark.parametrize(
        'project_id,dataset_id,table_id,location',
        [
            (None, _TEST_DATASET_ID, _TEST_TABLE_ID, None),
            (_TEST_PROJECT_ID, None, _TEST_TABLE_ID, None),
            (_TEST_PROJECT_ID, _TEST_DATASET_ID, None, None),
        ],
    )
    def test_ctor_nok_type(self, project_id: Any, dataset_id: Any, table_id: Any, location: Any):
        with pytest.raises(TypeError):
            sample.TableReference(
                project_id=project_id, dataset_id=dataset_id, table_id=table_id, location=location
            )


_TEST_TABLE_REFERENCE: sample.TableReference = sample.TableReference(
    project_id=_TEST_PROJECT_ID,
    dataset_id=_TEST_DATASET_ID,
    table_id=_TEST_TABLE_ID,
    location=_TEST_LOCATION,
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
