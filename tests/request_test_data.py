# vim: ai:sw=4:ts=4:sta:et:fo=croql
# pylint: disable=missing-module-docstring,missing-class-docstring,missing-function-docstring
# pylint: disable=missing-function-docstring,assignment-from-no-return,c-extension-no-member
# pylint: disable=protected-access,redefined-outer-name,no-self-use,using-constant-test
# pylint: disable=invalid-name,attribute-defined-outside-init,too-few-public-methods, redefined-builtin
# type: ignore
from bq_sampler.entity import request
from bq_sampler.entity import table

_TEST_SOURCE_PROJECT_ID: str = 'TEST_SOURCE_PROJECT_ID'
_TEST_TARGET_PROJECT_ID: str = 'TEST_TARGET_PROJECT_ID'
_TEST_DATASET_ID: str = 'TEST_DATASET_ID'
_TEST_TABLE_ID: str = 'TEST_TABLE_ID'
_TEST_SOURCE_TABLE_REF: table.TableReference = table.TableReference(
    project_id=_TEST_SOURCE_PROJECT_ID, dataset_id=_TEST_DATASET_ID, table_id=_TEST_TABLE_ID
)
_TEST_TARGET_TABLE_REF: table.TableReference = _TEST_SOURCE_TABLE_REF.clone(
    project_id=_TEST_TARGET_PROJECT_ID
)
_TEST_SAMPLE_SIZE: table.SizeSpec = table.SizeSpec(count=11)
_TEST_SORT_ALGORITHM: table.SampleSpec = table.SampleSpec()
_TEST_SORT_ALGORITHM_RANDOM: table.SampleSpec = table.SampleSpec(
    type=table.SortType.RANDOM.value
)
_TEST_SORT_BY: str = 'TEST_COLUMN'
_TEST_SORT_DIR: str = table.SortDirection.DESC.value
_TEST_SORT_PROPERTIES: table._SortProperties = table._SortProperties(
    by=_TEST_SORT_BY, direction=_TEST_SORT_DIR
)
_TEST_SORT_ALGORITHM_SORTED: table.SampleSpec = table.SampleSpec(
    type=table.SortType.SORTED.value, properties=_TEST_SORT_PROPERTIES
)
_TEST_SAMPLE: table.Sample = table.Sample(size=_TEST_SAMPLE_SIZE, spec=_TEST_SORT_ALGORITHM)
_TEST_SAMPLE_RANDOM: table.Sample = table.Sample(
    size=_TEST_SAMPLE_SIZE, spec=_TEST_SORT_ALGORITHM_RANDOM
)
_TEST_SAMPLE_SORTED: table.Sample = table.Sample(
    size=_TEST_SAMPLE_SIZE, spec=_TEST_SORT_ALGORITHM_SORTED
)
_TEST_SAMPLE_REQUEST: table.TableSample = table.TableSample(
    table_reference=_TEST_SOURCE_TABLE_REF, sample=_TEST_SAMPLE
)
_TEST_SAMPLE_REQUEST_RANDOM: table.TableSample = table.TableSample(
    table_reference=_TEST_SOURCE_TABLE_REF, sample=_TEST_SAMPLE_RANDOM
)
_TEST_SAMPLE_REQUEST_SORTED: table.TableSample = table.TableSample(
    table_reference=_TEST_SOURCE_TABLE_REF, sample=_TEST_SAMPLE_SORTED
)
TEST_EVENT_REQUEST_START: request.EventRequestStart = request.EventRequestStart(
    type=request.RequestType.START.value, request_timestamp=17
)
TEST_EVENT_REQUEST_SAMPLE_START: request.EventRequestSampleStart = request.EventRequestSampleStart(
    type=request.RequestType.SAMPLE_START.value,
    request_timestamp=17,
    sample_request=_TEST_SAMPLE_REQUEST,
    target_table=_TEST_TARGET_TABLE_REF,
)
TEST_EVENT_REQUEST_SAMPLE_START_RANDOM: request.EventRequestSampleStart = (
    request.EventRequestSampleStart(
        type=request.RequestType.SAMPLE_START.value,
        request_timestamp=17,
        sample_request=_TEST_SAMPLE_REQUEST_RANDOM,
        target_table=_TEST_TARGET_TABLE_REF,
    )
)
TEST_EVENT_REQUEST_SAMPLE_START_SORTED: request.EventRequestSampleStart = (
    request.EventRequestSampleStart(
        type=request.RequestType.SAMPLE_START.value,
        request_timestamp=17,
        sample_request=_TEST_SAMPLE_REQUEST_SORTED,
        target_table=_TEST_TARGET_TABLE_REF,
    )
)
TEST_EVENT_REQUEST_SAMPLE_DONE: request.EventRequestSampleDone = request.EventRequestSampleDone(
    type=request.RequestType.SAMPLE_DONE.value,
    request_timestamp=17,
    sample_request=_TEST_SAMPLE_REQUEST,
    target_table=_TEST_TARGET_TABLE_REF,
    start_timestamp=31,
    end_timestamp=79,
    error_message='NO_ERROR',
)
