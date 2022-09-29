# vim: ai:sw=4:ts=4:sta:et:fo=croql
# pylint: disable=missing-module-docstring,missing-class-docstring,missing-function-docstring
# pylint: disable=missing-function-docstring,assignment-from-no-return,c-extension-no-member
# pylint: disable=protected-access,redefined-outer-name,no-self-use,using-constant-test
# pylint: disable=invalid-name,attribute-defined-outside-init,too-few-public-methods, redefined-builtin
# type: ignore
from bq_sampler.entity import command
from bq_sampler.entity import table

_TEST_POLICY_PREFIX: str = 'project_id_a/dataset_id_a'
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
_TEST_SAMPLE_SPEC: table.SampleSpec = table.SampleSpec()
_TEST_SAMPLE_SPEC_RANDOM: table.SampleSpec = table.SampleSpec(type=table.SortType.RANDOM.value)
_TEST_SORT_BY: str = 'TEST_COLUMN'
_TEST_SORT_DIR: str = table.SortDirection.DESC.value
_TEST_SORT_PROPERTIES: table._SortProperties = table._SortProperties(
    by=_TEST_SORT_BY, direction=_TEST_SORT_DIR
)
_TEST_SAMPLE_SPEC_SORTED: table.SampleSpec = table.SampleSpec(
    type=table.SortType.SORTED.value, properties=_TEST_SORT_PROPERTIES
)
_TEST_SAMPLE: table.Sample = table.Sample(size=_TEST_SAMPLE_SIZE, spec=_TEST_SAMPLE_SPEC)
_TEST_SAMPLE_RANDOM: table.Sample = table.Sample(
    size=_TEST_SAMPLE_SIZE, spec=_TEST_SAMPLE_SPEC_RANDOM
)
_TEST_SAMPLE_SORTED: table.Sample = table.Sample(
    size=_TEST_SAMPLE_SIZE, spec=_TEST_SAMPLE_SPEC_SORTED
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
TEST_COMMAND_START: command.CommandStart = command.CommandStart(
    type=command.CommandType.START.value, timestamp=17
)
TEST_COMMAND_SAMPLE_START: command.CommandSampleStart = command.CommandSampleStart(
    type=command.CommandType.SAMPLE_START.value,
    timestamp=17,
    sample_request=_TEST_SAMPLE_REQUEST,
    target_table=_TEST_TARGET_TABLE_REF,
)
TEST_COMMAND_SAMPLE_POLICY_PREFIX: command.CommandSamplePolicyPrefix = (
    command.CommandSamplePolicyPrefix(
        type=command.CommandType.SAMPLE_POLICY_PREFIX.value,
        timestamp=17,
        prefix=_TEST_POLICY_PREFIX,
    )
)
TEST_COMMAND_SAMPLE_START_RANDOM: command.CommandSampleStart = command.CommandSampleStart(
    type=command.CommandType.SAMPLE_START.value,
    timestamp=17,
    sample_request=_TEST_SAMPLE_REQUEST_RANDOM,
    target_table=_TEST_TARGET_TABLE_REF,
)
TEST_COMMAND_SAMPLE_START_SORTED: command.CommandSampleStart = command.CommandSampleStart(
    type=command.CommandType.SAMPLE_START.value,
    timestamp=17,
    sample_request=_TEST_SAMPLE_REQUEST_SORTED,
    target_table=_TEST_TARGET_TABLE_REF,
)
TEST_COMMAND_SAMPLE_DONE: command.CommandSampleDone = command.CommandSampleDone(
    type=command.CommandType.SAMPLE_DONE.value,
    timestamp=17,
    sample_request=_TEST_SAMPLE_REQUEST,
    target_table=_TEST_TARGET_TABLE_REF,
    start_timestamp=31,
    end_timestamp=79,
    error_message='NO_ERROR',
)
