# vim: ai:sw=4:ts=4:sta:et:fo=croql
# pylint: disable=missing-module-docstring,missing-class-docstring,missing-function-docstring
# pylint: disable=missing-function-docstring,assignment-from-no-return,c-extension-no-member
# pylint: disable=protected-access,redefined-outer-name,no-self-use,using-constant-test
# pylint: disable=invalid-name,attribute-defined-outside-init,too-few-public-methods, redefined-builtin
# type: ignore
from bq_sampler.entity import policy
from bq_sampler.entity import table


TEST_SAMPLE_SIZE: table.SizeSpec = table.SizeSpec(count=17, percentage=11.1)
_TEST_COLUMN: str = "TEST_COLUMN"
_TEST_SORT_PROPERTIES: table._SortProperties = table._SortProperties(
    by=_TEST_COLUMN, direction=table.SortDirection.ASC.value
)
_TEST_SORT_ALGORITHM: table.SampleSpec = table.SampleSpec(
    type=table.SortType.SORTED.value, properties=_TEST_SORT_PROPERTIES
)
TEST_SAMPLE: table.Sample = table.Sample(size=TEST_SAMPLE_SIZE, spec=_TEST_SORT_ALGORITHM)

_TEST_PROJECT_ID: str = 'TEST_PROJECT_ID'
_TEST_DATASET_ID: str = 'TEST_DATASET_ID'
_TEST_TABLE_ID: str = 'TEST_TABLE_ID'
_TEST_LOCATION: str = 'TEST_REGION'
TEST_TABLE_REFERENCE: table.TableReference = table.TableReference(
    project_id=_TEST_PROJECT_ID,
    dataset_id=_TEST_DATASET_ID,
    table_id=_TEST_TABLE_ID,
    location=_TEST_LOCATION,
)
TEST_POLICY: policy.Policy = policy.Policy(limit=TEST_SAMPLE_SIZE, default_sample=TEST_SAMPLE)
TEST_TABLE_POLICY: policy.TablePolicy = policy.TablePolicy(
    table_reference=TEST_TABLE_REFERENCE, policy=TEST_POLICY
)
TEST_TABLE_SAMPLE: table.TableSample = table.TableSample(
    table_reference=TEST_TABLE_REFERENCE, sample=TEST_SAMPLE
)
