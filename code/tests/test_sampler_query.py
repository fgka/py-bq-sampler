# vim: ai:sw=4:ts=4:sta:et:fo=croql
# pylint: disable=missing-module-docstring,missing-class-docstring,missing-function-docstring
# pylint: disable=missing-function-docstring,assignment-from-no-return,c-extension-no-member
# pylint: disable=protected-access,redefined-outer-name,no-self-use,using-constant-test
# pylint: disable=invalid-name,attribute-defined-outside-init,too-few-public-methods, redefined-builtin
# type: ignore

from typing import Any, Callable, List, Optional

from google.cloud import bigquery

import pytest

from bq_sampler import const
from bq_sampler.entity import table
from bq_sampler import sampler_query


_TEST_SOURCE_TABLE_FQN_ID: str = (
    'test_project_id_a.test_dataset_id_a.test_table_id_a@test_location_a'
)
_TEST_SOURCE_TABLE_REF: table.TableReference = table.TableReference.from_str(
    _TEST_SOURCE_TABLE_FQN_ID
)
_TEST_TARGET_TABLE_FQN_ID: str = (
    'test_project_id_b.test_dataset_id_b.test_table_id_b@test_location_a'
)
_TEST_TARGET_TABLE_REF: table.TableReference = table.TableReference.from_str(
    _TEST_TARGET_TABLE_FQN_ID
)
_TEST_TARGET_DIFF_LOC_TABLE_FQN_ID: str = (
    'test_project_id_b.test_dataset_id_b.test_table_id_b@test_location_b'
)
_TEST_TARGET_DIFF_LOC_TABLE_REF: table.TableReference = table.TableReference.from_str(
    _TEST_TARGET_DIFF_LOC_TABLE_FQN_ID
)
_TEST_SAMPLE_AMOUNT: int = 13


def test_random_sample_ok(monkeypatch):
    # Given
    query_validation_fn = _query_validation_fn(is_random_query=True, has_insert=False)
    _mock_calls_bq(monkeypatch, query_validation_fn=query_validation_fn)
    # When
    result = sampler_query.random_sample(
        table_ref=_TEST_SOURCE_TABLE_REF, amount=_TEST_SAMPLE_AMOUNT
    )
    # Then
    assert result == _DEFAULT_MOCKED_QUERY_JOB_RESULT


_DEFAULT_MOCKED_QUERY_JOB_RESULT: str = "MOCKED_QUERY_JOB_RESULT"
_DEFAULT_MOCKED_ROW_COUNT: int = 1000

_COMMON_QUERY_SUB_STRINGS: List[str] = ['SELECT * FROM `', 'LIMIT ']
_INSERT_QUERY_SUB_STRINGS: List[str] = ['INSERT INTO `']
_RANDOM_QUERY_SUB_STRINGS: List[str] = [
    'TABLESAMPLE SYSTEM (',
    'PERCENT)',
]
_SORTED_QUERY_SUB_STRINGS: List[str] = [
    'ORDER BY ',
]


def _query_validation_fn(
    *,
    extra_query_sub_strings: Optional[List[str]] = None,
    is_random_query: Optional[bool] = True,
    has_insert: Optional[bool] = False,
) -> Callable[[str], None]:
    query_sub_strings = []
    query_sub_strings.extend(_COMMON_QUERY_SUB_STRINGS)
    if is_random_query:
        query_sub_strings.extend(_RANDOM_QUERY_SUB_STRINGS)
    else:
        query_sub_strings.extend(_SORTED_QUERY_SUB_STRINGS)
    if has_insert:
        query_sub_strings.extend(_INSERT_QUERY_SUB_STRINGS)
    if extra_query_sub_strings:
        query_sub_strings.extend(extra_query_sub_strings)

    def validation_fn(query: str) -> None:
        for sub_str in query_sub_strings:
            assert sub_str in query, f'Could not find <{sub_str}> in query <{query}>'

    return validation_fn


def _mock_calls_bq(
    monkeypatch,
    *,
    query_job_result: Optional[Any] = _DEFAULT_MOCKED_QUERY_JOB_RESULT,
    row_count: Optional[int] = _DEFAULT_MOCKED_ROW_COUNT,
    query_validation_fn: Optional[Callable[[str], None]] = None,
) -> None:
    def mocked_bq_query_job_result(*args, **kwargs) -> Any:  # pylint: disable=unused-argument
        if query_validation_fn is not None:
            query_validation_fn(kwargs.get('query'))
        return query_job_result

    monkeypatch.setattr(sampler_query.bq, 'query_job_result', mocked_bq_query_job_result)

    def mocked_bq_row_count(*args, **kwargs) -> int:  # pylint: disable=unused-argument
        return row_count

    monkeypatch.setattr(sampler_query.bq, 'row_count', mocked_bq_row_count)

    def mocked_bq_table(*, table_fqn_id: str) -> bigquery.Table:
        return bigquery.Table(table_fqn_id.split(const.BQ_TABLE_FQN_LOCATION_SEP)[0])

    monkeypatch.setattr(sampler_query.bq, 'table', mocked_bq_table)

    def mocked_bq_create_table(*args, **kwargs) -> None:  # pylint: disable=unused-argument
        pass

    monkeypatch.setattr(sampler_query.bq, 'create_table', mocked_bq_create_table)


@pytest.mark.parametrize(
    'table_ref,amount',
    [
        (None, None),  # None testing
        (_TEST_SOURCE_TABLE_REF, None),
        (None, _TEST_SAMPLE_AMOUNT),
        (_TEST_SOURCE_TABLE_FQN_ID, _TEST_SAMPLE_AMOUNT),  # table_ref is a string
        (_TEST_SOURCE_TABLE_REF, -1),
    ],
)
def test_random_sample_nok(monkeypatch, table_ref: Any, amount: Any):
    # Given
    _mock_calls_bq(monkeypatch)
    # When/Then
    with pytest.raises(ValueError):
        sampler_query.random_sample(table_ref=table_ref, amount=amount)


_TEST_SORT_COLUMN_NAME: str = 'TEST_COLUMN'
_TEST_SORT_ORDER: str = const.BQ_ORDER_BY_DESC


def test_sorted_sample_ok(monkeypatch):
    # Given
    query_validation_fn = _query_validation_fn(is_random_query=False, has_insert=False)
    _mock_calls_bq(monkeypatch, query_validation_fn=query_validation_fn)
    # When
    result = sampler_query.sorted_sample(
        table_ref=_TEST_SOURCE_TABLE_REF,
        amount=_TEST_SAMPLE_AMOUNT,
        column=_TEST_SORT_COLUMN_NAME,
        order=_TEST_SORT_ORDER,
    )
    # Then
    assert result == _DEFAULT_MOCKED_QUERY_JOB_RESULT


@pytest.mark.parametrize(
    'table_ref,amount,column,order',
    [
        (None, None, None, None),  # None testing
        (None, _TEST_SAMPLE_AMOUNT, _TEST_SORT_COLUMN_NAME, _TEST_SORT_ORDER),
        (_TEST_SOURCE_TABLE_REF, None, _TEST_SORT_COLUMN_NAME, _TEST_SORT_ORDER),
        (_TEST_SOURCE_TABLE_REF, _TEST_SAMPLE_AMOUNT, None, _TEST_SORT_ORDER),
        (_TEST_SOURCE_TABLE_REF, _TEST_SAMPLE_AMOUNT, _TEST_SORT_COLUMN_NAME, None),
        (
            _TEST_SOURCE_TABLE_FQN_ID,
            _TEST_SAMPLE_AMOUNT,
            _TEST_SORT_COLUMN_NAME,
            _TEST_SORT_ORDER,
        ),  # table_ref is a string
        (_TEST_SOURCE_TABLE_REF, -1, _TEST_SORT_COLUMN_NAME, _TEST_SORT_ORDER),
        (
            _TEST_SOURCE_TABLE_REF,
            _TEST_SAMPLE_AMOUNT,
            '',
            _TEST_SORT_ORDER,
        ),  # column not a valid string
        (_TEST_SOURCE_TABLE_REF, _TEST_SAMPLE_AMOUNT, ' ', _TEST_SORT_ORDER),
        (_TEST_SOURCE_TABLE_REF, _TEST_SAMPLE_AMOUNT, '\n', _TEST_SORT_ORDER),
        (
            _TEST_SOURCE_TABLE_REF,
            _TEST_SAMPLE_AMOUNT,
            _TEST_SORT_COLUMN_NAME,
            _TEST_SORT_ORDER + '_WRONG',
        ),  # invalid order
        (_TEST_SOURCE_TABLE_REF, _TEST_SAMPLE_AMOUNT, _TEST_SORT_COLUMN_NAME, 'NOT_VALID'),
    ],
)
def test_sorted_sample_nok(monkeypatch, table_ref: Any, amount: Any, column: Any, order: Any):
    # Given
    _mock_calls_bq(monkeypatch)
    # When/Then
    with pytest.raises(ValueError):
        sampler_query.sorted_sample(table_ref=table_ref, amount=amount, column=column, order=order)


@pytest.mark.parametrize(
    'row_count,amount,expected',
    [
        (10, 10, 100),
        (1, 1, 100),
        (1, 10, 100),  # more amount than available
        (1_000_000, 1, 1),  # very small amount compared to available
    ],
)
def test__int_percent_for_tablesample_stmt_ok(
    monkeypatch, row_count: int, amount: int, expected: int
):
    # Given
    _mock_calls_bq(monkeypatch, row_count=row_count)
    # When
    result = sampler_query._int_percent_for_tablesample_stmt(_TEST_SOURCE_TABLE_FQN_ID, amount)
    # Then
    assert result == expected


def test__named_placeholders_ok():
    # Given
    source_table_fqn_id = _TEST_SOURCE_TABLE_FQN_ID
    target_table_fqn_id = _TEST_TARGET_TABLE_FQN_ID
    amount = 11
    percent_int = 13
    column = _TEST_SORT_COLUMN_NAME
    order = _TEST_SORT_ORDER
    # When
    result = sampler_query._named_placeholders(
        source_table_fqn_id=source_table_fqn_id,
        target_table_fqn_id=target_table_fqn_id,
        amount=amount,
        percent_int=percent_int,
        column=column,
        order=order,
    )
    # Then
    assert isinstance(result, dict)
    assert result.get(sampler_query._BQ_SOURCE_TABLE_PARAM) == source_table_fqn_id
    assert result.get(sampler_query._BQ_TARGET_TABLE_PARAM) == target_table_fqn_id
    assert result.get(sampler_query._BQ_ROW_AMOUNT_INT_PARAM) == amount
    assert result.get(sampler_query._BQ_PERCENT_INT_PARAM) == percent_int
    assert result.get(sampler_query._BQ_ORDER_BY_COLUMN) == column
    assert result.get(sampler_query._BQ_ORDER_BY_DIRECTION) == order


def test__int_percent_for_tablesample_stmt_nok_empty_table(monkeypatch):
    # Given
    _mock_calls_bq(monkeypatch, row_count=0)
    # When/Then
    with pytest.raises(ValueError):
        sampler_query._int_percent_for_tablesample_stmt(_TEST_SOURCE_TABLE_FQN_ID, 1)


def test_create_table_with_random_sample_ok(monkeypatch):
    # Given
    query_validation_fn = _query_validation_fn(is_random_query=True, has_insert=True)
    _mock_calls_bq(monkeypatch, query_validation_fn=query_validation_fn)
    # When/Then
    sampler_query.create_table_with_random_sample(
        source_table_ref=_TEST_SOURCE_TABLE_REF,
        target_table_ref=_TEST_TARGET_TABLE_REF,
        amount=_TEST_SAMPLE_AMOUNT,
    )


@pytest.mark.parametrize(
    'source_table_ref,target_table_ref,amount',
    [
        (None, None, None),  # None
        (None, _TEST_TARGET_TABLE_REF, _TEST_SAMPLE_AMOUNT),
        (_TEST_SOURCE_TABLE_REF, None, _TEST_SAMPLE_AMOUNT),
        (_TEST_SOURCE_TABLE_REF, _TEST_TARGET_TABLE_REF, None),
        (
            _TEST_SOURCE_TABLE_REF,
            _TEST_TARGET_DIFF_LOC_TABLE_REF,
            _TEST_SAMPLE_AMOUNT,
        ),  # different regions
        (_TEST_SOURCE_TABLE_REF, _TEST_TARGET_TABLE_REF, -1),
    ],
)
def test_create_table_with_random_sample_nok(
    monkeypatch, source_table_ref: Any, target_table_ref: Any, amount: Any
):
    # Given
    _mock_calls_bq(monkeypatch)
    # When/Then
    with pytest.raises(ValueError):
        sampler_query.create_table_with_random_sample(
            source_table_ref=source_table_ref,
            target_table_ref=target_table_ref,
            amount=amount,
        )


def test_create_table_with_sorted_sample_ok(monkeypatch):
    # Given
    query_validation_fn = _query_validation_fn(is_random_query=False, has_insert=True)
    _mock_calls_bq(monkeypatch, query_validation_fn=query_validation_fn)
    # When/Then
    sampler_query.create_table_with_sorted_sample(
        source_table_ref=_TEST_SOURCE_TABLE_REF,
        target_table_ref=_TEST_TARGET_TABLE_REF,
        amount=_TEST_SAMPLE_AMOUNT,
        column=_TEST_SORT_COLUMN_NAME,
        order=_TEST_SORT_ORDER,
    )


@pytest.mark.parametrize(
    'source_table_ref,target_table_ref,amount,column,order',
    [
        (None, None, None, None, None),  # None
        (
            None,
            _TEST_TARGET_TABLE_REF,
            _TEST_SAMPLE_AMOUNT,
            _TEST_SORT_COLUMN_NAME,
            _TEST_SORT_ORDER,
        ),
        (
            _TEST_SOURCE_TABLE_REF,
            None,
            _TEST_SAMPLE_AMOUNT,
            _TEST_SORT_COLUMN_NAME,
            _TEST_SORT_ORDER,
        ),
        (
            _TEST_SOURCE_TABLE_REF,
            _TEST_TARGET_TABLE_REF,
            None,
            _TEST_SORT_COLUMN_NAME,
            _TEST_SORT_ORDER,
        ),
        (
            _TEST_SOURCE_TABLE_REF,
            _TEST_TARGET_TABLE_REF,
            _TEST_SAMPLE_AMOUNT,
            None,
            _TEST_SORT_ORDER,
        ),
        (
            _TEST_SOURCE_TABLE_REF,
            _TEST_TARGET_TABLE_REF,
            _TEST_SAMPLE_AMOUNT,
            _TEST_SORT_COLUMN_NAME,
            None,
        ),
        (
            _TEST_SOURCE_TABLE_REF,
            _TEST_TARGET_DIFF_LOC_TABLE_REF,
            _TEST_SAMPLE_AMOUNT,
            _TEST_SORT_COLUMN_NAME,
            _TEST_SORT_ORDER,
        ),  # different regions
        (
            _TEST_SOURCE_TABLE_REF,
            _TEST_TARGET_TABLE_REF,
            -1,
            _TEST_SORT_COLUMN_NAME,
            _TEST_SORT_ORDER,
        ),
        (
            _TEST_SOURCE_TABLE_REF,
            _TEST_TARGET_TABLE_REF,
            _TEST_SAMPLE_AMOUNT,
            '',
            _TEST_SORT_ORDER,
        ),  # invalid column
        (
            _TEST_SOURCE_TABLE_REF,
            _TEST_TARGET_TABLE_REF,
            _TEST_SAMPLE_AMOUNT,
            '\n',
            _TEST_SORT_ORDER,
        ),
        (
            _TEST_SOURCE_TABLE_REF,
            _TEST_TARGET_TABLE_REF,
            _TEST_SAMPLE_AMOUNT,
            _TEST_SORT_COLUMN_NAME,
            _TEST_SORT_ORDER + '_WRONG',
        ),  # invalid order
        (
            _TEST_SOURCE_TABLE_REF,
            _TEST_TARGET_TABLE_REF,
            _TEST_SAMPLE_AMOUNT,
            _TEST_SORT_COLUMN_NAME,
            'INVALID',
        ),
        (
            _TEST_SOURCE_TABLE_REF,
            _TEST_TARGET_TABLE_REF,
            _TEST_SAMPLE_AMOUNT,
            _TEST_SORT_COLUMN_NAME,
            '',
        ),
    ],
)
def test_create_table_with_sorted_sample_nok(  # pylint: disable=too-many-arguments
    monkeypatch, source_table_ref: Any, target_table_ref: Any, amount: Any, column: Any, order: Any
):
    # Given
    _mock_calls_bq(monkeypatch)
    # When/Then
    with pytest.raises(ValueError):
        sampler_query.create_table_with_sorted_sample(
            source_table_ref=source_table_ref,
            target_table_ref=target_table_ref,
            amount=amount,
            column=column,
            order=order,
        )
