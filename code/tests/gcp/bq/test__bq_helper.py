# vim: ai:sw=4:ts=4:sta:et:fo=croql
# pylint: disable=missing-module-docstring,missing-class-docstring,missing-function-docstring
# pylint: disable=missing-function-docstring,assignment-from-no-return,c-extension-no-member
# pylint: disable=protected-access,redefined-outer-name,no-self-use,using-constant-test
# pylint: disable=invalid-name,attribute-defined-outside-init,too-few-public-methods, redefined-builtin
# type: ignore

from typing import Dict, Generator, Optional

from google.cloud import bigquery

import pytest

from bq_sampler.gcp.bq import _bq_helper


_TEST_SOURCE_TABLE_FQN_ID: str = (
    'test_project_id_a.test_dataset_id_a.test_table_id_a@test_location_a'
)


class _StubTable:
    def __init__(self, *, num_rows: Optional[int] = None):
        self.num_rows = num_rows


def test_row_count_ok(monkeypatch):
    # Given
    expected = 17
    bq_table = _StubTable(num_rows=expected)
    _mock_calls__big_query(monkeypatch, bq_table=bq_table)
    # When
    result = _bq_helper.row_count(table_fqn_id=_TEST_SOURCE_TABLE_FQN_ID)
    # Then
    assert result == expected


def _mock_calls__big_query(
    monkeypatch,
    *,
    bq_table: Optional[bigquery.Table] = None,
    query_job: Optional[bigquery.job.query.QueryJob] = None,
    table_fqn_id_lst: Optional[Generator[str, None, None]] = None,
    drop_table_exception: Optional[Exception] = None,
) -> None:
    def mocked_table(*args, **kwargs) -> bigquery.Table:  # pylint: disable=unused-argument
        return bq_table

    monkeypatch.setattr(_bq_helper._bq_base, 'table', mocked_table)

    def mocked_query_job(  # pylint: disable=unused-argument
        *args, **kwargs
    ) -> bigquery.job.query.QueryJob:
        return query_job

    monkeypatch.setattr(_bq_helper._bq_base, 'query_job', mocked_query_job)

    if not table_fqn_id_lst:
        table_fqn_id_lst = []

    def mocked_list_all_tables_with_filter(  # pylint: disable=unused-argument
        *args, **kwargs
    ) -> Generator[str, None, None]:
        assert kwargs.get('filter_fn') is not None
        for id in table_fqn_id_lst:
            yield id

    monkeypatch.setattr(
        _bq_helper._bq_base, 'list_all_tables_with_filter', mocked_list_all_tables_with_filter
    )

    def mocked_drop_table(*args, **kwargs) -> None:  # pylint: disable=unused-argument
        assert kwargs.get('table_fqn_id') is not None
        if drop_table_exception is not None:
            raise drop_table_exception

    monkeypatch.setattr(_bq_helper._bq_base, 'drop_table', mocked_drop_table)


class _StubQueryJob:
    def __init__(
        self,
        *,
        query: Optional[str] = None,
        result: Optional[bigquery.table.RowIterator] = None,
        result_exception: Optional[Exception] = None,
    ):
        self.query = query
        self._result = result
        self._result_exception = result_exception

    def result(self) -> bigquery.table.RowIterator:
        if self._result_exception is not None:
            raise self._result_exception
        return self._result


def test_query_job_result_ok(monkeypatch):
    # Given
    expected = 'TEST_RESULT'
    query_job = _StubQueryJob(result=expected)
    _mock_calls__big_query(monkeypatch, query_job=query_job)
    # When
    result = _bq_helper.query_job_result(query='TEST_QUERY')
    # Then
    assert result == expected


def test_query_job_result_nok(monkeypatch):
    # Given
    query_job = _StubQueryJob(result_exception=ConnectionError('TEST_EXCEPTION'))
    _mock_calls__big_query(monkeypatch, query_job=query_job)
    # When/Then
    with pytest.raises(RuntimeError):
        _bq_helper.query_job_result(query='TEST_QUERY')


_TEST_PROJECT_ID: str = 'TEST_PROJECT_ID'
_TEST_LOCATION: str = 'TEST_LOCATION'
_TEST_LABELS: Dict[str, str] = {'TEST_LABEL_KEY': 'TEST_LABEL_VALUE'}


def test_drop_all_tables_by_labels_ok(monkeypatch):
    # Given
    _mock_calls__big_query(monkeypatch, table_fqn_id_lst=[_TEST_SOURCE_TABLE_FQN_ID])
    # When/Then
    _bq_helper.drop_all_tables_by_labels(
        project_id=_TEST_PROJECT_ID, location=_TEST_LOCATION, labels=_TEST_LABELS
    )


def test_drop_all_tables_by_labels_nok(monkeypatch):
    # Given
    _mock_calls__big_query(
        monkeypatch,
        table_fqn_id_lst=[_TEST_SOURCE_TABLE_FQN_ID],
        drop_table_exception=ConnectionError(),
    )
    # When/Then
    with pytest.raises(RuntimeError):
        _bq_helper.drop_all_tables_by_labels(
            project_id=_TEST_PROJECT_ID, location=_TEST_LOCATION, labels=_TEST_LABELS
        )
