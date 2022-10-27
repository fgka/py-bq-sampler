# vim: ai:sw=4:ts=4:sta:et:fo=croql
# pylint: disable=missing-module-docstring,missing-class-docstring,missing-function-docstring
# pylint: disable=missing-function-docstring,assignment-from-no-return,c-extension-no-member
# pylint: disable=protected-access,redefined-outer-name,no-self-use,using-constant-test
# pylint: disable=invalid-name,attribute-defined-outside-init,too-few-public-methods, redefined-builtin
# type: ignore
from typing import Any, Dict, Optional

from google.cloud import bigquery
from google.api_core import page_iterator

import pytest

from bq_sampler import const
from bq_sampler.gcp.bq import _bq_base


_TEST_PROJECT_ID: str = 'test_project_id_a'
_TEST_DATASET_ID: str = 'test_dataset_id_a'
_TEST_TABLE_ID: str = 'test_table_id_a'
_TEST_LOCATION: str = 'test_location_a'
_TEST_TABLE_ID_ONLY: str = const.BQ_TABLE_FQN_ID_SEP.join(
    [_TEST_PROJECT_ID, _TEST_DATASET_ID, _TEST_TABLE_ID]
)
_TEST_TABLE_FQN_ID: str = f'{_TEST_TABLE_ID_ONLY}{const.BQ_TABLE_FQN_LOCATION_SEP}{_TEST_LOCATION}'


class _StubClient:  # pylint: disable=too-many-instance-attributes,too-many-locals
    def __init__(
        self,
        *,
        project_id: Optional[str] = None,
        bq_table: Optional[bigquery.Table] = None,
        get_table_exception: Optional[Exception] = None,
        query_job: Optional[bigquery.job.query.QueryJob] = None,
        query: Optional[str] = None,
        job_config: Optional[bigquery.QueryJobConfig] = None,
        query_exception: Optional[Exception] = None,
        create_dataset_exception: Optional[Exception] = None,
        update_dataset_exception: Optional[Exception] = None,
        create_table_exception: Optional[Exception] = None,
        update_table_exception: Optional[Exception] = None,
        delete_table_exception: Optional[Exception] = None,
        list_datasets: Optional[page_iterator.Iterator] = None,
        list_datasets_exception: Optional[Exception] = None,
        list_tables: Optional[page_iterator.Iterator] = None,
        list_tables_exception: Optional[Exception] = None,
    ):
        self.project = project_id
        self._table = bq_table
        self._get_table_exception = get_table_exception
        self._query_job = query_job
        self._query = query
        self._job_config = job_config
        self._query_exception = query_exception
        self._create_dataset_exception = create_dataset_exception
        self._update_dataset_exception = update_dataset_exception
        self._create_table_exception = create_table_exception
        self._update_table_exception = update_table_exception
        self._delete_table_exception = delete_table_exception
        self._list_datasets = list_datasets
        self._list_datasets_exception = list_datasets_exception
        self._list_tables = list_tables
        self._list_tables_exception = list_tables_exception

    def get_table(self, *args) -> bigquery.Table:
        if self._get_table_exception is not None:
            raise self._get_table_exception
        assert args[0] == self._table.full_table_id.split(const.BQ_TABLE_FQN_LOCATION_SEP)[0]
        return self._table

    def query(self, *args, **kwargs) -> bigquery.job.query.QueryJob:
        if self._query_exception is not None:
            raise self._query_exception
        if self._query is not None:
            assert args[0] == self._query
        if self._job_config is not None:
            assert kwargs.get('job_config') == self._job_config
        return self._query_job

    def create_dataset(  # pylint: disable=unused-argument
        self, *args, **kwargs
    ) -> bigquery.Dataset:
        if self._create_dataset_exception is not None:
            raise self._create_dataset_exception
        return args[0]

    def update_dataset(  # pylint: disable=unused-argument
        self, *args, **kwargs
    ) -> bigquery.Dataset:
        if self._update_dataset_exception is not None:
            raise self._update_dataset_exception
        assert 'labels' in args[1]
        return args[0]

    def create_table(self, *args, **kwargs) -> bigquery.Table:  # pylint: disable=unused-argument
        if self._create_table_exception is not None:
            raise self._create_table_exception
        return args[0]

    def update_table(self, *args, **kwargs) -> bigquery.Table:  # pylint: disable=unused-argument
        if self._update_table_exception is not None:
            raise self._update_table_exception
        assert 'labels' in args[1]
        assert 'schema' in args[1]
        return args[0]

    def delete_table(self, *args, **kwargs) -> None:  # pylint: disable=unused-argument
        if self._delete_table_exception is not None:
            raise self._delete_table_exception
        assert args[0]

    def list_datasets(  # pylint: disable=unused-argument
        self, *args, **kwargs
    ) -> page_iterator.Iterator:
        if self._list_datasets_exception is not None:
            raise self._list_datasets_exception
        assert kwargs.get('include_all')
        for ds in self._list_datasets:
            yield _StubDataset(dataset_id=ds, project=self.project)

    def list_tables(  # pylint: disable=unused-argument
        self, *args, **kwargs
    ) -> page_iterator.Iterator:
        if self._list_tables_exception is not None:
            raise self._list_tables_exception
        for t in self._list_tables:
            yield _StubTable(table_id=t)


class _StubJobConfig:
    def __init__(self, *, query_parameters: Optional[bigquery.query.ArrayQueryParameter] = None):
        self.query_parameters = query_parameters


class _StubQueryJob:
    def __init__(
        self,
        *,
        query: Optional[str] = None,
        query_parameters: Optional[bigquery.query.ArrayQueryParameter] = None,
        total_bytes_processed: Optional[int] = None,
        total_bytes_billed: Optional[int] = None,
        slot_millis: Optional[int] = None,
    ):
        self.query = query
        self.query_parameters = query_parameters
        self.total_bytes_processed = total_bytes_processed
        self.total_bytes_billed = total_bytes_billed
        self.slot_millis = slot_millis


class _StubDataset:
    def __init__(
        self,
        *,
        bq_table: Optional[bigquery.Table] = None,
        dataset_id: Optional[str] = None,
        project: Optional[str] = None,
    ):
        self._table = bq_table
        self.dataset_id = dataset_id
        self.project = project

    def table(self, *args, **kwargs) -> bigquery.Table:  # pylint: disable=unused-argument
        assert args[0] == self._table.table_id
        return self._table


class _StubTable:
    def __init__(self, *, full_table_id: Optional[str] = None, table_id: Optional[str] = None):
        self.full_table_id = full_table_id
        self.table_id = table_id


def test_table_ok(monkeypatch):
    # Given
    expected = _StubTable(full_table_id=_TEST_TABLE_FQN_ID)
    client = _StubClient(bq_table=expected)
    _mock_client(monkeypatch, client=client, project_id=_TEST_PROJECT_ID, location=_TEST_LOCATION)
    # When
    result = _bq_base.table(table_fqn_id=_TEST_TABLE_FQN_ID)
    # Then
    assert result == expected


def _mock_client(
    monkeypatch,
    *,
    client: Optional[_StubClient] = None,
    project_id: Optional[str] = None,
    location: Optional[str] = None,
) -> None:
    def mocked_client(*args, **kwargs) -> bigquery.Client:  # pylint: disable=unused-argument
        if project_id is not None:
            assert args[0] == project_id
            client.project = project_id
        if location is not None and len(args) > 1:
            assert args[1] == location
        return client

    monkeypatch.setattr(_bq_base, '_client', mocked_client)


def test_table_nok(monkeypatch):
    # Given
    client = _StubClient(get_table_exception=ConnectionError())
    _mock_client(monkeypatch, client=client, project_id=_TEST_PROJECT_ID, location=_TEST_LOCATION)
    # When/Then
    with pytest.raises(ValueError):
        _bq_base.table(table_fqn_id=_TEST_TABLE_FQN_ID)


_TEST_QUERY: str = 'TEST_QUERY'
_TEST_QUERY_PARAMETERS: str = 'TEST_QUERY_PARAMETERS'
_TEST_JOB_CONFIG: _StubJobConfig = _StubJobConfig(query_parameters=_TEST_QUERY_PARAMETERS)
_TEST_QUERY_JOB: _StubQueryJob = _StubQueryJob(
    query=_TEST_QUERY,
    query_parameters=_TEST_QUERY_PARAMETERS,
    total_bytes_processed=17,
    total_bytes_billed=13,
    slot_millis=1000,
)


def test_query_job_ok(monkeypatch):
    # Given
    query = _TEST_QUERY
    job_config = _TEST_JOB_CONFIG
    project_id = _TEST_PROJECT_ID
    location = _TEST_LOCATION
    expected = _TEST_QUERY_JOB
    client = _StubClient(query_job=expected, query=query, job_config=job_config)
    _mock_client(monkeypatch, client=client, project_id=_TEST_PROJECT_ID, location=_TEST_LOCATION)
    # When
    result = _bq_base.query_job(
        query=query, job_config=job_config, project_id=project_id, location=location
    )
    # Then
    assert result == expected


def test_query_job_nok(monkeypatch):
    # Given
    query = _TEST_QUERY
    job_config = _TEST_JOB_CONFIG
    project_id = _TEST_PROJECT_ID
    location = _TEST_LOCATION
    expected = _TEST_QUERY_JOB
    client = _StubClient(query_job=expected, query_exception=ConnectionError())
    _mock_client(monkeypatch, client=client, project_id=_TEST_PROJECT_ID, location=_TEST_LOCATION)
    # When/Then
    with pytest.raises(RuntimeError):
        _bq_base.query_job(
            query=query, job_config=job_config, project_id=project_id, location=location
        )


_TEST_SCHEMA: Dict[str, Any] = {
    'TEST_COLUMN_A': int,
    'TEST_COLUMN_B': str,
}
_TEST_LABELS: Dict[str, str] = {
    'TEST_LABEL_KEY_A': 'TEST_LABEL_VAL_A',
    'TEST_LABEL_KEY_B': 'TEST_LABEL_VAL_B',
}


def test_create_table_ok(monkeypatch):
    # Given
    table_fqn_id = _TEST_TABLE_FQN_ID
    schema = _TEST_SCHEMA
    labels = _TEST_LABELS
    client = _StubClient()
    _mock_client(monkeypatch, client=client, project_id=_TEST_PROJECT_ID, location=_TEST_LOCATION)
    # When/Then
    _bq_base.create_table(
        table_fqn_id=table_fqn_id, schema=schema, labels=labels, drop_table_before=True
    )


@pytest.mark.parametrize(
    'client_kwargs',
    [
        dict(create_dataset_exception=ConnectionError()),
        dict(update_dataset_exception=ConnectionError()),
        dict(create_table_exception=ConnectionError()),
        dict(update_table_exception=ConnectionError()),
    ],
)
def test_create_table_nok_client_fails(monkeypatch, client_kwargs: Dict[str, Any]):
    # Given
    client = _StubClient(**client_kwargs)
    _mock_client(monkeypatch, client=client, project_id=_TEST_PROJECT_ID, location=_TEST_LOCATION)
    # When/Then
    with pytest.raises(ValueError):
        _bq_base.create_table(
            table_fqn_id=_TEST_TABLE_FQN_ID,
            schema=_TEST_SCHEMA,
            labels=_TEST_LABELS,
            drop_table_before=True,
        )


@pytest.mark.parametrize('not_found_ok', [True, False])
def test_drop_table_ok(monkeypatch, not_found_ok: bool):
    # Given
    client = _StubClient()
    _mock_client(monkeypatch, client=client, project_id=_TEST_PROJECT_ID, location=_TEST_LOCATION)
    # When
    _bq_base.drop_table(table_fqn_id=_TEST_TABLE_FQN_ID, not_found_ok=not_found_ok)


def test_list_all_tables_with_filter_ok_default_filter(monkeypatch):
    # Given
    datasets = ['dataset_a', 'dataset_b']
    tables = ['table_a', 'table_b']
    expected = set()
    for ds in datasets:
        for t in tables:
            table_id = const.BQ_TABLE_FQN_ID_SEP.join([_TEST_PROJECT_ID, ds, t])
            expected.add(f'{table_id}')
    client = _StubClient(list_datasets=datasets, list_tables=tables)
    _mock_client(monkeypatch, client=client, project_id=_TEST_PROJECT_ID, location=_TEST_LOCATION)
    # When
    result = _bq_base.list_all_tables_with_filter(project_id=_TEST_PROJECT_ID)
    # Then
    assert result
    s_result = set()
    for r in result:
        s_result.add(r)
    assert s_result == expected


def test_list_all_tables_with_filter_ok_exclude_all_filter(monkeypatch):
    # Given
    datasets = ['dataset_a', 'dataset_b']
    tables = ['table_a', 'table_b']
    expected = set()
    client = _StubClient(list_datasets=datasets, list_tables=tables)
    _mock_client(monkeypatch, client=client, project_id=_TEST_PROJECT_ID, location=_TEST_LOCATION)
    # When
    result = _bq_base.list_all_tables_with_filter(
        project_id=_TEST_PROJECT_ID, filter_fn=lambda _: False
    )
    # Then
    assert result
    s_result = set()
    for r in result:
        s_result.add(r)
    assert s_result == expected


@pytest.mark.parametrize(
    'client_kwargs',
    [
        dict(list_datasets_exception=ConnectionError()),
        dict(list_tables_exception=ConnectionError()),
    ],
)
def test_list_all_tables_with_filter_nok_client_fails(monkeypatch, client_kwargs: Dict[str, Any]):
    # Given
    datasets = ['dataset_a', 'dataset_b']
    tables = ['table_a', 'table_b']
    client = _StubClient(list_datasets=datasets, list_tables=tables, **client_kwargs)
    _mock_client(monkeypatch, client=client, project_id=_TEST_PROJECT_ID, location=_TEST_LOCATION)
    # When
    result = _bq_base.list_all_tables_with_filter(project_id=_TEST_PROJECT_ID)
    # Then
    with pytest.raises(ValueError):
        next(result)
