# vim: ai:sw=4:ts=4:sta:et:fo=croql
# pylint: disable=line-too-long
"""
Reads an object from `Cloud Big Query`_ using `Python client`_.

.. Cloud Big Query: https://cloud.google.com/bigquery/docs/reference/libraries#client-libraries-install-python
.. Python client: https://googleapis.dev/python/bigquery/latest/index.html
"""
# pylint: enable=line-too-long
import logging
from typing import Any, Dict, Generator, Mapping, Optional, Sequence, Tuple, Union

import cachetools

from google.cloud import bigquery
from google.api_core import page_iterator


_LOGGER = logging.getLogger(__name__)

_BQ_ID_SEP: str = '.'

DEFAULT_CREATE_TABLE_LABELS: Dict[str, str] = {
    'sample_table': 'true',
}
"""
Default GCP resource label to be applied table created here.
"""


@cachetools.cached(cache=cachetools.LRUCache(maxsize=100_000))
def get_table_row_count(table_fqn_id: str, location: Optional[str] = None) -> int:
    """
    Compute table size (in rows) for the argument.

    **NOTE**: This call is cached,
        since it is assumed that during a session the row count
        will not dramatically change.

    :param table_fqn_id: in the format <PROJECT_ID>.<DATASET_ID>.<TABLE_ID>
    :param location:
    :return:
    """
    return _get_table_size(
        _validate_table_fqn_id(table_fqn_id), _get_stripped_str_arg('location', location, True)
    )


def _validate_table_fqn_id(value: str) -> str:
    result = _get_stripped_str_arg('table_fqn_id', value)
    _break_down_table_fqn_id(result)
    return result


def _get_stripped_str_arg(name: str, value: str, accept_none: Optional[bool] = False) -> str:
    if accept_none and value is None:
        result = None
    elif not isinstance(value, str) or not value.strip():
        raise ValueError(f'Argument <{name}> must be a non-empty string. Got: <{value}>')
    else:
        result = value.strip()
    return result


_BREAK_DOWN_TABLE_FQN_ID_ERROR_MSG_TMPL: str = 'Table FQN ID does not contain a {}. Got: <{}>'


def _break_down_table_fqn_id(table_fqn_id: str) -> Tuple[str, str, str]:
    try:
        project_id, dataset_id, table_id = (v.strip() for v in table_fqn_id.split('.'))
    except Exception as err:
        raise ValueError(
            _BREAK_DOWN_TABLE_FQN_ID_ERROR_MSG_TMPL.format('enough tokens', table_fqn_id)
        ) from err
    if not project_id:
        raise ValueError(_BREAK_DOWN_TABLE_FQN_ID_ERROR_MSG_TMPL.format('project ID', table_fqn_id))
    if not dataset_id:
        raise ValueError(_BREAK_DOWN_TABLE_FQN_ID_ERROR_MSG_TMPL.format('dataset ID', table_fqn_id))
    if not table_id:
        raise ValueError(_BREAK_DOWN_TABLE_FQN_ID_ERROR_MSG_TMPL.format('table ID', table_fqn_id))
    return project_id, dataset_id, table_id


def _get_table_size(table_fqn_id: str, location: Optional[str] = None) -> int:
    _LOGGER.info('Reading table size from :<%s>', table_fqn_id)
    table = _get_table(table_fqn_id, location)
    return table.num_rows


def _get_table(table_fqn_id: str, location: Optional[str] = None) -> bigquery.table.Table:
    project_id, _, _ = _break_down_table_fqn_id(table_fqn_id)
    try:
        result = _client(project_id, location).get_table(table_fqn_id)
    except Exception as err:  # pylint: disable=broad-except
        msg = f'Could not retrieve table object for <{table_fqn_id}>. Error: {err}'
        _LOGGER.critical(msg)
        raise ValueError(msg) from err
    return result


@cachetools.cached(cache=cachetools.LRUCache(maxsize=5))
def _client(project_id: Optional[str] = None, location: Optional[str] = None) -> bigquery.Client:
    _LOGGER.info(
        'Creating BigQuery client for project ID <%s> and location <%s>', project_id, location
    )
    return bigquery.Client(project=project_id, location=location)


def query_job_result(
    query: str,
    job_config: Optional[bigquery.QueryJobConfig] = None,
    project_id: Optional[str] = None,
    location: Optional[str] = None,
) -> bigquery.table.RowIterator:
    """

    :param query:
    :param job_config:
    :return:
    """
    job = query_job(query, job_config, project_id, location)
    try:
        result = job.result()
    except Exception as err:  # pylint: disable=broad-except
        msg = f'Could not retrieve results from query <{_query_job_to_log_str(job)}>. Error: {err}'
        _LOGGER.critical(msg)
        raise RuntimeError(msg) from err
    return result


def query_job(
    query: str,
    job_config: Optional[bigquery.QueryJobConfig] = None,
    project_id: Optional[str] = None,
    location: Optional[str] = None,
) -> bigquery.job.query.QueryJob:
    """
    :py:class:`bigquery.job.query.QueryJob` are Async by nature, see `docs`_.

    :param query:
    :param job_config:
    :return:

    .. docs: https://googleapis.dev/python/bigquery/latest/reference.html#job
    """
    query = _get_stripped_str_arg('query', query)
    query_parameters = job_config.query_parameters if job_config else None
    _LOGGER.info('Issuing BigQuery query: <%s> with job_config: <%s>', query, query_parameters)
    project_id = _get_stripped_str_arg('project_id', project_id, True)
    location = _get_stripped_str_arg('location', location, True)
    try:
        result = _client(project_id, location).query(query, job_config=job_config)
    except Exception as err:  # pylint: disable=broad-except
        msg = (
            f'Could not issue BigQuery query: <{query}> '
            f'and job config: <{query_parameters}>. '
            f'Error: {err}'
        )
        _LOGGER.critical(msg)
        raise RuntimeError(msg) from err
    _LOGGER.info(
        'Query <%s> stats: total bytes processed %s; total bytes billed %s; slot milliseconds: %s.',
        _query_job_to_log_str(result),
        result.total_bytes_processed,
        result.total_bytes_billed,
        result.slot_millis,
    )
    return result


def _query_job_to_log_str(query_job_: bigquery.job.query.QueryJob) -> str:
    """
    Two reasons for this:
    1- f-strings do not allow '\' in the parameters;
    2- standard way to report query job's query in the logs.
    """
    return query_job_.query.replace('\n', '').strip() + f' -> {query_job_.query_parameters}'


def get_table(table_fqn_id: str, location: Optional[str] = None) -> bigquery.Table:
    """

    :param table_fqn_id:
    :return:
    """
    # validate input
    table_fqn_id = _validate_table_fqn_id(table_fqn_id)
    # logic
    return _get_table(table_fqn_id, location)


def create_table(
    table_fqn_id: str,
    schema: Sequence[Union[bigquery.schema.SchemaField, Mapping[str, Any]]],
    labels: Optional[Dict[str, str]] = None,
    drop_table_before: Optional[bool] = True,
    location: Optional[str] = None,
) -> None:
    """

    :param table_fqn_id:
    :param schema:
    :param labels:
    :param drop_table_before:
    :param region:
    :return:
    """
    # validate input
    table_fqn_id = _validate_table_fqn_id(table_fqn_id)
    labels = _validate_table_labels(labels)
    # logic
    _LOGGER.info('Creating table <%s> with labels: <%s>', table_fqn_id, labels)
    project_id, dataset_id, table_id = _break_down_table_fqn_id(table_fqn_id)
    dataset = _create_dataset(project_id, dataset_id, labels, location)
    if drop_table_before:
        drop_table(table_fqn_id)
    _create_table(dataset, table_id, schema, labels, location)


def _validate_table_labels(labels: Optional[Dict[str, str]] = None) -> str:
    if not isinstance(labels, dict):
        labels = DEFAULT_CREATE_TABLE_LABELS
    return labels


def _validate_schema(
    schema: Sequence[Union[bigquery.schema.SchemaField, Mapping[str, Any]]]
) -> None:
    if schema is None:
        raise ValueError('Table schema cannot be None')


def _create_dataset(
    project_id: str,
    dataset_id: str,
    labels: Dict[str, str],
    exists_ok: Optional[bool] = True,
    location: Optional[str] = None,
) -> bigquery.Dataset:
    _LOGGER.info('Creating dataset <%s.%s> with labels: <%s>', project_id, dataset_id, labels)
    # Dataset obj
    try:
        result = bigquery.Dataset(_BQ_ID_SEP.join([project_id, dataset_id]))
        result.labels = labels
    except Exception as err:  # pylint: disable=broad-except
        msg = (
            f'Could not create input object {bigquery.Dataset.__name__} '
            f'for project ID <{project_id}> and dataset ID <{dataset_id}>. '
            f'Error: {err}'
        )
        _LOGGER.critical(msg)
        raise ValueError(msg) from err
    # Create dataset
    try:
        result = _client(project_id, location).create_dataset(result, exists_ok=exists_ok)
    except Exception as err:  # pylint: disable=broad-except
        msg = f'Could not create dataset for <{result.dataset_id}>. Error: {err}'
        _LOGGER.critical(msg)
        raise ValueError(msg) from err
    # Add labels
    try:
        result.labels = labels
        result = _client(project_id, location).update_dataset(result, ['labels'])
    except Exception as err:  # pylint: disable=broad-except
        msg = (
            f'Could not set labels for dataset <{result.dataset_id}> '
            f'with content: <{labels}>. '
            f'Error: {err}'
        )
        _LOGGER.critical(msg)
        raise ValueError(msg) from err
    return result


def _create_table(  # pylint: disable=too-many-arguments
    dataset: bigquery.Dataset,
    table_id: str,
    schema: Sequence[Union[bigquery.schema.SchemaField, Mapping[str, Any]]],
    labels: Dict[str, str],
    exists_ok: Optional[bool] = True,
    location: Optional[str] = None,
) -> bigquery.Table:
    _LOGGER.info(
        'Creating table <%s> in dataset <%s> with labels: <%s>',
        table_id,
        dataset.dataset_id,
        labels,
    )
    # Table obj
    try:
        result = dataset.table(table_id)
    except Exception as err:  # pylint: disable=broad-except
        msg = (
            f'Could not create input object {bigquery.Table.__name__} '
            f'for dataset <{dataset.dataset_id}> and table ID <{table_id}>. '
            f'Error: {err}'
        )
        _LOGGER.critical(msg)
        raise ValueError(msg) from err
    # Create table
    try:
        result = _client(dataset.project, location).create_table(result, exists_ok=exists_ok)
    except Exception as err:  # pylint: disable=broad-except
        msg = f'Could not create table for <{result.table_id}>. Error: {err}'
        _LOGGER.critical(msg)
        raise ValueError(msg) from err
    # Add labels and schema
    result.labels = labels
    result.schema = schema
    try:
        result = _client(dataset.project, location).update_table(result, ['labels', 'schema'])
    except Exception as err:  # pylint: disable=broad-except
        msg = (
            f'Could not set labels for table <{result.table_id}> '
            f'with content: <{labels}>. '
            f'Error: {err}'
        )
        _LOGGER.critical(msg)
        raise ValueError(msg) from err
    return result


def drop_table(
    table_fqn_id: str, location: Optional[str] = None, not_found_ok: Optional[bool] = True
) -> None:
    """

    :param table_fqn_id:
    :param not_found_ok:
    :return:
    """
    table_fqn_id = _validate_table_fqn_id(table_fqn_id)
    _LOGGER.info('Dropping table <%s> with not_found_ok=<%s>', table_fqn_id, not_found_ok)
    _drop_table(bigquery.Table(table_fqn_id), not_found_ok, location)


def _drop_table(table: bigquery.Table, not_found_ok: bool, location: Optional[str] = None) -> None:
    _LOGGER.info('Dropping table <%s> with not_found_ok=<%s>', table.table_id, not_found_ok)
    try:
        _client(table.project, location).delete_table(table, not_found_ok=not_found_ok)
    except Exception as err:  # pylint: disable=broad-except
        msg = f'Could not drop table <{table.table_id}>. Error: {err}'
        _LOGGER.critical(msg)
        raise ValueError(msg) from err


def drop_all_tables_by_labels(
    project_id: str, location: Optional[str] = None, labels: Optional[Dict[str, str]] = None
) -> None:
    """

    :param project_id:
    :param labels:
    :return:
    """
    project_id = _get_stripped_str_arg('project_id', project_id)
    location = _get_stripped_str_arg('location', location, True)
    labels = _validate_table_labels(labels)
    _LOGGER.info('Dropping all tables in project <%s> with labels <%s>', project_id, labels)
    _drop_all_tables_in_iter(_list_all_tables_with_labels(project_id, labels, location), location)


def _list_all_tables_with_labels(
    project_id: str, labels: Dict[str, str], location: Optional[str] = None
) -> Generator[str, None, None]:
    _LOGGER.info('Listing all tables in project <%s> with labels <%s>', project_id, labels)
    try:
        for ds_list_item in _list_all_datasets(project_id, location):
            for t_list_item in _list_all_tables_in_dataset(ds_list_item, location):
                if _has_table_labels(t_list_item, labels):
                    yield f'{project_id}.{ds_list_item.dataset_id}.{t_list_item.table_id}'
    except Exception as err:  # pylint: disable=broad-except
        msg = f'Could not list all datasets in project <{project_id}>. Error: {err}'
        _LOGGER.critical(msg)
        raise ValueError(msg) from err


def _list_all_datasets(project_id: str, location: Optional[str] = None) -> page_iterator.Iterator:
    _LOGGER.info('Listing all datasets in project <%s>', project_id)
    try:
        result = _client(project_id, location).list_datasets(project=project_id, include_all=True)
    except Exception as err:  # pylint: disable=broad-except
        msg = f'Could not list all datasets in project <{project_id}>. Error: {err}'
        _LOGGER.critical(msg)
        raise ValueError(msg) from err
    return result


def _list_all_tables_in_dataset(
    dataset_list_item: bigquery.dataset.DatasetListItem, location: Optional[str] = None
) -> page_iterator.Iterator:
    _LOGGER.info('Listing all tables in dataset <%s>', dataset_list_item.dataset_id)
    try:
        result = _client(dataset_list_item.project, location).list_tables(dataset_list_item)
    except Exception as err:  # pylint: disable=broad-except
        msg = f'Could not list all tables in dataset <{dataset_list_item.dataset_id}>. Error: {err}'
        _LOGGER.critical(msg)
        raise ValueError(msg) from err
    return result


def _has_table_labels(
    table_list_item: bigquery.table.TableListItem, labels: Dict[str, str]
) -> bool:
    result = True
    for key, val in labels.items():
        result = key in table_list_item.labels and val == table_list_item.labels.get(key)
        if not result:
            break
    return result


def _drop_all_tables_in_iter(
    tables_to_drop_gen: Generator[str, None, None], location: Optional[str] = None
) -> None:
    error_msgs = []
    last_error = None
    for table_fqn_id in tables_to_drop_gen:
        try:
            drop_table(table_fqn_id, location)
        except Exception as err:  # pylint: disable=broad-except
            msg = f'Cloud not drop table <{table_fqn_id}>. Error: {err}'
            _LOGGER.critical(msg)
            error_msgs.append(msg)
            last_error = err
    if last_error is not None:
        raise RuntimeError('+++'.join(error_msgs)) from last_error
