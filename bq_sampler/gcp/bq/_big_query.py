# vim: ai:sw=4:ts=4:sta:et:fo=croql
# pylint: disable=line-too-long
"""
Reads an object from `Cloud Big Query`_ using `Python client`_.

.. Cloud Big Query: https://cloud.google.com/bigquery/docs/reference/libraries#client-libraries-install-python
.. Python client: https://googleapis.dev/python/bigquery/latest/index.html
"""
# pylint: enable=line-too-long
import logging
from typing import Any, Callable, Dict, Generator, Mapping, Optional, Sequence, Tuple, Union

import cachetools

from google.cloud import bigquery
from google.api_core import page_iterator

from bq_sampler import const


_LOGGER = logging.getLogger(__name__)


class _SimpleTableSpec:  # pylint: disable=too-few-public-methods
    def __init__(self, table_fqn_id: str):
        self.table_fqn_id = _stripped_str_arg('table_fqn_id', table_fqn_id)
        (
            self.project_id,
            self.dataset_id,
            self.table_id,
            self.location,
        ) = self._break_down_table_fqn_id(table_fqn_id)
        self.table_id_only = const.BQ_TABLE_FQN_ID_SEP.join(
            [self.project_id, self.dataset_id, self.table_id]
        )

    _BREAK_DOWN_TABLE_FQN_ID_ERROR_MSG_TMPL: str = 'Table FQN ID does not contain a {}. Got: <{}>'

    @staticmethod
    def _break_down_table_fqn_id(table_fqn_id: str) -> Tuple[str, str, str]:
        location = None
        table_only_id = table_fqn_id
        if const.BQ_TABLE_FQN_LOCATION_SEP in table_fqn_id:
            try:
                table_only_id, location = (
                    v.strip() for v in table_fqn_id.split(const.BQ_TABLE_FQN_LOCATION_SEP)
                )
            except Exception as err:
                raise ValueError(
                    _SimpleTableSpec._BREAK_DOWN_TABLE_FQN_ID_ERROR_MSG_TMPL.format(
                        'enough tokens', table_fqn_id
                    )
                ) from err
        try:
            project_id, dataset_id, table_id = (
                v.strip() for v in table_only_id.split(const.BQ_TABLE_FQN_ID_SEP)
            )
        except Exception as err:
            raise ValueError(
                _SimpleTableSpec._BREAK_DOWN_TABLE_FQN_ID_ERROR_MSG_TMPL.format(
                    'enough tokens', table_fqn_id
                )
            ) from err
        if not project_id:
            raise ValueError(
                _SimpleTableSpec._BREAK_DOWN_TABLE_FQN_ID_ERROR_MSG_TMPL.format(
                    'project ID', table_fqn_id
                )
            )
        if not dataset_id:
            raise ValueError(
                _SimpleTableSpec._BREAK_DOWN_TABLE_FQN_ID_ERROR_MSG_TMPL.format(
                    'dataset ID', table_fqn_id
                )
            )
        if not table_id:
            raise ValueError(
                _SimpleTableSpec._BREAK_DOWN_TABLE_FQN_ID_ERROR_MSG_TMPL.format(
                    'table ID', table_fqn_id
                )
            )
        return project_id, dataset_id, table_id, location

    def __str__(self) -> str:
        result = self.table_id_only
        if self.location:
            result = f'{result}{const.BQ_TABLE_FQN_LOCATION_SEP}{self.location}'
        return result


def _stripped_str_arg(name: str, value: str, accept_none: Optional[bool] = False) -> str:
    if accept_none and value is None:
        result = None
    elif not isinstance(value, str) or not value.strip():
        raise ValueError(f'Argument <{name}> must be a non-empty string. Got: <{value}>')
    else:
        result = value.strip()
    return result


def table(*, table_fqn_id: str) -> bigquery.Table:
    """
    Query table information and returns an initialized instance of :py:class:`bigquery.Table`.

    :param table_fqn_id: in the format `<PROJECT_ID>.<DATASET_ID>.<TABLE_ID>[@<LOCATION>]`.
    :return:
    """
    # validate input
    table_spec = _SimpleTableSpec(table_fqn_id)
    # logic
    return _table(table_spec)


def _table(table_spec: _SimpleTableSpec) -> bigquery.Table:
    try:
        result = _client(table_spec.project_id, table_spec.location).get_table(
            table_spec.table_id_only
        )
    except Exception as err:  # pylint: disable=broad-except
        msg = f'Could not retrieve table object for <{table_spec}>. Error: {err}'
        _LOGGER.critical(msg)
        raise ValueError(msg) from err
    return result


@cachetools.cached(cache=cachetools.LRUCache(maxsize=5))
def _client(project_id: Optional[str] = None, location: Optional[str] = None) -> bigquery.Client:
    _LOGGER.info(
        'Creating BigQuery client for project ID <%s> and location <%s>', project_id, location
    )
    return bigquery.Client(project=project_id, location=location)


def query_job(
    *,
    query: str,
    job_config: Optional[bigquery.QueryJobConfig] = None,
    project_id: Optional[str] = None,
    location: Optional[str] = None,
) -> bigquery.job.query.QueryJob:
    """
    :py:class:`bigquery.job.query.QueryJob` are Async by nature, see `docs`_.

    :param query:
    :param job_config:
    :param location:
    :param project_id:
    :return:

    .. docs: https://googleapis.dev/python/bigquery/latest/reference.html#job
    """
    # validate input
    query = _stripped_str_arg('query', query)
    project_id = _stripped_str_arg('project_id', project_id, True)
    location = _stripped_str_arg('location', location, True)
    # logic
    result = _query_job(query, job_config, project_id, location)
    _LOGGER.info(
        'Query <%s> stats: total bytes processed %s; total bytes billed %s; slot milliseconds: %s.',
        _query_job_to_log_str(result),
        result.total_bytes_processed,
        result.total_bytes_billed,
        result.slot_millis,
    )
    return result


def _query_job(
    query: str,
    job_config: Optional[bigquery.QueryJobConfig] = None,
    project_id: Optional[str] = None,
    location: Optional[str] = None,
) -> bigquery.job.query.QueryJob:
    query_parameters = job_config.query_parameters if job_config else None
    _LOGGER.info('Issuing BigQuery query: <%s> with job_config: <%s>', query, query_parameters)
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
    return result


def _query_job_to_log_str(query_job_: bigquery.job.query.QueryJob) -> str:
    """
    Two reasons for this:
    1- f-strings do not allow '\' in the parameters;
    2- standard way to report query job's query in the logs.
    """
    return query_job_.query.replace('\n', '').strip() + f' -> {query_job_.query_parameters}'


def create_table(
    *,
    table_fqn_id: str,
    schema: Sequence[Union[bigquery.schema.SchemaField, Mapping[str, Any]]],
    labels: Optional[Dict[str, str]] = None,
    drop_table_before: Optional[bool] = True,
) -> None:
    """

    :param table_fqn_id: in the format `<PROJECT_ID>.<DATASET_ID>.<TABLE_ID>[@<LOCATION>]`.
    :param schema:
    :param labels:
    :param drop_table_before:
    :return:
    """
    # validate input
    table_spec = _SimpleTableSpec(table_fqn_id)
    labels = _validate_table_labels(labels)
    _validate_schema(schema)
    # logic
    _LOGGER.info('Creating table <%s> with labels: <%s>', table_fqn_id, labels)
    dataset = _create_dataset(table_spec, labels, exists_ok=True)
    if drop_table_before:
        drop_table(table_fqn_id=table_fqn_id, not_found_ok=True)
    _create_table(dataset, table_spec, schema, labels, exists_ok=not drop_table_before)


def _validate_table_labels(labels: Optional[Dict[str, str]] = None) -> str:
    if not isinstance(labels, dict):
        labels = const.DEFAULT_CREATE_TABLE_LABELS
    return labels


def _validate_schema(
    schema: Sequence[Union[bigquery.schema.SchemaField, Mapping[str, Any]]]
) -> None:
    if schema is None:
        raise ValueError('Table schema cannot be None')


def _create_dataset(
    table_spec: _SimpleTableSpec,
    labels: Dict[str, str],
    exists_ok: Optional[bool] = True,
) -> bigquery.Dataset:
    _LOGGER.info(
        'Creating dataset <%s.%s> with labels: <%s>',
        table_spec.project_id,
        table_spec.dataset_id,
        labels,
    )
    # Dataset obj
    try:
        result = bigquery.Dataset(
            const.BQ_TABLE_FQN_ID_SEP.join([table_spec.project_id, table_spec.dataset_id])
        )
    except Exception as err:  # pylint: disable=broad-except
        msg = (
            f'Could not create input object {bigquery.Dataset.__name__} '
            f'for project ID <{table_spec.project_id}> and dataset ID <{table_spec.dataset_id}>. '
            f'Error: {err}'
        )
        _LOGGER.critical(msg)
        raise ValueError(msg) from err
    # Create dataset
    try:
        result = _client(table_spec.project_id, table_spec.location).create_dataset(
            result, exists_ok=exists_ok
        )
    except Exception as err:  # pylint: disable=broad-except
        msg = f'Could not create dataset for <{result.dataset_id}>. Error: {err}'
        _LOGGER.critical(msg)
        raise ValueError(msg) from err
    # Add labels
    try:
        result.labels = labels
        result = _client(table_spec.project_id, table_spec.location).update_dataset(
            result, ['labels']
        )
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
    table_spec: _SimpleTableSpec,
    schema: Sequence[Union[bigquery.schema.SchemaField, Mapping[str, Any]]],
    labels: Dict[str, str],
    exists_ok: Optional[bool] = True,
) -> bigquery.Table:
    _LOGGER.info(
        'Creating table <%s> in dataset <%s> with labels: <%s>',
        table_spec.table_id,
        dataset.dataset_id,
        labels,
    )
    # Table obj
    try:
        result = dataset.table(table_spec.table_id)
    except Exception as err:  # pylint: disable=broad-except
        msg = (
            f'Could not create input object {bigquery.Table.__name__} '
            f'for dataset <{dataset.dataset_id}> and table ID <{table_spec.table_id}>. '
            f'Error: {err}'
        )
        _LOGGER.critical(msg)
        raise ValueError(msg) from err
    # Create table
    try:
        result = _client(dataset.project, table_spec.location).create_table(
            result, exists_ok=exists_ok
        )
    except Exception as err:  # pylint: disable=broad-except
        msg = f'Could not create table for <{result.table_id}>. Error: {err}'
        _LOGGER.critical(msg)
        raise ValueError(msg) from err
    # Add labels and schema
    result.labels = labels
    result.schema = schema
    try:
        result = _client(dataset.project, table_spec.location).update_table(
            result, ['labels', 'schema']
        )
    except Exception as err:  # pylint: disable=broad-except
        msg = (
            f'Could not set labels for table <{result.table_id}> '
            f'with content: <{labels}>. '
            f'Error: {err}'
        )
        _LOGGER.critical(msg)
        raise ValueError(msg) from err
    return result


def drop_table(*, table_fqn_id: str, not_found_ok: Optional[bool] = True) -> None:
    """

    :param table_fqn_id: in the format `<PROJECT_ID>.<DATASET_ID>.<TABLE_ID>[@<LOCATION>]`.
    :param not_found_ok:
    :return:
    """
    # validate input
    table_spec = _SimpleTableSpec(table_fqn_id)
    # logic
    _drop_table(bigquery.Table(table_spec.table_id_only), not_found_ok, table_spec.location)


def _drop_table(
    bq_table: bigquery.Table, not_found_ok: bool, location: Optional[str] = None
) -> None:
    _LOGGER.info('Dropping table <%s> with not_found_ok=<%s>', bq_table.table_id, not_found_ok)
    try:
        _client(bq_table.project, location).delete_table(bq_table, not_found_ok=not_found_ok)
    except Exception as err:  # pylint: disable=broad-except
        msg = f'Could not drop table <{bq_table.table_id}>. Error: {err}'
        _LOGGER.critical(msg)
        raise ValueError(msg) from err


_FALLBACK_FILTER_FN: Callable[[bigquery.table.TableListItem], bool] = lambda _: True


def list_all_tables_with_filter(
    *,
    project_id: str,
    location: Optional[str] = None,
    filter_fn: Optional[Callable[[bigquery.table.TableListItem], bool]] = None,
) -> Generator[str, None, None]:
    """

    :param project_id:
    :param location:
    :param filter_fn:
    :return:
    """
    # validate input
    project_id = _stripped_str_arg('project_id', project_id, True)
    location = _stripped_str_arg('location', location, True)
    if not callable(filter_fn):
        filter_fn = _FALLBACK_FILTER_FN
    # logic
    for table_fqn_id in _list_all_tables_with_filter(project_id, location, filter_fn):
        yield table_fqn_id


def _list_all_tables_with_filter(
    project_id: str,
    location: Optional[str] = None,
    filter_fn: Optional[Callable[[bigquery.table.TableListItem], bool]] = None,
) -> Generator[str, None, None]:
    _LOGGER.info('Listing all tables in project <%s> with filter function', project_id)
    try:
        for ds_list_item in _list_all_datasets(project_id, location):
            for t_list_item in _list_all_tables_in_dataset(ds_list_item, location):
                if filter_fn(t_list_item):
                    table_fqn_id = const.BQ_TABLE_FQN_ID_SEP.join(
                        [project_id, ds_list_item.dataset_id, t_list_item.table_id]
                    )
                    if location:
                        table_fqn_id = f'{table_fqn_id}{const.BQ_TABLE_FQN_LOCATION_SEP}{location}'
                    yield table_fqn_id
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


def cross_region_dataset_copy() -> None:
    """
    Equivalent to::
        bq mk --transfer_config \
            --data_source=cross_region_copy \
            --project_id=<TARGET_PROJECT_ID> \
            --target_dataset=<TARGET_DATASET_ID> \
            --display_name=<TARGET_DATASET_ID> \
            --params='{
                "source_project_id":"<SOURCE_PROJECT_ID>",
                "source_dataset_id":"<SOURCE_DATASET_ID>",
                "overwrite_destination_table":"true"
            }'
    :return:
    """
    raise NotImplementedError('Missing use case')
