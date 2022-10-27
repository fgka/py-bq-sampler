# vim: ai:sw=4:ts=4:sta:et:fo=croql
# pylint: disable=line-too-long
"""
Reads an object from `Cloud Big Query`_ using `Python client`_.

.. Cloud Big Query: https://cloud.google.com/bigquery/docs/reference/libraries#client-libraries-install-python
.. Python client: https://googleapis.dev/python/bigquery/latest/index.html
"""
# pylint: enable=line-too-long
from typing import Any, Callable, Dict, Generator, Mapping, Optional, Sequence, Tuple, Union

import cachetools

from google.api_core import page_iterator
from google import auth
from google.cloud import bigquery, bigquery_datatransfer
from google.protobuf import struct_pb2, timestamp_pb2

from bq_sampler import const, logger

_LOGGER = logger.get(__name__)


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
                        'enough tokens including location', table_fqn_id
                    )
                ) from err
        try:
            project_id, dataset_id, table_id = (
                v.strip() for v in table_only_id.split(const.BQ_TABLE_FQN_ID_SEP)
            )
        except Exception as err:
            raise ValueError(
                _SimpleTableSpec._BREAK_DOWN_TABLE_FQN_ID_ERROR_MSG_TMPL.format(
                    'enough tokens without location', table_fqn_id
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
    _LOGGER.debug('Retrieving table metadata for <%s>', table_fqn_id)
    table_spec = _SimpleTableSpec(table_fqn_id)
    # logic
    result = _table(table_spec)
    _LOGGER.debug('Retrieved table metadata for <%s> = %s', table_fqn_id, result)
    return result


def _table(table_spec: _SimpleTableSpec) -> bigquery.Table:
    try:
        result = _client(table_spec.project_id, table_spec.location).get_table(
            table_spec.table_id_only
        )
    except Exception as err:  # pylint: disable=broad-except
        raise ValueError(
            f'Could not retrieve table object for <{table_spec}>. Error: {err}'
        ) from err
    return result


@cachetools.cached(cache=cachetools.LRUCache(maxsize=5))
def _client(project_id: Optional[str] = None, location: Optional[str] = None) -> bigquery.Client:
    _LOGGER.debug(
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
    _LOGGER.debug(
        'Issuing query job for query <%s> in project <%s>@<%s>', query, project_id, location
    )
    query = _stripped_str_arg('query', query)
    project_id = _stripped_str_arg('project_id', project_id, True)
    location = _stripped_str_arg('location', location, True)
    # logic
    result = _query_job(query, job_config, project_id, location)
    _LOGGER.debug(
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
    _LOGGER.debug('Issuing BigQuery query: <%s> with job_config: <%s>', query, query_parameters)
    try:
        result = _client(project_id, location).query(query, job_config=job_config)
    except Exception as err:  # pylint: disable=broad-except
        raise RuntimeError(
            f'Could not issue BigQuery query: <{query}> '
            f'and job config: <{query_parameters}>. '
            f'Error: {err}'
        ) from err
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
    Using the full-qualified ID creates a table.

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
    _LOGGER.debug('Creating table <%s> with labels: <%s>', table_fqn_id, labels)
    dataset = _create_dataset(table_spec, labels, exists_ok=True)
    if drop_table_before:
        drop_table(table_fqn_id=table_fqn_id, not_found_ok=True)
    _create_table(dataset, table_spec, schema, labels, exists_ok=True)
    _LOGGER.debug('Created table <%s> with labels: <%s>', table_fqn_id, labels)


def _validate_table_labels(value: Optional[Dict[str, str]] = None) -> str:
    if not isinstance(value, dict):
        value = const.DEFAULT_CREATE_TABLE_LABELS
    else:
        value = {**value, **const.DEFAULT_CREATE_TABLE_LABELS}
    return value


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
    _LOGGER.debug(
        'Creating dataset <%s.%s>@<%s> with labels: <%s>',
        table_spec.project_id,
        table_spec.dataset_id,
        table_spec.location,
        labels,
    )
    # Dataset obj
    try:
        result = bigquery.Dataset(
            const.BQ_TABLE_FQN_ID_SEP.join([table_spec.project_id, table_spec.dataset_id])
        )
    except Exception as err:  # pylint: disable=broad-except
        raise ValueError(
            f'Could not create input object {bigquery.Dataset.__name__} '
            f'for project ID <{table_spec.project_id}> and dataset ID <{table_spec.dataset_id}>. '
            f'Error: {err}'
        ) from err
    # Create dataset
    try:
        result = _client(table_spec.project_id, table_spec.location).create_dataset(
            result, exists_ok=exists_ok
        )
    except Exception as err:  # pylint: disable=broad-except
        raise ValueError(
            f'Could not create dataset for <{result.dataset_id}>. Error: {err}'
        ) from err
    # Add labels
    try:
        result.labels = labels
        result = _client(table_spec.project_id, table_spec.location).update_dataset(
            result, ['labels']
        )
    except Exception as err:  # pylint: disable=broad-except
        raise ValueError(
            f'Could not set labels for dataset <{result.dataset_id}> '
            f'with content: <{labels}>. '
            f'Error: {err}'
        ) from err
    return result


def _create_table(  # pylint: disable=too-many-arguments
    dataset: bigquery.Dataset,
    table_spec: _SimpleTableSpec,
    schema: Sequence[Union[bigquery.schema.SchemaField, Mapping[str, Any]]],
    labels: Dict[str, str],
    exists_ok: Optional[bool] = True,
) -> bigquery.Table:
    _LOGGER.debug(
        'Creating table <%s> in dataset <%s> and project <%s> exists_ok <%s> with labels: <%s>',
        table_spec.table_id,
        dataset.dataset_id,
        dataset.project,
        exists_ok,
        labels,
    )
    # Table obj
    try:
        result = dataset.table(table_spec.table_id)
    except Exception as err:  # pylint: disable=broad-except
        raise ValueError(
            f'Could not create input object {bigquery.Table.__name__} '
            f'for dataset <{dataset.dataset_id}> and table ID <{table_spec.table_id}>. '
            f'Error: {err}'
        ) from err
    # Create table
    try:
        result = _client(dataset.project, table_spec.location).create_table(
            result, exists_ok=exists_ok
        )
    except Exception as err:  # pylint: disable=broad-except
        raise ValueError(f'Could not create table for <{result.table_id}>. Error: {err}') from err
    # Add labels and schema
    result.labels = labels
    result.schema = schema
    try:
        result = _client(dataset.project, table_spec.location).update_table(
            result, ['labels', 'schema']
        )
    except Exception as err:  # pylint: disable=broad-except
        raise ValueError(
            f'Could not set labels for table <{result.table_id}> '
            f'with content: <{labels}>. '
            f'Error: {err}'
        ) from err
    return result


def drop_table(*, table_fqn_id: str, not_found_ok: Optional[bool] = True) -> None:
    """
    Will drop the specified table.

    :param table_fqn_id: in the format `<PROJECT_ID>.<DATASET_ID>.<TABLE_ID>[@<LOCATION>]`.
    :param not_found_ok:
    :return:
    """
    # validate input
    _LOGGER.debug('Dropping table <%s> with not found ok <%s>', table_fqn_id, not_found_ok)
    table_spec = _SimpleTableSpec(table_fqn_id)
    # logic
    _drop_table(bigquery.Table(table_spec.table_id_only), not_found_ok, table_spec.location)
    _LOGGER.debug('Dropped table <%s> with not found ok <%s>', table_fqn_id, not_found_ok)


def _drop_table(
    bq_table: bigquery.Table, not_found_ok: bool, location: Optional[str] = None
) -> None:
    tbl_full_id = f'{bq_table.project}:{bq_table.dataset_id}.{bq_table.table_id}'
    _LOGGER.debug('Dropping table <%s> with not_found_ok=<%s>', tbl_full_id, not_found_ok)
    try:
        _client(bq_table.project, location).delete_table(bq_table, not_found_ok=not_found_ok)
        _LOGGER.info('Dropped table <%s> with not_found_ok=<%s>', tbl_full_id, not_found_ok)
    except Exception as err:  # pylint: disable=broad-except
        raise ValueError(f'Could not drop table <{tbl_full_id}>. Error: {err}') from err


_FALLBACK_FILTER_FN: Callable[[Any], bool] = lambda _: True


def list_all_tables_with_filter(
    *,
    project_id: str,
    filter_fn: Optional[Callable[[bigquery.table.TableListItem], bool]] = None,
) -> Generator[str, None, None]:
    """
    Lists all tables matching the label criteria, if given.
    Notice that it actually go over all datasets and tables and
        filters out after the API has been called.

    :param project_id:
    :param filter_fn:
    :return:
    """
    # validate input
    _LOGGER.debug('Listing tables with filter in project <%s>', project_id)
    project_id = _stripped_str_arg('project_id', project_id, True)
    if not callable(filter_fn):
        filter_fn = _FALLBACK_FILTER_FN
    # logic
    for table_fqn_id in _list_all_tables_with_filter(project_id, filter_fn):
        yield table_fqn_id


def _list_all_tables_with_filter(
    project_id: str,
    filter_fn: Callable[[bigquery.table.TableListItem], bool],
) -> Generator[str, None, None]:
    _LOGGER.debug('Listing all tables in project <%s> with filter function', project_id)
    try:
        for ds_list_item in _list_all_datasets(project_id):
            for t_list_item in _list_all_tables_in_dataset(ds_list_item):
                if filter_fn(t_list_item):
                    table_fqn_id = const.BQ_TABLE_FQN_ID_SEP.join(
                        [project_id, ds_list_item.dataset_id, t_list_item.table_id]
                    )
                    table_location = _extract_location_from_ds_list_item(ds_list_item)
                    if table_location:
                        table_fqn_id = (
                            f'{table_fqn_id}{const.BQ_TABLE_FQN_LOCATION_SEP}{table_location}'
                        )
                    yield table_fqn_id
    except Exception as err:  # pylint: disable=broad-except
        raise ValueError(
            f'Could not list all datasets in project <{project_id}>. Error: {err}'
        ) from err


def _list_all_datasets(project_id: str) -> page_iterator.Iterator:
    # pylint: disable=line-too-long
    """
    Page iterator over :py:class:`bigquery.dataset.DatasetListItem` (`documentation`_)

    .. _documentation: https://cloud.google.com/python/docs/reference/bigquery/latest/google.cloud.bigquery.dataset.DatasetListItem
    """
    # pylint: enable=line-too-long
    _LOGGER.debug('Listing all datasets in project <%s>', project_id)
    try:
        result = _client(project_id).list_datasets(project=project_id, include_all=True)
    except Exception as err:  # pylint: disable=broad-except
        raise ValueError(
            f'Could not list all datasets in project <{project_id}>. Error: {err}'
        ) from err
    return result


_DATASET_LIST_ITEM_PROPERTIES_ATTR: str = '_properties'
_DATASET_LIST_ITEM_PROPERTIES_LOCATION_ATTR: str = 'location'


def _extract_location_from_ds_list_item(value: bigquery.dataset.DatasetListItem) -> str:
    """
    This is based on inspecting the object and confirming with the raw API `datasets.list`_.

    .. _datasets.list: https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets/list
    """
    result = None
    if hasattr(value, _DATASET_LIST_ITEM_PROPERTIES_ATTR):
        props = getattr(value, _DATASET_LIST_ITEM_PROPERTIES_ATTR)
        if isinstance(props, dict) and props.get(_DATASET_LIST_ITEM_PROPERTIES_LOCATION_ATTR):
            result = props.get(_DATASET_LIST_ITEM_PROPERTIES_LOCATION_ATTR)
    return result


def _list_all_tables_in_dataset(
    dataset_list_item: Union[bigquery.DatasetReference, bigquery.dataset.DatasetListItem],
) -> page_iterator.Iterator:
    # pylint: disable=line-too-long
    """
    Page iterator over :py:class:`bigquery.table.TableListItem` (`documentation`_)

    .. _documentation: https://cloud.google.com/python/docs/reference/bigquery/latest/google.cloud.bigquery.table.TableListItem
    """
    # pylint: enable=line-too-long
    _LOGGER.debug('Listing all tables in dataset <%s>', dataset_list_item.dataset_id)
    try:
        result = _client(dataset_list_item.project).list_tables(dataset_list_item)
    except Exception as err:  # pylint: disable=broad-except
        raise ValueError(
            f'Could not list all tables in dataset <{dataset_list_item.dataset_id}>. Error: {err}'
        ) from err
    return result


def get_dataset(project_id: str, dataset_id: str) -> bigquery.Dataset:
    """
    Retrieves the full py:class:`bigquery.Dataset` object.

    :param project_id:
    :param dataset_id:
    :return:
    """
    # validate input
    project_id = _stripped_str_arg('project_id', project_id)
    dataset_id = _stripped_str_arg('dataset_id', dataset_id)
    # logic
    try:
        result = _client().get_dataset(
            bigquery.DatasetReference(project=project_id, dataset_id=dataset_id)
        )
    except Exception as err:
        raise RuntimeError(
            f'Could not retrieve dataset {dataset_id} in project {project_id}. Error: {err}'
        ) from err
    return result


def remove_empty_datasets(
    *,
    project_id: str,
    filter_fn: Optional[Callable[[bigquery.dataset.DatasetListItem], bool]] = None,
) -> None:
    """
    Removes an empty dataset according to filtering function `filter_fn`, if present.

    :param project_id:
    :param filter_fn:
    :return:
    """
    _LOGGER.debug('Removing empty datasets with filter in project <%s>', project_id)
    # validation
    project_id = _stripped_str_arg('project_id', project_id, True)
    if not callable(filter_fn):
        filter_fn = _FALLBACK_FILTER_FN
    # logic
    errors = []
    for dataset_item in _list_all_datasets(project_id):
        if filter_fn(dataset_item):
            try:
                remove_dataset(
                    project_id=dataset_item.project,
                    dataset_id=dataset_item.dataset_id,
                    only_if_empty=True,
                )
            except Exception as err:  # pylint: disable=broad-except
                msg = f'Could remove {dataset_item.full_dataset_id}. Error: {err}'
                _LOGGER.error(msg)
                errors.append(err)
    if errors:
        raise RuntimeError(f'Could not remove dataset(s). Errors: {errors}') from errors[-1]


def remove_dataset(
    *,
    project_id: str,
    dataset_id: str,
    not_found_ok: Optional[bool] = True,
    only_if_empty: Optional[bool] = False,
) -> None:
    """
    Removes a dataset.

    :param project_id:
    :param dataset_id:
    :param not_found_ok:
    :param only_if_empty:
    :return:
    """
    _LOGGER.debug('Removing dataset <%s> in project <%s>', dataset_id, project_id)
    # validate input
    project_id = _stripped_str_arg('project_id', project_id)
    dataset_id = _stripped_str_arg('dataset_id', dataset_id)
    # logic
    to_remove = not only_if_empty
    if only_if_empty:
        dataset_ref = bigquery.DatasetReference(project=project_id, dataset_id=dataset_id)
        page_iter = _list_all_tables_in_dataset(dataset_ref)
        if page_iter and page_iter.num_results == 0:
            to_remove = True
    if to_remove:
        _remove_dataset(project_id=project_id, dataset_id=dataset_id, not_found_ok=not_found_ok)
        _LOGGER.debug('Removed dataset <%s> in project <%s>', dataset_id, project_id)


def _remove_dataset(
    *, project_id: str, dataset_id: str, not_found_ok: Optional[bool] = True
) -> None:
    _LOGGER.debug(
        'Removing dataset <%s> in project <%s> with not_found_ok <%s>',
        dataset_id,
        project_id,
        not_found_ok,
    )
    client = _client(project_id)
    try:
        client.delete_dataset(
            dataset=bigquery.DatasetReference(project=project_id, dataset_id=dataset_id),
            not_found_ok=not_found_ok,
        )
        _LOGGER.info(
            'Removed dataset <%s> in project <%s> with not_found_ok <%s>',
            dataset_id,
            project_id,
            not_found_ok,
        )
    except Exception as err:  # pylint: disable=broad-except
        raise RuntimeError(
            f'Could not remove dataset {dataset_id} in project {project_id}. ' f'Error: {err}'
        ) from err


def cross_location_dataset_copy(
    *,
    source_table_fqn_id: str,
    target_table_fqn_id: str,
    notification_pubsub_topic: Optional[str] = None,
    bq_transfer_sa: Optional[str] = None,
) -> Sequence[bigquery_datatransfer.TransferRun]:
    # pylint: disable=line-too-long
    """
    This is wrapper around `DataTransferServiceClient`_ methods:
    * `create_transfer_config`_;
    * `start_manual_transfer_runs`_.

    :param source_table_fqn_id:
    :param target_table_fqn_id:
    :param notification_pubsub_topic:
    :param bq_transfer_sa:
    :return:

    .. DataTransferServiceClient: https://cloud.google.com/python/docs/reference/bigquerydatatransfer/latest/google.cloud.bigquery_datatransfer_v1.services.data_transfer_service.DataTransferServiceClient
    .. create_transfer_config: https://cloud.google.com/python/docs/reference/bigquerydatatransfer/latest/google.cloud.bigquery_datatransfer_v1.services.data_transfer_service.DataTransferServiceClient#google_cloud_bigquery_datatransfer_v1_services_data_transfer_service_DataTransferServiceClient_create_transfer_config
    .. start_manual_transfer_runs: https://cloud.google.com/python/docs/reference/bigquerydatatransfer/latest/google.cloud.bigquery_datatransfer_v1.services.data_transfer_service.DataTransferServiceClient#google_cloud_bigquery_datatransfer_v1_services_data_transfer_service_DataTransferServiceClient_start_manual_transfer_runs
    """
    # pylint: enable=line-too-long
    _LOGGER.warning(
        'Cross location copy is expensive, consider relocating source table. '
        'Copying from <%s> into <%s> with done notification sent to <%s>',
        source_table_fqn_id,
        target_table_fqn_id,
        notification_pubsub_topic,
    )
    src_tbl = _SimpleTableSpec(source_table_fqn_id)
    tgt_tbl = _SimpleTableSpec(target_table_fqn_id)
    notification_pubsub_topic = _stripped_str_arg(
        'pubsub_transfer_done_topic', notification_pubsub_topic, True
    )
    # create transfer config
    client = _data_transfer_client(tgt_tbl.project_id)
    transfer_config_request = _create_transfer_config_request(
        source_table=src_tbl,
        target_table=tgt_tbl,
        notification_pubsub_topic=notification_pubsub_topic,
        bq_transfer_sa=bq_transfer_sa,
    )
    try:
        create_response: bigquery_datatransfer.TransferConfig = client.create_transfer_config(
            request=transfer_config_request,
        )
    except Exception as err:  # pylint: disable=broad-except
        str_transfer_config_request = str(transfer_config_request).replace('\n', '\\n')
        raise RuntimeError(
            f'Could not create transfer config with request <{str_transfer_config_request}> '
            f'Error: {err}'
        ) from err
    _LOGGER.info(
        'Created BigQuery %s from <%s> to <%s>. Transfer name: %s',
        bigquery_datatransfer.TransferConfig.__name__,
        src_tbl,
        tgt_tbl,
        create_response.name,
    )
    # manually trigger the transfer run
    transfer_run_request = _create_transfer_run_request(name=create_response.name)
    try:
        run_response: bigquery_datatransfer.StartManualTransferRunsResponse = (
            client.start_manual_transfer_runs(request=transfer_run_request)
        )
    except Exception as err:  # pylint: disable=broad-except
        raise RuntimeError(
            f'Could not manually start run {create_response.name} '
            f'with request <{transfer_run_request}> '
            f'Error: {err}'
        ) from err
    result: Sequence[bigquery_datatransfer.TransferRun] = run_response.runs
    _LOGGER.info(
        'Triggered %s: %s with runs: [%s]',
        bigquery_datatransfer.TransferConfig.__name__,
        create_response.name,
        ', '.join([run.name for run in result]),
    )
    # return the runs
    return result


_DATA_TRANSFER_DATA_SOURCE_ID: str = 'cross_region_copy'
_DATA_TRANSFER_AUTH_SCOPES: Sequence[str] = [
    'https://www.googleapis.com/auth/cloud-platform',
    'https://www.googleapis.com/auth/bigquery',
]


@cachetools.cached(cache=cachetools.LRUCache(maxsize=5))
def _data_transfer_client(
    project_id: Optional[str] = None,
) -> bigquery_datatransfer.DataTransferServiceClient:
    # pylint: disable=line-too-long
    """
    Embed project into `client`_ object
        py:class:`bigquery_datatransfer.DataTransferServiceClient` auth
        using `auth.default`_.

    .. client: https://cloud.google.com/python/docs/reference/bigquerydatatransfer/latest/google.cloud.bigquery_datatransfer_v1.services.data_transfer_service.DataTransferServiceClient
    .. auth.default: https://google-auth.readthedocs.io/en/master/reference/google.auth.html
    """
    # pylint: enable=line-too-long
    credentials, _ = auth.default(quota_project_id=project_id, scopes=_DATA_TRANSFER_AUTH_SCOPES)
    return bigquery_datatransfer.DataTransferServiceClient(credentials=credentials)


def _create_transfer_config_request(
    *,
    source_table: _SimpleTableSpec,
    target_table: _SimpleTableSpec,
    notification_pubsub_topic: Optional[str] = None,
    bq_transfer_sa: Optional[str] = None,
) -> bigquery_datatransfer.CreateTransferConfigRequest:
    """
        Equivalent to::
            bq mk --transfer_config \
                --data_source=cross_region_copy \
                --project_id=<TARGET_PROJECT_ID> \
                --target_dataset=<TARGET_DATASET_ID> \
                --display_name=<TARGET_DATASET_ID> \
                --notification_pubsub_topic=<PUBSUB_TOPIC> \
                --params='{
                    "source_project_id":"<SOURCE_PROJECT_ID>",
                    "source_dataset_id":"<SOURCE_DATASET_ID>",
                    "overwrite_destination_table":"true"
                }'
    """
    transfer_config = bigquery_datatransfer.TransferConfig(
        display_name=f'Cross-region tansfer {target_table}',
        data_source_id=_DATA_TRANSFER_DATA_SOURCE_ID,
        destination_dataset_id=target_table.dataset_id,
        data_refresh_window_days=0,
        schedule_options=bigquery_datatransfer.ScheduleOptions(disable_auto_scheduling=True),
        notification_pubsub_topic=notification_pubsub_topic,
        params=struct_pb2.Struct(  # pylint: disable=no-member
            fields=dict(
                source_project_id=struct_pb2.Value(  # pylint: disable=no-member
                    string_value=source_table.project_id
                ),
                source_dataset_id=struct_pb2.Value(  # pylint: disable=no-member
                    string_value=source_table.dataset_id
                ),
                overwrite_destination_table=struct_pb2.Value(  # pylint: disable=no-member
                    bool_value=True
                ),
            )
        ),
    )
    return bigquery_datatransfer.CreateTransferConfigRequest(
        parent=f'projects/{target_table.project_id}',
        transfer_config=transfer_config,
        service_account_name=bq_transfer_sa,
    )


def _create_transfer_run_request(
    name: str, run_time_ms: Optional[int] = None
) -> bigquery_datatransfer.StartManualTransferRunsRequest:
    """
    Equivalent to::
        bq mk --transfer_run \
              --location=<TARGET_LOCATION> \
              --project_id=<TARGET_PROJECT_ID> \
              --run_time=<TIMESTAMP WHEN TO RUN> \
              <TRANSFER_NAME>
    """
    requested_run_time = timestamp_pb2.Timestamp()  # pylint: disable=no-member
    if run_time_ms is not None:
        requested_run_time.FromMilliseconds(run_time_ms)
    return bigquery_datatransfer.StartManualTransferRunsRequest(
        parent=name,
        requested_run_time=requested_run_time,
    )


def remove_transfer_config(name: str) -> None:
    """
    Removes a transfer config by name.

    :param name:
    :return:
    """
    _LOGGER.debug('Removing transfer config <%s>', name)
    # validated input
    name = _stripped_str_arg('name', name)
    name_match = const.TRANSFER_CONFIG_NAME_RE.match(name)
    if not name_match:
        raise ValueError(
            f'Transfer config name <{name}> '
            f'does not match rule in <{const.TRANSFER_CONFIG_NAME_RE}>'
        )
    # logic
    project_id = name_match.group(0)
    request = bigquery_datatransfer.DeleteTransferConfigRequest(name=name)
    client = _data_transfer_client(project_id)
    try:
        client.delete_transfer_config(request=request)
    except Exception as err:
        raise RuntimeError(f'Could not remove transfer config {name}. Error: {err}') from err
