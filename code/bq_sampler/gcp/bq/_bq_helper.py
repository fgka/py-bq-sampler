# vim: ai:sw=4:ts=4:sta:et:fo=croql
# pylint: disable=line-too-long
"""
Reads an object from `Cloud Big Query`_ using `Python client`_.

.. _Cloud Big Query: https://cloud.google.com/bigquery/docs/reference/libraries#client-libraries-install-python
.. _Python client: https://googleapis.dev/python/bigquery/latest/index.html
"""
# pylint: enable=line-too-long
import re
from typing import Callable, Dict, Generator, Optional, Sequence

import cachetools

from google.cloud import bigquery, bigquery_datatransfer

from bq_sampler import const, logger
from bq_sampler.gcp.bq import _bq_base

_LOGGER = logger.get(__name__)

_ROW_COUNT_FOR_VIEW_QUERY_TMPL: str = 'SELECT COUNT(*) FROM `%s`'


@cachetools.cached(cache=cachetools.LRUCache(maxsize=100_000))
def row_count(*, table_fqn_id: str) -> int:
    """
    Compute table size (in rows) for the argument.

    **NOTE**: This call is cached,
        since it is assumed that during a session the row count
        will not dramatically change.

    :param table_fqn_id: in the format `<PROJECT_ID>.<DATASET_ID>.<TABLE_ID>[@<LOCATION>]`.
    :return:
    """
    _LOGGER.debug('Reading table size from <%s>', table_fqn_id)
    table = _bq_base.table(table_fqn_id=table_fqn_id)
    result = table.num_rows
    if result == 0 and table.view_query:
        _LOGGER.info('The table <%s> is view, computing num of rows using SQL', table_fqn_id)
        result = _row_count_by_count(table=table)
    _LOGGER.debug('Table <%s> has %d rows', table_fqn_id, result)
    return result


def _row_count_by_count(table: bigquery.Table) -> int:
    _LOGGER.info('Computing num of rows for table <%s> using SQL', table.full_table_id)
    query_result: bigquery.table.RowIterator = query_job_result(
        query=_ROW_COUNT_FOR_VIEW_QUERY_TMPL % table.full_table_id.replace(':', '.'),
        project_id=table.project,
        location=table.location,
    )
    row: bigquery.table.Row = next(query_result)
    return row.values()[0]


def query_job_result(
    *,
    query: str,
    job_config: Optional[bigquery.QueryJobConfig] = None,
    project_id: Optional[str] = None,
    location: Optional[str] = None,
) -> bigquery.table.RowIterator:
    """
    Forces the py:class:`bigquery.job.query.QueryJob` to get the results
        (works as a synchronization mechanism too).

    :param query:
    :param job_config:
    :param project_id:
    :param location:
    :return:
    """
    _LOGGER.debug(
        'Issuing query job results for query <%s> in project <%s>@<%s>', query, project_id, location
    )
    job = _bq_base.query_job(
        query=query, job_config=job_config, project_id=project_id, location=location
    )
    try:
        result = job.result()
    except Exception as err:  # pylint: disable=broad-except
        raise RuntimeError(
            f'Could not retrieve results from query <{job.query}>. Error: {err}'
        ) from err
    _LOGGER.info(
        'Query <%s> stats: '
        'total bytes processed %s; '
        'total bytes billed %s; '
        'slot milliseconds: %s.',
        _query_from_job_to_log_str(job),
        job.total_bytes_processed,
        job.total_bytes_billed,
        job.slot_millis,
    )
    _LOGGER.debug('Query Job <%s> results: <%s>', job, result)
    return result


def _query_from_job_to_log_str(query_job_: bigquery.job.query.QueryJob) -> str:
    """
    Two reasons for this:
    1- f-strings do not allow '\' in the parameters;
    2- standard way to report query job's query in the logs.
    """
    return query_job_.query.replace('\n', '').strip() + f' -> {query_job_.query_parameters}'


def drop_all_tables_by_labels(*, project_id: str, labels: Optional[Dict[str, str]] = None) -> None:
    """
    Will list all tables and remove all that matches the label criteria.

    :param project_id:
    :param labels:
    :return:
    """
    # validate input
    _LOGGER.debug('Dropping all tables in project <%s> with labels <%s>', project_id, labels)
    labels = _validate_table_labels(labels)
    # logic
    filter_fn = _has_table_labels_fn(labels)
    _drop_all_tables_in_iter(
        _bq_base.list_all_tables_with_filter(project_id=project_id, filter_fn=filter_fn)
    )
    _LOGGER.debug('Dropped all tables in project <%s> with labels <%s>', project_id, labels)


def _validate_table_labels(labels: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    if not isinstance(labels, dict):
        labels = const.DEFAULT_CREATE_TABLE_LABELS
    return labels


def _has_table_labels_fn(labels: Dict[str, str]) -> Callable[[bigquery.table.TableListItem], bool]:
    def result_fn(table_list_item: bigquery.table.TableListItem) -> bool:
        result = True
        for key, val in labels.items():
            result = key in table_list_item.labels and val == table_list_item.labels.get(key)
            if not result:
                break
        return result

    return result_fn


def _drop_all_tables_in_iter(tables_to_drop_gen: Generator[str, None, None]) -> None:
    error_msgs = []
    last_error = None
    for table_fqn_id in tables_to_drop_gen:
        try:
            _bq_base.drop_table(table_fqn_id=table_fqn_id)
        except Exception as err:  # pylint: disable=broad-except
            error_msgs.append(f'Cloud not drop table <{table_fqn_id}>. Error: {err}')
            last_error = err
    if last_error is not None:
        raise RuntimeError('+++'.join(error_msgs)) from last_error


def remove_all_empty_datasets_by_labels(
    *, project_id: str, labels: Optional[Dict[str, str]] = None
) -> None:
    """
    Will list all datasets and remove all that matches the label criteria and are empty.

    :param project_id:
    :param labels:
    :return:
    """
    # validate input
    _LOGGER.debug(
        'Dropping all datasets empty in project <%s> with labels <%s>', project_id, labels
    )
    labels = _validate_table_labels(labels)
    # logic
    filter_fn = _has_table_labels_fn(labels)
    _bq_base.remove_empty_datasets(project_id=project_id, filter_fn=filter_fn)
    _LOGGER.debug('Dropped all datasets empty in project <%s> with labels <%s>', project_id, labels)


def cross_location_copy(
    *,
    source_table_fqn_id: str,
    target_table_fqn_id: str,
    notification_pubsub_topic: Optional[str] = None,
    transfer_config_display_name_prefix: Optional[str] = None,
) -> Sequence[bigquery_datatransfer.TransferRun]:
    """
    Forces the py:class:`bigquery.job.query.QueryJob` to get the results
        (works as a synchronization mechanism too).

    :param source_table_fqn_id:
    :param target_table_fqn_id:
    :param notification_pubsub_topic:
    :param transfer_config_display_name_prefix: if :py:obj:`None`
      uses :py:data:`const.TRANSFER_CONFIG_DISPLAY_NAME_PREFIX`.
    :return:
    """
    _LOGGER.debug(
        'Issuing copy job results from <%s> into <%s> with done notification sent to <%s>',
        source_table_fqn_id,
        target_table_fqn_id,
        notification_pubsub_topic,
    )
    try:
        result = _bq_base.dataset_transfer_config_run(
            source_table_fqn_id=source_table_fqn_id,
            target_table_fqn_id=target_table_fqn_id,
            notification_pubsub_topic=notification_pubsub_topic,
            transfer_config_display_name_prefix=transfer_config_display_name_prefix,
        )
    except Exception as err:  # pylint: disable=broad-except
        raise RuntimeError(
            f'Could not retrieve results from dataset copy from <{source_table_fqn_id}> '
            f'into <{target_table_fqn_id}> '
            f'with notification sent to <{notification_pubsub_topic}>. '
            f'Error: {err}'
        ) from err
    return result


def remove_all_transfer_config_by_display_name_prefix(
    *, project_id: str, location: str, prefix: Optional[str] = None
) -> None:
    """
    Gets all :py:class:`bigquery_datatransfer.TransferConfig` in `project_id`
      and whose display name starts with `prefix` and remove them.

    :param project_id:
    :param location:
    :param prefix: if :py:obj:`None` uses :py:data:`const.TRANSFER_CONFIG_DISPLAY_NAME_PREFIX`.
    :return:
    """
    for t_config in _bq_base.list_transfer_config_by_display_name_prefix(
        project_id=project_id, location=location, prefix=prefix
    ):
        _bq_base.remove_transfer_config(t_config.name)


def bigquery_valid_string(
    value: str,
) -> str:
    """
    Compliance with BigQuery naming constraints:
    * Only ASCII letters and digits, plus `_`;
    * Up to 1024 characters long.

    :param value:
    :return:
    """
    result = re.sub(r'\W', '_', value, flags=re.ASCII)
    return result[0:1023]
