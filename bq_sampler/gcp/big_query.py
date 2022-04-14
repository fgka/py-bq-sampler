# vim: ai:sw=4:ts=4:sta:et:fo=croql
# pylint: disable=line-too-long
"""
Reads an object from `Cloud Big Query`_ using `Python client`_.

.. Cloud Big Query: https://cloud.google.com/bigquery/docs/reference/libraries#client-libraries-install-python
.. Python client: https://googleapis.dev/python/bigquery/latest/index.html
"""
# pylint: enable=line-too-long
import logging
from typing import Optional

import cachetools

from google.cloud import bigquery


_LOGGER = logging.getLogger(__name__)

DEFAULT_CREATE_TABLE_TAG: str = 'sample_table'
"""
Default GCP resource tag to be applied table created here.
"""


@cachetools.cached(cache=cachetools.LRUCache(maxsize=100_000))
def get_table_row_count(table_fqn_id: str) -> int:
    """
    Compute table size (in rows) for the argument.

    **NOTE**: This call is cached,
        since it is assumed that during a session the row count
        will not dramatically change.

    :param table_fqn_id: in the format <PROJECT_ID>.<DATASET_ID>.<TABLE_ID>
    :return:
    """
    return _get_table_size(_get_stripped_str_arg('table_fqn_id', table_fqn_id))


def _get_stripped_str_arg(name: str, value: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f'Argument <{name}> must be a non-empty string. Got: <{value}>')
    return value.strip()


def _get_table_size(table_fqn_id: str) -> int:
    _LOGGER.info('Reading table size from :<%s>', table_fqn_id)
    table = _get_table(table_fqn_id)
    return table.num_rows


def _get_table(table_fqn_id: str) -> bigquery.table.Table:
    try:
        result = _client().get_table(table_fqn_id)
    except Exception as err:
        msg = f'Could not retrieve table object for <{table_fqn_id}>. Error: {err}'
        _LOGGER.critical(msg)
        raise ValueError(msg) from err
    return result


@cachetools.cached(cache=cachetools.LRUCache(maxsize=1))
def _client() -> bigquery.Client:
    return bigquery.Client()


def query_job_result(
    query: str, job_config: Optional[bigquery.QueryJobConfig] = None
) -> bigquery.table.RowIterator:
    """

    :param query:
    :param job_config:
    :return:
    """
    job = query_job(query, job_config)
    try:
        result = job.result()
    except Exception as err:
        msg = f'Could not retrieve results from query <{_query_job_to_log_str(job)}>. Error: {err}'
        _LOGGER.critical(msg)
        raise RuntimeError(msg) from err
    return result


def query_job(
    query: str, job_config: Optional[bigquery.QueryJobConfig] = None
) -> bigquery.job.query.QueryJob:
    """
    :py:class:`bigquery.job.query.QueryJob` are Async by nature, see `docs`_.

    :param query:
    :param job_config:
    :return:

    .. docs: https://googleapis.dev/python/bigquery/latest/reference.html#job
    """
    query = _get_stripped_str_arg('query', query)
    _LOGGER.info(
        'Issuing BigQuery query: <%s> with job_config: <%s>', query, job_config.query_parameters
    )
    try:
        result = _client().query(query, job_config=job_config)
    except Exception as err:
        msg = (
            f'Could not issue BigQuery query: <{query}> '
            f'and job config: <{job_config.query_parameters}>. '
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


def drop_table(table_fqn_id: str) -> None:
    """

    :param table_fqn_id:
    :return:
    """
    table_fqn_id = _get_stripped_str_arg('table_fqn_id', table_fqn_id)
    # TODO


def drop_all_tables_by_tag(project_id: str, tag: str) -> None:
    """

    :param project_id:
    :param tag:
    :return:
    """
    project_id = _get_stripped_str_arg('project_id', project_id)
    tag = _get_stripped_str_arg('tag', tag)
    # TODO


def create_table(table_fqn_id: str, tag: Optional[str] = None) -> None:
    """

    :param table_fqn_id:
    :param tag:
    :return:
    """
    # validate input
    table_fqn_id = _get_stripped_str_arg('table_fqn_id', table_fqn_id)
    tag = _validate_table_tag(tag)
    # logic


def _validate_table_tag(table_tag: str) -> str:
    if not isinstance(table_tag, str) or not table_tag.strip():
        table_tag = DEFAULT_CREATE_TABLE_TAG
    else:
        table_tag = table_tag.strip()
    return table_tag
