# vim: ai:sw=4:ts=4:sta:et:fo=croql
# pylint: disable=line-too-long
"""
Reads an object from `Cloud Big Query`_ using `Python client`_.

.. Cloud Big Query: https://cloud.google.com/bigquery/docs/reference/libraries#client-libraries-install-python
.. Python client: https://googleapis.dev/python/bigquery/latest/index.html
"""
# pylint: enable=line-too-long
from typing import Callable, Dict, Generator, Optional

import cachetools

from google.cloud import bigquery

from bq_sampler import const
from bq_sampler.gcp.bq import _bq_base
from bq_sampler import logger

_LOGGER = logger.get(__name__)

"""
Default GCP resource label to be applied table created here.
"""


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
    _LOGGER.debug('Reading table size from :<%s>', table_fqn_id)
    table = _bq_base.table(table_fqn_id=table_fqn_id)
    return table.num_rows


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
    job = _bq_base.query_job(
        query=query, job_config=job_config, project_id=project_id, location=location
    )
    try:
        result = job.result()
    except Exception as err:  # pylint: disable=broad-except
        raise RuntimeError(
            f'Could not retrieve results from query <{job.query}>. Error: {err}'
        ) from err
    return result


def drop_all_tables_by_labels(
    *, project_id: str, location: Optional[str] = None, labels: Optional[Dict[str, str]] = None
) -> None:
    """
    Will list all tables and remove all that matches the label criteria.

    :param project_id:
    :param location:
    :param labels:
    :return:
    """
    # validate input
    labels = _validate_table_labels(labels)
    # logic
    _LOGGER.debug('Dropping all tables in project <%s> with labels <%s>', project_id, labels)
    filter_fn = _has_table_labels_fn(labels)
    _drop_all_tables_in_iter(
        _bq_base.list_all_tables_with_filter(
            project_id=project_id, location=location, filter_fn=filter_fn
        )
    )


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
