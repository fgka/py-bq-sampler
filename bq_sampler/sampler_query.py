# vim: ai:sw=4:ts=4:sta:et:fo=croql
# pylint: disable=line-too-long
"""
Reads an object from `Cloud Big Query`_ using `Python client`_.

.. Cloud Big Query: https://cloud.google.com/bigquery/docs/reference/libraries#client-libraries-install-python
.. Python client: https://googleapis.dev/python/bigquery/latest/index.html
"""
# pylint: enable=line-too-long
import math
import logging
from typing import List, Tuple

from google.cloud import bigquery

from bq_sampler.gcp import big_query


_LOGGER = logging.getLogger(__name__)

SORT_ASC: str = "ASC"
SORT_DESC: str = "DESC"
_BQ_VALID_SORTING: List[str] = [SORT_ASC, SORT_DESC]

_BQ_PERCENT_INT_PARAM: str = 'percent_int'
_BQ_ROW_AMOUNT_INT_PARAM: str = 'row_amount_int'
_BQ_RANDOM_SAMPLE_QUERY_TMPL: str = f"""
    SELECT * FROM `%s`
    TABLESAMPLE SYSTEM (@{_BQ_PERCENT_INT_PARAM} PERCENT)
    LIMIT @{_BQ_ROW_AMOUNT_INT_PARAM}
"""
_BQ_SORTED_SAMPLE_QUERY_TMPL: str = f"""
    SELECT * FROM `%s`
    ORDER BY %s %s
    LIMIT @{_BQ_ROW_AMOUNT_INT_PARAM}
"""


def create_random_sample(
    *, project_id: str, dataset_id: str, table_id: str, amount: int
) -> bigquery.table.RowIterator:
    """
    This will query the target table using BigQuery's `TABLESAMPLE`_ clause.
    This is important to know because for large tables the minimum amount is 1%
        (which will be reduced, if necessary, to the requested amount).
    This also means that your query cost is based on the closest (_ceil_) integer
        percentage to provide the `amount` in the argument.

    :param project_id:
    :param dataset_id:
    :param table_id:
    :param amount:
    :return:

    .. TABLESAMPLE: https://cloud.google.com/bigquery/docs/table-sampling
    """
    # validate input
    _validate_amount(amount)
    table_fqn_id = _get_fqn_table(project_id, dataset_id, table_id)
    # logic
    return _create_random_sample(table_fqn_id, amount)


def _get_fqn_table(project_id: str, dataset_id: str, table_id: str) -> str:
    project_id, dataset_id, table_id = _validate_str_args(project_id, dataset_id, table_id)
    return '.'.join([project_id, dataset_id, table_id])


def _validate_str_args(*args) -> Tuple[str]:
    type_val_str = ' '.join([f'{type(arg)}=<{arg}>' for arg in args])
    # check input for non string
    if not all((isinstance(arg, str) for arg in args)):
        raise ValueError(f'All arguments must be strings. Got: {type_val_str}')
    # cleaning input
    all_args = tuple((arg.strip() for arg in args))
    # check input for empty
    if not all((bool(arg) for arg in all_args)):
        raise ValueError(f'All arguments must be non-empty string. Got: {type_val_str}')
    return all_args


def _validate_amount(amount: int) -> None:
    if not isinstance(amount, int) or amount <= 0:
        raise ValueError(
            f'Amount must be an int greater than 0. Got: amount:{type(amount)}=<{amount}>'
        )


def _create_random_sample(table_fqn_id: str, amount: int) -> bigquery.table.RowIterator:
    _LOGGER.info('Getting random sample of <%d> rows from table <%s>', amount, table_fqn_id)
    # query
    percent_int = _get_int_percent(table_fqn_id, amount)
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(_BQ_PERCENT_INT_PARAM, 'INT64', percent_int),
            bigquery.ScalarQueryParameter(_BQ_ROW_AMOUNT_INT_PARAM, 'INT64', amount),
        ]
    )
    return big_query.query_job_result(_BQ_RANDOM_SAMPLE_QUERY_TMPL % table_fqn_id, job_config)


def _get_int_percent(table_fqn_id: str, amount: int) -> int:
    size = big_query.get_table_row_count(table_fqn_id)
    percent = int(math.ceil(amount / size * 100.0))
    return min(100, max(1, percent))


def create_sorted_sample(
    *, project_id: str, dataset_id: str, table_id: str, amount: int, column: str, order: str
) -> bigquery.table.RowIterator:
    # pylint: disable=line-too-long
    """
    Extra a sample sorted by given `column` and using
        `ORDER BY`_ clause with the direction in `order`.
    The amount is used in `LIMIT`_ clause.

    :param project_id:
    :param dataset_id:
    :param table_id:
    :param amount:
    :param column:
    :param order:
    :return:

    .. ORDER BY: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#order_by_clause
    .. LIMIT: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#limit_and_offset_clause
    """
    # pylint: enable=line-too-long
    # validate input
    _validate_amount(amount)
    order = _validate_order(order)
    (column,) = _validate_str_args(column)
    table_fqn_id = _get_fqn_table(project_id, dataset_id, table_id)
    return _create_sorted_sample(table_fqn_id, amount, column, order)


def _create_sorted_sample(
    table_fqn_id: str, amount: int, column: str, order: str
) -> bigquery.table.RowIterator:
    _LOGGER.info(
        'Getting sorted by <%s>:<%s> sample of <%d> rows from table <%s>',
        column,
        order,
        amount,
        table_fqn_id,
    )
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(_BQ_ROW_AMOUNT_INT_PARAM, 'INT64', amount),
        ]
    )
    return big_query.query_job_result(
        _BQ_SORTED_SAMPLE_QUERY_TMPL % (table_fqn_id, column, order), job_config
    )


def _validate_order(order: str) -> str:
    (result,) = _validate_str_args(order)
    result = result.upper()
    if result not in _BQ_VALID_SORTING:
        raise ValueError(f'Order must be a value in {_BQ_VALID_SORTING}. Got: <{order}>')
    return result
