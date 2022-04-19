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
from typing import Any, Dict, List, Optional, Tuple

from google.cloud import bigquery

from bq_sampler.dto import sample
from bq_sampler.gcp import big_query


_LOGGER = logging.getLogger(__name__)

SORT_ASC: str = "ASC"
SORT_DESC: str = "DESC"
_BQ_VALID_SORTING: List[str] = [SORT_ASC, SORT_DESC]

_BQ_PERCENT_INT_PARAM: str = 'percent_int'
_BQ_ROW_AMOUNT_INT_PARAM: str = 'row_amount_int'
_BQ_SOURCE_TABLE_PARAM: str = 'source_table'
_BQ_ORDER_BY_COLUMN: str = 'column_name'
_BQ_ORDER_BY_DIRECTION: str = 'direction'
_BQ_TARGET_TABLE_PARAM: str = 'target_table'

_BQ_RANDOM_SAMPLE_QUERY_TMPL: str = f"""
    SELECT * FROM `%({_BQ_SOURCE_TABLE_PARAM})s`
    TABLESAMPLE SYSTEM (%({_BQ_PERCENT_INT_PARAM})d PERCENT)
    LIMIT %({_BQ_ROW_AMOUNT_INT_PARAM})d
"""
# pylint: disable=line-too-long
"""
Uses `SELECT`_ statement using `TABLESAMPLE`_ operator.

.. SELECT: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#select_list
.. TABLESAMPLE: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#tablesample_operator
"""
# pylint: enable=line-too-long

_BQ_SORTED_SAMPLE_QUERY_TMPL: str = f"""
    SELECT * FROM `%({_BQ_SOURCE_TABLE_PARAM})s`
    ORDER BY %({_BQ_ORDER_BY_COLUMN})s %({_BQ_ORDER_BY_DIRECTION})s
    LIMIT %({_BQ_ROW_AMOUNT_INT_PARAM})d
"""
# pylint: disable=line-too-long
"""
Uses `SELECT`_ statement `ORDER BY`_ clause.

.. INSERT: https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#insert_statement
.. SELECT: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#select_list
.. ORDER BY: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#order_by_clause
"""
# pylint: enable=line-too-long

_BQ_INSERT_RANDOM_SAMPLE_QUERY_TMPL: str = f"""
    INSERT INTO `%({_BQ_TARGET_TABLE_PARAM})s`
    SELECT * FROM `%({_BQ_SOURCE_TABLE_PARAM})s`
    TABLESAMPLE SYSTEM (%({_BQ_PERCENT_INT_PARAM})d PERCENT)
    LIMIT %({_BQ_ROW_AMOUNT_INT_PARAM})d
"""
# pylint: disable=line-too-long
"""
Uses `INSERT`_ statements combined with `SELECT`_ statement using `TABLESAMPLE`_ operator.

.. INSERT: https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#insert_statement
.. SELECT: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#select_list
.. TABLESAMPLE: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#tablesample_operator
"""
# pylint: enable=line-too-long

_BQ_INSERT_RANDOM_SAMPLE_QUERY_TMPL: str = f"""
    INSERT INTO `%({_BQ_TARGET_TABLE_PARAM})s`
    SELECT * FROM `%({_BQ_SOURCE_TABLE_PARAM})s`
    TABLESAMPLE SYSTEM (%({_BQ_PERCENT_INT_PARAM})d PERCENT)
    LIMIT %({_BQ_ROW_AMOUNT_INT_PARAM})d
"""
# pylint: disable=line-too-long
"""
Uses `INSERT`_ statements combined with `SELECT`_ statement using `ORDER BY`_ clause.

.. INSERT: https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#insert_statement
.. SELECT: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#select_list
.. ORDER BY: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#order_by_clause
"""
# pylint: enable=line-too-long


def create_random_sample(
    *, table_ref: sample.TableReference, amount: int
) -> bigquery.table.RowIterator:
    """
    This will query the target table using BigQuery's `TABLESAMPLE`_ clause.
    This is important to know because for large tables the minimum amount is 1%
        (which will be reduced, if necessary, to the requested amount).
    This also means that your query cost is based on the closest (_ceil_) integer
        percentage to provide the `amount` in the argument.

    :param table_ref:
    :param amount:
    :return:

    .. TABLESAMPLE: https://cloud.google.com/bigquery/docs/table-sampling
    """
    # validate input
    _validate_amount(amount)
    _validate_table_reference('table_ref', table_ref)
    # logic
    return _create_random_sample(table_ref.get_table_fqn(), amount)


def _validate_table_reference(arg_name: str, table_ref: sample.TableReference) -> None:
    if not isinstance(table_ref, sample.TableReference):
        raise ValueError(
            f'Table reference for {arg_name} must be '
            f'an instance of {sample.TableReference.__name__}. '
            f'Got: <{table_ref}>{type(table_ref)}'
        )


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
    query_placeholders = _get_named_placeholders(  # pylint: disable=missing-kwoa
        source_table_fqn_id=table_fqn_id, amount=amount, percent_int=percent_int
    )
    return big_query.query_job_result(_BQ_RANDOM_SAMPLE_QUERY_TMPL % query_placeholders)


def _get_named_placeholders(  # pylint: disable=too-many-arguments
    *,
    source_table_fqn_id: Optional[str] = None,
    target_table_fqn_id: Optional[str] = None,
    amount: Optional[int] = None,
    percent_int: Optional[int] = None,
    column: Optional[str] = None,
    order: Optional[str] = None,
) -> Dict[str, Any]:
    result = {}
    if source_table_fqn_id is not None:
        result[_BQ_SOURCE_TABLE_PARAM] = source_table_fqn_id
    if target_table_fqn_id is not None:
        result[_BQ_TARGET_TABLE_PARAM] = target_table_fqn_id
    if percent_int is not None:
        result[_BQ_PERCENT_INT_PARAM] = percent_int
    if amount is not None:
        result[_BQ_ROW_AMOUNT_INT_PARAM] = amount
    if column is not None:
        result[_BQ_ORDER_BY_COLUMN] = column
    if order is not None:
        result[_BQ_ORDER_BY_DIRECTION] = order
    return result


def _get_int_percent(table_fqn_id: str, amount: int) -> int:
    size = big_query.get_table_row_count(table_fqn_id)
    percent = int(math.ceil(amount / size * 100.0))
    return min(100, max(1, percent))


def create_sorted_sample(
    *, table_ref: sample.TableReference, amount: int, column: str, order: str
) -> bigquery.table.RowIterator:
    # pylint: disable=line-too-long
    """
    Extra a sample sorted by given `column` and using
        `ORDER BY`_ clause with the direction in `order`.
    The amount is used in `LIMIT`_ clause.

    :param table_ref:
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
    _validate_table_reference('table_ref', table_ref)
    return _create_sorted_sample(table_ref.get_table_fqn(), amount, column, order)


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
    query_placeholders = _get_named_placeholders(
        source_table_fqn_id=table_fqn_id, amount=amount, column=column, order=order
    )
    return big_query.query_job_result(_BQ_SORTED_SAMPLE_QUERY_TMPL % query_placeholders)


def _validate_order(order: str) -> str:
    (result,) = _validate_str_args(order)
    result = result.upper()
    if result not in _BQ_VALID_SORTING:
        raise ValueError(f'Order must be a value in {_BQ_VALID_SORTING}. Got: <{order}>')
    return result


def create_table_with_random_sample(
    *,
    source_table_ref: sample.TableReference,
    target_table_ref: sample.TableReference,
    amount: int,
    labels: Optional[Dict[str, str]] = None,
    recreate_table: Optional[bool] = True,
) -> None:
    """

    :param source_table_ref:
    :param target_table_ref:
    :param amount:
    :param labels:
    :param recreate_table: if :py:obj:`True` (default) will drop the table prior to create it.
        If the table does not exist, it will ignore the drop.
    :return:
    """
    # validate input
    _validate_amount(amount)
    _validate_table_reference('source_table_ref', source_table_ref)
    _validate_table_reference('target_table_ref', target_table_ref)
    # logic
    _create_table_with_random_sample(
        source_table_ref.get_table_fqn(),
        target_table_ref.get_table_fqn(),
        amount,
        labels,
        recreate_table,
    )


def _create_table_with_random_sample(
    source_table_fqn_id: str,
    target_table_fqn_id: str,
    amount: int,
    labels: Optional[Dict[str, str]] = None,
    recreate_table: Optional[bool] = True,
) -> None:
    # create table
    _create_table(source_table_fqn_id, target_table_fqn_id, labels, recreate_table)
    # insert data
    percent_int = _get_int_percent(source_table_fqn_id, amount)
    query_placeholders = _get_named_placeholders(  # pylint: disable=missing-kwoa
        source_table_fqn_id=source_table_fqn_id,
        target_table_fqn_id=target_table_fqn_id,
        amount=amount,
        percent_int=percent_int,
    )
    big_query.query_job_result(_BQ_INSERT_RANDOM_SAMPLE_QUERY_TMPL % query_placeholders)


def _create_table(
    source_table_fqn_id: str,
    target_table_fqn_id: str,
    labels: Optional[Dict[str, str]] = None,
    recreate_table: Optional[bool] = True,
) -> None:
    src_table = big_query.get_table(source_table_fqn_id)
    big_query.create_table(target_table_fqn_id, src_table.schema, labels, recreate_table)


def create_table_with_sorted_sample(
    *,
    source_table_ref: sample.TableReference,
    target_table_ref: sample.TableReference,
    amount: int,
    column: str,
    order: str,
    labels: Optional[Dict[str, str]] = None,
    recreate_table: Optional[bool] = True,
) -> None:
    """

    :param source_table_ref:
    :param target_table_ref:
    :param amount:
    :param column:
    :param order:
    :param labels:
    :param recreate_table:
    :return:
    """
    # validate input
    _validate_amount(amount)
    _validate_table_reference('source_table_ref', source_table_ref)
    _validate_table_reference('target_table_ref', target_table_ref)
    # logic
    _create_table_with_sorted_sample(
        source_table_ref.get_table_fqn(),
        target_table_ref.get_table_fqn(),
        amount,
        column,
        order,
        labels,
        recreate_table,
    )


def _create_table_with_sorted_sample(  # pylint: disable=too-many-arguments
    source_table_fqn_id: str,
    target_table_fqn_id: str,
    amount: int,
    column: str,
    order: str,
    labels: Optional[Dict[str, str]] = None,
    recreate_table: Optional[bool] = True,
) -> None:
    # create table
    _create_table(source_table_fqn_id, target_table_fqn_id, labels, recreate_table)
    # insert data
    query_placeholders = _get_named_placeholders(  # pylint: disable=missing-kwoa
        source_table_fqn_id=source_table_fqn_id,
        target_table_fqn_id=target_table_fqn_id,
        amount=amount,
        column=column,
        order=order,
    )
    big_query.query_job_result(_BQ_INSERT_RANDOM_SAMPLE_QUERY_TMPL % query_placeholders)
