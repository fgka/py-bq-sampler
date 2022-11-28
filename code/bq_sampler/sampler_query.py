# vim: ai:sw=4:ts=4:sta:et:fo=croql
# pylint: disable=line-too-long
"""
Reads an object from `Cloud Big Query`_ using `Python client`_.

.. Cloud Big Query: https://cloud.google.com/bigquery/docs/reference/libraries#client-libraries-install-python
.. Python client: https://googleapis.dev/python/bigquery/latest/index.html
"""
# pylint: enable=line-too-long
import math
from typing import Any, Dict, List, Optional, Tuple
import uuid

from bq_sampler import const, logger
from bq_sampler.entity import table
from bq_sampler.gcp import bq

_LOGGER = logger.get(__name__)

_BQ_VALID_SORTING: List[str] = [const.BQ_ORDER_BY_ASC, const.BQ_ORDER_BY_DESC]

_BQ_PERCENT_INT_PARAM: str = 'percent_int'
_BQ_ROW_AMOUNT_INT_PARAM: str = 'row_amount_int'
_BQ_SOURCE_TABLE_PARAM: str = 'source_table'
_BQ_ORDER_BY_COLUMN: str = 'column_name'
_BQ_ORDER_BY_DIRECTION: str = 'direction'
_BQ_TARGET_TABLE_PARAM: str = 'target_table'

_BQ_RANDOM_SAMPLE_QUERY_RAND_TMPL: str = f"""
    SELECT * FROM `%({_BQ_SOURCE_TABLE_PARAM})s`
    ORDER BY RAND()
    LIMIT %({_BQ_ROW_AMOUNT_INT_PARAM})d
"""
# pylint: disable=line-too-long
"""
Uses `SELECT`_ statement using `RAND`_ operator.

.. SELECT: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#select_list
.. RAND: https://cloud.google.com/bigquery/docs/reference/standard-sql/functions-and-operators#rand
"""
# pylint: enable=line-too-long
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

_BQ_INSERT_RANDOM_SAMPLE_QUERY_RAND_TMPL: str = (
    f'INSERT INTO `%({_BQ_TARGET_TABLE_PARAM})s`' + _BQ_RANDOM_SAMPLE_QUERY_RAND_TMPL
)
# pylint: disable=line-too-long
"""
Uses `INSERT`_ statements combined with `SELECT`_ statement using `TABLESAMPLE`_ operator.

.. INSERT: https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#insert_statement
.. SELECT: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#select_list
.. RAND: https://cloud.google.com/bigquery/docs/reference/standard-sql/functions-and-operators#rand
"""
# pylint: enable=line-too-long
_BQ_INSERT_RANDOM_SAMPLE_QUERY_TMPL: str = (
    f'INSERT INTO `%({_BQ_TARGET_TABLE_PARAM})s`' + _BQ_RANDOM_SAMPLE_QUERY_TMPL
)
# pylint: disable=line-too-long
"""
Uses `INSERT`_ statements combined with `SELECT`_ statement using `TABLESAMPLE`_ operator.

.. INSERT: https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#insert_statement
.. SELECT: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#select_list
.. TABLESAMPLE: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#tablesample_operator
"""
# pylint: enable=line-too-long

_BQ_INSERT_SORTED_SAMPLE_QUERY_TMPL: str = (
    f'INSERT INTO `%({_BQ_TARGET_TABLE_PARAM})s`' + _BQ_SORTED_SAMPLE_QUERY_TMPL
)
# pylint: disable=line-too-long
"""
Uses `INSERT`_ statements combined with `SELECT`_ statement using `ORDER BY`_ clause.

.. INSERT: https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#insert_statement
.. SELECT: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#select_list
.. ORDER BY: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#order_by_clause
"""
# pylint: enable=line-too-long


def row_count(table_ref: table.TableReference) -> int:
    """
    Simple wrapper to :py:func:`bq.row_count`

    :param table_ref:
    :return:
    """
    return bq.row_count(table_fqn_id=table_ref.table_fqn_id())


def drop_all_sample_tables(
    *,
    project_id: str,
    labels: Optional[Dict[str, str]] = None,
) -> None:
    """
    Just a wrapper for :py:func:`bq.drop_all_tables_by_labels`.

    :param project_id:
    :param labels:
    :return:
    """
    bq.drop_all_tables_by_labels(project_id=project_id, labels=labels)


def remove_all_empty_sample_datasets(
    *,
    project_id: str,
    labels: Optional[Dict[str, str]] = None,
) -> None:
    """
    Just a wrapper for :py:func:`bq.remove_all_empty_datasets_by_labels`.

    :param project_id:
    :param labels:
    :return:
    """
    bq.remove_all_empty_datasets_by_labels(project_id=project_id, labels=labels)


def remove_all_transfer_config(
    *,
    project_id: str,
    location: str,
) -> None:
    """
    Just a wrapper for :py:func:`bq.remove_all_transfer_config_by_display_name_prefix`.

    :param project_id:
    :param location:
    :return:
    """
    bq.remove_all_transfer_config_by_display_name_prefix(project_id=project_id, location=location)


def create_table_with_random_sample(
    *,
    source_table_ref: table.TableReference,
    target_table_ref: table.TableReference,
    amount: int,
    labels: Optional[Dict[str, str]] = None,
    notification_pubsub_topic: Optional[str] = None,
    recreate_table: Optional[bool] = True,
) -> int:
    """
    Will create the target table and put the source table sample directly into it.
    See :py:func:`random_sample` for details in the sampling strategy.

    :param source_table_ref:
    :param target_table_ref:
    :param amount:
    :param labels:
    :param notification_pubsub_topic:
    :param recreate_table: if :py:obj:`True` (default) will drop the table prior to create it.
        If the table does not exist, it will ignore the drop.
    :return: amount of rows inserted
    """
    # validate input
    _validate_table_to_table_sample(source_table_ref, target_table_ref)
    _validate_amount(amount)
    labels = _add_standard_labels(source_table_ref, labels)
    # logic
    return _create_table_with_random_sample(
        source_table_ref=source_table_ref,
        target_table_ref=target_table_ref,
        amount=amount,
        labels=labels,
        notification_pubsub_topic=notification_pubsub_topic,
        recreate_table=recreate_table,
    )


def _validate_amount(amount: int) -> None:
    if not isinstance(amount, int) or amount < 0:
        raise ValueError(
            f'Amount must be an int greater or equal 0. Got: amount:{type(amount)}=<{amount}>'
        )


def _add_standard_labels(
    source_table_ref: table.TableReference, value: Optional[Dict[str, str]] = None
) -> Dict[str, str]:
    if not isinstance(value, dict):
        value = {}
    value = {
        **value,
        **dict(
            source_project_id=source_table_ref.project_id,
            source_location=source_table_ref.location,
        ),
    }
    return value


def _validate_table_to_table_sample(
    source_table_ref: table.TableReference,
    target_table_ref: table.TableReference,
) -> None:
    _validate_table_reference('source_table_ref', source_table_ref)
    _validate_table_reference('target_table_ref', target_table_ref)
    _LOGGER.warning(
        'Source <%s> and target <%s> tables are not in the same location. '
        'It will increase costs.',
        source_table_ref,
        target_table_ref,
    )


def _validate_table_reference(arg_name: str, table_ref: table.TableReference) -> None:
    if not isinstance(table_ref, table.TableReference):
        raise ValueError(
            f'Table reference for {arg_name} must be '
            f'an instance of {table.TableReference.__name__}. '
            f'Got: <{table_ref}>{type(table_ref)}'
        )


def _create_table_with_random_sample(
    *,
    source_table_ref: table.TableReference,
    target_table_ref: table.TableReference,
    amount: int,
    labels: Optional[Dict[str, str]] = None,
    notification_pubsub_topic: Optional[str] = None,
    recreate_table: Optional[bool] = True,
) -> int:
    # setup
    staging_target_table_ref = _pre_sample_setup(
        source_table_ref=source_table_ref,
        target_table_ref=target_table_ref,
        labels=labels,
        recreate_table=recreate_table,
    )
    # insert data
    percent_int = _int_percent_for_tablesample_stmt(source_table_ref.table_fqn_id(), amount)
    if amount <= 0 or percent_int <= 0:
        _LOGGER.warning(
            'Ignoring random sample request for table <%s> '
            'because either the amount <%s> or percentual <%s> are zero',
            source_table_ref.table_fqn_id(False),
            amount,
            percent_int,
        )
    else:
        query_placeholders = _named_placeholders(  # pylint: disable=missing-kwoa
            source_table_fqn_id=source_table_ref.table_fqn_id(False),
            target_table_fqn_id=staging_target_table_ref.table_fqn_id(False),
            amount=amount,
            percent_int=percent_int,
        )
        _sample_query_execution(
            query=_BQ_INSERT_RANDOM_SAMPLE_QUERY_TMPL % query_placeholders,
            fallback_query=_BQ_INSERT_RANDOM_SAMPLE_QUERY_RAND_TMPL % query_placeholders,
            staging_target_table_ref=staging_target_table_ref,
            target_table_ref=target_table_ref,
            notification_pubsub_topic=notification_pubsub_topic,
        )
    return row_count(target_table_ref)


def _pre_sample_setup(
    *,
    source_table_ref: table.TableReference,
    target_table_ref: table.TableReference,
    labels: Optional[Dict[str, str]] = None,
    recreate_table: Optional[bool] = True,
) -> table.TableReference:
    # create target table
    _create_table(
        source_table_fqn_id=source_table_ref.table_fqn_id(),
        target_table_fqn_id=target_table_ref.table_fqn_id(),
        labels=labels,
        recreate_table=recreate_table,
    )
    # return target staging table
    return _staging_target_table_ref(
        source_table_ref=source_table_ref,
        target_table_ref=target_table_ref,
        labels=labels,
        recreate_table=recreate_table,
    )


def _create_table(
    *,
    source_table_fqn_id: str,
    target_table_fqn_id: str,
    labels: Optional[Dict[str, str]] = None,
    recreate_table: Optional[bool] = True,
) -> None:
    src_table = bq.table(table_fqn_id=source_table_fqn_id)
    try:
        bq.create_table(
            table_fqn_id=target_table_fqn_id,
            schema=src_table.schema,
            labels=labels,
            drop_table_before=recreate_table,
        )
    except Exception as err:  # pylint: disable=broad-except
        raise RuntimeError(f'Could not create table {target_table_fqn_id}. Error: {err}') from err


def _staging_target_table_ref(
    *,
    source_table_ref: table.TableReference,
    target_table_ref: table.TableReference,
    labels: Optional[Dict[str, str]] = None,
    recreate_table: Optional[bool] = True,
) -> table.TableReference:
    result = target_table_ref
    # for different locations we need to have a stage table for sampling
    # and then transfer to the correct region
    if source_table_ref.location != target_table_ref.location:
        dataset_id = _staging_dataset_id(source_table_ref, target_table_ref)
        # create temp table on different temp dataset in the same location
        result = target_table_ref.clone(dataset_id=dataset_id, location=source_table_ref.location)
        _create_table(
            source_table_fqn_id=source_table_ref.table_fqn_id(),
            target_table_fqn_id=result.table_fqn_id(),
            labels=labels,
            recreate_table=recreate_table,
        )
        _LOGGER.info(
            "Defined staging target for x-location sampling. Staging: %s. Actual Target: %s",
            result,
            target_table_ref,
        )
    return result


def _staging_dataset_id(
    source_table_ref: table.TableReference,
    target_table_ref: table.TableReference,
) -> str:
    return bq.bigquery_valid_string(
        f'{const.TRANSFER_TEMP_DATASET_NAME_PREFIX}{target_table_ref.dataset_id[0:200]}'
        f'_{target_table_ref.table_id[0:200]}'
        f'_{source_table_ref.location[0:200]}'
        f'_{uuid.uuid4()}'
    )


def _int_percent_for_tablesample_stmt(table_fqn_id: str, amount: int) -> int:
    size = bq.row_count(table_fqn_id=table_fqn_id)
    if not isinstance(size, int) or size < 0:
        raise ValueError(
            f'Table {table_fqn_id} number of rows must be greater or equal 0. Got: <{size}>'
        )
    if size == 0:
        result = 0
    else:
        percent = int(math.ceil(amount / size * 100.0))
        result = min(100, max(1, percent))
    return result


def _named_placeholders(  # pylint: disable=too-many-arguments
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


def _sample_query_execution(
    *,
    query: str,
    staging_target_table_ref: table.TableReference,
    target_table_ref: table.TableReference,
    fallback_query: Optional[str] = None,
    notification_pubsub_topic: Optional[str] = None,
) -> None:
    try:
        bq.query_job_result(
            query=query,
            project_id=staging_target_table_ref.project_id,
            location=staging_target_table_ref.location,
        )
    except Exception as err_query:  # pylint: disable=broad-except
        if fallback_query:
            _LOGGER.warning(
                'Query failed, trying fallback query. Query: %s. Error: %s. Fallback query: %s',
                query,
                err_query,
                fallback_query,
            )
            try:
                bq.query_job_result(
                    query=fallback_query,
                    project_id=staging_target_table_ref.project_id,
                    location=staging_target_table_ref.location,
                )
            except Exception as err_fallback_query:  # pylint: disable=broad-except
                raise RuntimeError(
                    f'Could not execute fallback query. Query: {fallback_query}. '
                    f'Error: {err_fallback_query}'
                ) from err_fallback_query
        else:
            raise RuntimeError(
                f'Could not execute query and no fallback provided. Query: {query}. '
                f'Error: {err_query}'
            ) from err_query
    if staging_target_table_ref != target_table_ref:
        # since there was a staging table, we need to transfer to the target table
        _transfer_content_x_location(
            source_table_ref=staging_target_table_ref,
            target_table_ref=target_table_ref,
            notification_pubsub_topic=notification_pubsub_topic,
        )


def _transfer_content_x_location(
    *,
    source_table_ref: table.TableReference,
    target_table_ref: table.TableReference,
    notification_pubsub_topic: Optional[str] = None,
) -> None:
    if source_table_ref.location != target_table_ref.location:
        # A transfer needs to happen
        bq.cross_location_copy(
            source_table_fqn_id=source_table_ref.table_fqn_id(),
            target_table_fqn_id=target_table_ref.table_fqn_id(),
            notification_pubsub_topic=notification_pubsub_topic,
        )


def create_table_with_sorted_sample(
    *,
    source_table_ref: table.TableReference,
    target_table_ref: table.TableReference,
    amount: int,
    column: str,
    order: str,
    labels: Optional[Dict[str, str]] = None,
    notification_pubsub_topic: Optional[str] = None,
    recreate_table: Optional[bool] = True,
) -> int:
    """
    Will create the target table and put the source table sample directly into it.
    See :py:func:`sorted_sample` for details in the sampling strategy.

    :param source_table_ref:
    :param target_table_ref:
    :param amount:
    :param column:
    :param order:
    :param labels:
    :param notification_pubsub_topic:
    :param recreate_table:
    :return: amount of rows inserted
    """
    # validate input
    _validate_table_to_table_sample(source_table_ref, target_table_ref)
    _validate_amount(amount)
    labels = _add_standard_labels(source_table_ref, labels)
    (column,) = _validate_str_args(column)
    order = _validate_order(order)
    # logic
    return _create_table_with_sorted_sample(
        source_table_ref=source_table_ref,
        target_table_ref=target_table_ref,
        amount=amount,
        column=column,
        order=order,
        labels=labels,
        notification_pubsub_topic=notification_pubsub_topic,
        recreate_table=recreate_table,
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


def _validate_order(order: str) -> str:
    (result,) = _validate_str_args(order)
    result = result.upper()
    if result not in _BQ_VALID_SORTING:
        raise ValueError(f'Order must be a value in {_BQ_VALID_SORTING}. Got: <{order}>')
    return result


def _create_table_with_sorted_sample(  # pylint: disable=too-many-arguments
    *,
    source_table_ref: table.TableReference,
    target_table_ref: table.TableReference,
    amount: int,
    column: str,
    order: str,
    labels: Optional[Dict[str, str]] = None,
    notification_pubsub_topic: Optional[str] = None,
    recreate_table: Optional[bool] = True,
) -> int:
    # setup
    staging_target_table_ref = _pre_sample_setup(
        source_table_ref=source_table_ref,
        target_table_ref=target_table_ref,
        labels=labels,
        recreate_table=recreate_table,
    )
    # insert data
    if amount <= 0:
        _LOGGER.warning(
            'Ignoring sorted sample request for table <%s> because either the amount <%s> is zero',
            source_table_ref.table_fqn_id(False),
            amount,
        )
    else:
        query_placeholders = _named_placeholders(  # pylint: disable=missing-kwoa
            source_table_fqn_id=source_table_ref.table_fqn_id(False),
            target_table_fqn_id=staging_target_table_ref.table_fqn_id(False),
            amount=amount,
            column=column,
            order=order,
        )
        _sample_query_execution(
            query=_BQ_INSERT_SORTED_SAMPLE_QUERY_TMPL % query_placeholders,
            staging_target_table_ref=staging_target_table_ref,
            target_table_ref=target_table_ref,
            notification_pubsub_topic=notification_pubsub_topic,
        )
    return row_count(target_table_ref)
