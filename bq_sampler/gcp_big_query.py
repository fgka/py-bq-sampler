# vim: ai:sw=4:ts=4:sta:et:fo=croql
# pylint: disable=line-too-long
"""
Reads an object from `Cloud Big Query`_ using `Python client`_.

.. Cloud Big Query: https://cloud.google.com/bigquery/docs/reference/libraries#client-libraries-install-python
.. Python client: https://googleapis.dev/python/bigquery/latest/index.html
"""
# pylint: enable=line-too-long
import logging
from typing import Any, List, Tuple

import cachetools

from google.cloud import bigquery


SORT_ASC: str = "ASC"
SORT_DESC: str = "DESC"
VALID_SORTING: List[str] = [SORT_ASC, SORT_DESC]


def get_table_size(project_id: str, dataset_id: str, table_id: str) -> int:
    """
    Compute table size for the argument.

    :param project_id:
    :param dataset_id:
    :param table_id:
    :return:
    """
    # validate input
    project_id, dataset_id, table_id = _validate_str_args(project_id, dataset_id, table_id)
    logging.info('Reading table size from :<%s.%s.%s>', project_id, dataset_id, table_id)
    # TODO
    return None


def _validate_str_args(*args) -> Tuple[str]:
    # check input for non string
    if not all([isinstance(arg, str) for arg in args]):
        type_val_str = ' '.join([f'{type(arg)}=<{arg}>' for arg in args])
        raise ValueError(f'All arguments must be strings. Got: {type_val_str}')
    # cleaning input
    all_args = tuple([arg.strip() for arg in args])
    # check input for empty
    if any([arg.empty() for arg in all_args]):
        type_val_str = ' '.join([f'{type(arg)}=<{arg}>' for arg in all_args])
        raise ValueError(f'All arguments must be non-empty string. Got: {type_val_str}')
    return all_args


@cachetools.cached(cache=cachetools.LRUCache(maxsize=1))
def _client() -> bigquery.Client:
    return bigquery.Client()


def create_random_sample(project_id: str, dataset_id: str, table_id: str, size: int) -> Any:
    # validate input
    project_id, dataset_id, table_id = _validate_str_args(project_id, dataset_id, table_id)
    _validate_size(size)
    # TODO
    pass


def _validate_size(size: int) -> None:
    if not isinstance(size, int) or size <= 0:
        raise ValueError(f'Size must be an int greater than 0. Got: size:{type(size)}=<{size}>')


def create_sorted_sample(project_id: str, dataset_id: str, table_id: str, size: int, column: str, order: str) -> Any:
    # validate input
    project_id, dataset_id, table_id, column, order = _validate_str_args(project_id, dataset_id, table_id, column, order)
    _validate_size(size)
    # TODO
    pass


def drop_table(project_id: str, dataset_id: str, table_id: str) -> None:
    # validate input
    project_id, dataset_id, table_id = _validate_str_args(project_id, dataset_id, table_id)
    # TODO
    pass


def drop_all_tables_by_tag(project_id: str, tag: str) -> None:
    # validate input
    project_id, tag = _validate_str_args(project_id, tag)
    # TODO
    pass


def create_table_with_data(project_id: str, dataset_id: str, table_id: str, data: Any) -> None:
    # validate input
    project_id, dataset_id, table_id = _validate_str_args(project_id, dataset_id, table_id)
    # TODO
    pass
