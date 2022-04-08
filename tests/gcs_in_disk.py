# vim: ai:sw=4:ts=4:sta:et:fo=croql
# pylint: disable=missing-module-docstring,missing-class-docstring,missing-function-docstring
# pylint: disable=missing-function-docstring
# pylint: disable=protected-access,redefined-outer-name,no-self-use,using-constant-test
# pylint: disable=invalid-name,attribute-defined-outside-init,too-few-public-methods
# type: ignore
import os
import pathlib
from typing import Any, Dict, Generator

POLICY_BUCKET: str = 'policy_bucket'
REQUEST_BUCKET: str = 'request_bucket'

_TEST_DATA_DIR: pathlib.Path = pathlib.Path(__file__).parent.parent.joinpath('test_data')


def read_object(bucket_name: str, object_path: str) -> bytes:
    """
    To mimic `gcp_storage.read_object(bucket_name, object_path)`.
    :param bucket_name:
    :param object_path:
    :return:
    """
    return _read_bytes(_get_bucket_path(bucket_name, object_path))


def _get_bucket_path(bucket_name: str, object_path: str) -> pathlib.Path:
    return _TEST_DATA_DIR.joinpath(bucket_name, object_path)


def _read_bytes(file_path: pathlib.Path) -> Dict[str, Any]:
    with open(file_path, 'rb') as in_file:
        result = in_file.read()
    return result


def list_blob_names(bucket_name: str) -> Generator[str, None, None]:
    """
    To mimic `gcp_storage._list_blob_names(bucket_name)`.
    :param bucket_name:
    :return:
    """
    path = _TEST_DATA_DIR.joinpath(bucket_name)
    path_len = len(str(path))
    for root, d_names, f_names in os.walk(path):
        actual_root = root[path_len + 1 :]
        if actual_root:
            actual_root += '/'
        for dirname in d_names:
            yield actual_root + dirname + '/'
        for filename in f_names:
            yield actual_root + filename
