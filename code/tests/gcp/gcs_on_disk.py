# vim: ai:sw=4:ts=4:sta:et:fo=croql
# pylint: disable=missing-module-docstring,missing-class-docstring,missing-function-docstring
# pylint: disable=missing-function-docstring
# pylint: disable=protected-access,redefined-outer-name,no-self-use,using-constant-test
# pylint: disable=invalid-name,attribute-defined-outside-init,too-few-public-methods
# type: ignore
import os
import pathlib
from typing import Any, Callable, Dict, Generator, Optional

POLICY_BUCKET: str = 'policy_bucket'
REQUEST_BUCKET: str = 'request_bucket'

_TEST_DATA_DIR: pathlib.Path = pathlib.Path(__file__).parent.parent.parent.joinpath('test_data')


def read_object(bucket_name: str, object_path: str, warn_read_failure: Optional[bool] = True) -> bytes:
    """
    To mimic `gcp_storage.read_object(bucket_name, object_path)`.
    :param bucket_name:
    :param object_path:
    :param warn_read_failure:
    :return:
    """
    return _read_bytes(_get_bucket_path(bucket_name, object_path))


def _get_bucket_path(bucket_name: str, object_path: str) -> pathlib.Path:
    return _TEST_DATA_DIR.joinpath(bucket_name, object_path)


def _read_bytes(file_path: pathlib.Path) -> Dict[str, Any]:
    with open(file_path, 'rb') as in_file:
        result = in_file.read()
    return result


def get_gcs_prefixes_http_iterator(bucket_name: str, prefix: str) -> Generator[str, None, None]:
    root_path = _TEST_DATA_DIR.joinpath(bucket_name)
    path = root_path.joinpath(prefix)
    for item in path.iterdir():
        if item.is_dir():
            yield f'{item.relative_to(root_path)}/'


def list_blob_names(bucket_name: str, prefix: str) -> Generator[str, None, None]:
    """
    To mimic `gcp_storage._list_blob_names(bucket_name)`.
    :param prefix:
    :param bucket_name:
    :return:
    """
    path = _TEST_DATA_DIR.joinpath(bucket_name)
    root_path_len = len(str(path))
    if isinstance(prefix, str):
        path = path.joinpath(prefix)
    for root, d_names, f_names in os.walk(path):
        actual_root = root[root_path_len + 1 :]
        if actual_root:
            actual_root += '/'
        for dirname in d_names:
            yield actual_root + dirname + '/'
        for filename in f_names:
            yield actual_root + filename
