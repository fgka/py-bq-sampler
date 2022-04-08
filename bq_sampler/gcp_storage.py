# vim: ai:sw=4:ts=4:sta:et:fo=croql
# pylint: disable=line-too-long
"""
Reads an object from `Cloud Storage`_.

.. Cloud Storage: https://cloud.google.com/appengine/docs/standard/python/googlecloudstorageclient/read-write-to-cloud-storage
"""
# pylint: enable=line-too-long
import logging
from typing import Callable, Generator, Optional

import cachetools

from google.cloud import storage


class CloudStorageDownloadError(Exception):
    """To code all GCS download errors"""


class CloudStorageListError(Exception):
    """To code all GCS list errors"""


def read_object(bucket_name: str, path: str) -> bytes:
    """Reads the content of a blob
    Source: https://googleapis.dev/python/storage/latest/index.html
    Args:
        bucket_name (str): Bucket name
        path (str): Path to the object to read from (**WITHOUT** leading `/`)

    Returns:
        Content of the object
    """
    # cleaning leading '/' from path
    path = path.lstrip('/')
    # removing '/' affixes from bucket name
    bucket_name = bucket_name.strip('/')
    # logic
    gcs_uri = f'gs://{bucket_name}/{path}'
    logging.info('Reading <%s>', gcs_uri)
    try:
        bucket = _bucket(bucket_name)
        blob = bucket.get_blob(path)
        result = blob.download_as_bytes()
    except Exception as err:
        msg = f'Could not download content from <{gcs_uri}>. Error: {err}'
        logging.critical(msg)
        raise CloudStorageDownloadError(msg) from err
    return result


@cachetools.cached(cache=cachetools.LRUCache(maxsize=1))
def _client() -> storage.Client:
    return storage.Client()


def _bucket(bucket_name: str) -> storage.Bucket:
    return _client().get_bucket(bucket_name)


def _accept_all_list_objects(value: str) -> bool:  # pylint: disable=unused-argument
    return True


def list_objects(
    bucket_name: str, filter_fn: Optional[Callable[[str], bool]] = None
) -> Generator[str, None, None]:
    # pylint: disable=line-too-long
    """
    This function is just wrapper around GSC client `list_blobs`_ method.
    What it adds is the possibility to filter out the results using `filter_fn` argument.
    Example::
        my_bucket/
          object_a
          object_b
          folder_a/
            object_c
            folder_b/
              object_d

    If there are no filters the results will be equivalent of::
        result = []
        result.append('folder_a/')
        result.append('folder_a/folder_b/')
        result.append('folder_a/folder_b/object_d')
        result.append('folder_a/object_c')
        result.append('object_a')
        result.append('object_b')

    If you want only the objects, a `filter_fn` could be::

        def filter_fn(value: str) -> bool:
            return not value.endswith('/')

    This will be equivalent to::
        result = []
        result.append('folder_a/folder_b/object_d')
        result.append('folder_a/object_c')
        result.append('object_a')
        result.append('object_b')

    .. list_blobs: https://googleapis.dev/python/storage/latest/client.html#google.cloud.storage.client.Client.list_blobs
    :param bucket_name:
    :param filter_fn:
    :return:
    """
    # pylint: enable=line-too-long
    # if no filter, accept all
    if filter_fn is None:
        filter_fn = _accept_all_list_objects
    for obj_path in _list_blob_names(bucket_name):
        try:
            if filter_fn(obj_path):
                yield obj_path
        except Exception as err:
            msg = (
                f'Could not add blob named <{obj_path}> from bucket <{bucket_name}>. '
                f'Stopping list now. Error: <{err}>'
            )
            logging.critical(msg)
            raise CloudStorageListError(msg) from err


def _list_blob_names(bucket_name: str) -> Generator[str, None, None]:
    for blob in _client().list_blobs(bucket_name):
        yield blob.name
