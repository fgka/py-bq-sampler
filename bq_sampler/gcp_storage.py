# vim: ai:sw=4:ts=4:sta:et:fo=croql
# pylint: disable=line-too-long
"""
Reads an object from `Cloud Storage`_.

.. Cloud Storage: https://cloud.google.com/appengine/docs/standard/python/googlecloudstorageclient/read-write-to-cloud-storage
"""
# pylint: enable=line-too-long
import logging
from typing import List

import cachetools

from google.cloud import storage


class CloudStorageDownloadError(Exception):
    """To code all GCS errors"""


def read_object(bucket_name: str, path: str) -> bytes:
    """Reads the content of a blob
    Source: https://googleapis.dev/python/storage/latest/index.html
    Args:
        bucket_name (str): Bucket name
        path (str): Path to the object to read from (**WITHOUT** leading `/`)

    Returns:
        Content of the object
    """
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


def list_objects(bucket_name: str, path: str) -> List[str]:
    # TODO
    return None
