# vim: ai:sw=4:ts=4:sta:et:fo=croql
# pylint: disable=line-too-long
"""
Reads an object from `Cloud Storage`_.

.. Cloud Storage: https://cloud.google.com/appengine/docs/standard/python/googlecloudstorageclient/read-write-to-cloud-storage
"""
import logging

# pylint: enable=line-too-long
import re
from typing import Any, Callable, Dict, Generator, Optional, Tuple

import cachetools

from google.api_core import page_iterator
from google.cloud import storage

from bq_sampler import const, logger

_LOGGER = logger.get(__name__)

_GS_URI_REGEX: str = 'gs://(.*?)/(.*)'
_GCS_PAGE_ITERATOR_PREFIXES_ITEMS_KEY: str = 'prefixes'


class CloudStorageDownloadError(Exception):
    """To code all GCS download errors"""


class CloudStorageListError(Exception):
    """To code all GCS list errors"""


def bucket_path_from_uri(value: str) -> Tuple[str, str]:
    """
    Converts a URI string into its bucket and path components.

    :param value: Something like `'gs://bucket/path/to/object'`
    :return: Something like: `('bucket', 'path/to/object')`
    """
    if not isinstance(value, str):
        raise TypeError(f'Given value is not a string. Got: <{value}>({type(value)})')
    gs_match = re.match(_GS_URI_REGEX, value)
    if not isinstance(gs_match, re.Match) or len(gs_match.groups()) != 2:
        raise ValueError(f'The value must specify a Cloud Storage URI. Got: <{value}>')
    return gs_match.groups()


def read_object(bucket_name: str, path: str, warn_read_failure: Optional[bool] = True) -> bytes:
    """
    Reads the content of a blob.

    :param bucket_name: Bucket name
    :param path: Path to the object to read from (**WITHOUT** leading `/`)
    :param warn_read_failure: if :py:obj:`True` will warn about failure to read,
        if :py:obj:`False` will just inform about it.
    :return: Content of the object
    """
    # cleaning leading '/' from path
    path = path.lstrip('/')
    # removing '/' affixes from bucket name
    bucket_name = bucket_name.strip('/')
    # logic
    gcs_uri = f'gs://{bucket_name}/{path}'
    _LOGGER.debug('Reading <%s>', gcs_uri)
    try:
        bucket = _bucket(bucket_name)
        blob = bucket.get_blob(path)
        if blob is not None:
            result = blob.download_as_bytes()
            _LOGGER.debug('Read <%s>', gcs_uri)
        else:
            result = None
            _LOGGER.log(
                logging.WARN if warn_read_failure else logging.INFO,
                'Object %s does not exist or does not contain data. Returning %s',
                gcs_uri,
                result,
            )
    except Exception as err:
        raise CloudStorageDownloadError(
            f'Could not download content from <{gcs_uri}>. Error: {err}'
        ) from err
    return result


@cachetools.cached(cache=cachetools.LRUCache(maxsize=1))
def _client() -> storage.Client:
    return storage.Client()


def _bucket(bucket_name: str) -> storage.Bucket:
    return _client().get_bucket(bucket_name)


def list_prefixes(
    bucket_name: str,
    prefix: Optional[str] = None,
    filter_fn: Optional[Callable[[str], bool]] = None,
) -> Generator[str, None, None]:
    """
    Source: https://stackoverflow.com/questions/37074977/how-to-get-list-of-folders-in-a-given-bucket-using-google-cloud-api/59008580#59008580

    Example::
        my_bucket/
          object_a
          object_b
          folder_a/
            object_c
            folder_b/
              object_d
          folder_c/
            object_e
            folder_d/
                folder_e/
                    object_f
                    folder_f/
                        object_g
        folder_g/
            object_h

        result = list_prefixes("my_bucket")
        result = [
            "folder_a/",
            "folder_a/folder_b/",
            "folder_c/",
            "folder_c/folder_d/",
            "folder_c/folder_d/folder_e/",
            "folder_c/folder_d/folder_e/folder_f/",
            "folder_g/",
        ]

    :param bucket_name:
    :param prefix: if given, list from this value. Default: py:obj:`None`.
    :param filter_fn:
    :return:
    """
    # validate
    prefix = _get_list_prefixes_root_prefix(prefix)
    _LOGGER.info("Listing prefixes from gs://%s/%s", bucket_name, prefix)
    # if no filter, accept all
    if filter_fn is None:
        filter_fn = _accept_all_list_objects
    # logic
    for item in _list_prefixes_ok(bucket_name, prefix, filter_fn):
        yield item


def _get_list_prefixes_root_prefix(prefix: Optional[str] = None) -> str:
    result = prefix
    if not isinstance(prefix, str):
        result = ""
    elif prefix and not prefix.endswith(const.GS_PREFIX_DELIM):
        result = prefix + const.GS_PREFIX_DELIM
    return result


def _accept_all_list_objects(value: str) -> bool:  # pylint: disable=unused-argument
    return True


def _list_prefixes_ok(
    bucket_name: str,
    prefix: Optional[str] = None,
    filter_fn: Optional[Callable[[str], bool]] = None,
) -> Generator[str, None, None]:
    # logic
    for item in _get_gcs_prefixes_http_iterator(bucket_name, prefix):
        _LOGGER.debug("Found prefix <%s> in <%s> in bucket <%s>", item, prefix, bucket_name)
        try:
            if filter_fn(item):
                yield item
        except Exception as err:
            raise CloudStorageListError(
                f'Could not add prefix named <{item}> in <{prefix}> from bucket <{bucket_name}>. '
                f'Stopping list now. Error: <{err}>'
            ) from err
        # recursion
        for sub_item in _list_prefixes_ok(bucket_name, item, filter_fn):
            yield sub_item


def _get_gcs_prefixes_http_iterator(bucket_name: str, prefix: str) -> page_iterator.HTTPIterator:
    client = _client()
    return page_iterator.HTTPIterator(
        client=client,
        api_request=client._connection.api_request,
        path=_gcs_http_iterator_bucket_path(bucket_name),
        items_key=_GCS_PAGE_ITERATOR_PREFIXES_ITEMS_KEY,
        item_to_value=_list_prefixes_item_to_value,
        extra_params=_list_prefixes_extra_params(prefix),
    )


def _gcs_http_iterator_bucket_path(bucket_name: str) -> str:
    return f"/b/{bucket_name}/o"


def _list_prefixes_item_to_value(iterator: page_iterator.HTTPIterator, item: Any) -> str:
    return item


def _list_prefixes_extra_params(prefix: str) -> Dict[str, Any]:
    return {"projection": "noAcl", "prefix": prefix, "delimiter": const.GS_PREFIX_DELIM}


def list_objects(
    bucket_name: str,
    filter_fn: Optional[Callable[[str], bool]] = None,
    prefix: Optional[str] = None,
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
    :param prefix: limits the search by prefix
    :return:
    """
    # pylint: enable=line-too-long
    # if no filter, accept all
    if filter_fn is None:
        filter_fn = _accept_all_list_objects
    for obj_path in _list_blob_names(bucket_name, prefix):
        try:
            if filter_fn(obj_path):
                yield obj_path
        except Exception as err:
            raise CloudStorageListError(
                f'Could not add blob named <{obj_path}> from bucket <{bucket_name}>. '
                f'Stopping list now. Error: <{err}>'
            ) from err


def _list_blob_names(bucket_name: str, prefix: Optional[str] = None) -> Generator[str, None, None]:
    for blob in _client().list_blobs(bucket_name, prefix=prefix):
        yield blob.name


if __name__ == '__main__':
    bucket = "test-list-blobs"
    entries = list(list_prefixes(bucket))
    for x in entries:
        print(x)
