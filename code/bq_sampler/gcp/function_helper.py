# vim: ai:sw=4:ts=4:sta:et:fo=croql
"""
GCP CloudFunction helper functions
"""
import urllib.request
from typing import Tuple

import cachetools

import google.auth
import google.oauth2.credentials

from bq_sampler import logger

_LOGGER = logger.get(__name__)

_PROJECT_ID_METADATA_URL: str = (
    'http://metadata.google.internal/computeMetadata/v1/project/project-id'
)


@cachetools.cached(cache=cachetools.LRUCache(maxsize=1))
def project_id() -> str:
    # pylint: disable=line-too-long
    """
    Source: https://stackoverflow.com/questions/65088076/trying-to-find-the-current-project-id-of-the-deployed-python-function-in-google
    :return:
    """
    # pylint: enable=line-too-long
    try:
        req = urllib.request.Request(_PROJECT_ID_METADATA_URL)
        req.add_header("Metadata-Flavor", "Google")
        result = urllib.request.urlopen(req).read().decode()  # pylint: disable=consider-using-with
    except Exception as err:  # pylint: disable=broad-except
        _LOGGER.error(
            'Could not retrieve Project ID from URL: <%s>.'
            ' Trying with default credentials.'
            ' Error: %s',
            _PROJECT_ID_METADATA_URL,
            err,
        )
        _, result = gcp_default_credentials()
    return result


def gcp_default_credentials() -> Tuple[google.oauth2.credentials.Credentials, str]:
    """
    Retrieves the default GCP credentials with the project ID as well.
    :return:
    """
    try:
        result = google.auth.default()
    except Exception as err:
        raise RuntimeError(f'Could not retrieve GCP\'s default credentials. Error: {err}') from err
    return result
