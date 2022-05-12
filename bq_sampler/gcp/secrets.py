# vim: ai:sw=4:ts=4:sta:et:fo=croql
# pylint: disable=line-too-long
"""
GCP `Secret Manager`_ entry point
.. _Secret Manager: https://cloud.google.com/secret-manager/docs/quickstart#secretmanager-quickstart-python
"""
# pylint: enable=line-too-long
from typing import Optional

import cachetools

from google.cloud import secretmanager

from bq_sampler.gcp import function_helper
from bq_sampler import logger

_LOGGER = logger.get(__name__)

_GCP_SECRET_NAME_TMPL: str = 'projects/{project_id}/secrets/{secret_id}/versions/{version}'
_DEFAULT_GCP_SECRET_VERSION: str = 'latest'


class SecretManagerAccessError(Exception):
    """To code all Secret Manager errors"""


def secret(
    secret_id: str, *, project_id: Optional[str] = None, version: Optional[str] = None
) -> str:
    """
    Retrieves a secret, by name.

    :param secret_id: Which secret to retrieve
    :param project_id: Project ID where the secret is stored
    :param version: Which version to use, if :py:obj:`None` given,
        uses :py:data:`_DEFAULT_GCP_SECRET_VERSION`
    :return:
    """
    if version is None:
        version = _DEFAULT_GCP_SECRET_VERSION
    if project_id is None:
        project_id = function_helper.project_id()
    secret_name = _GCP_SECRET_NAME_TMPL.format(
        project_id=project_id, secret_id=secret_id, version=version
    )
    _LOGGER.info('Retrieving secret <%s>', secret_name)
    try:
        response = _client().access_secret_version(request={'name': secret_name})
    except Exception as err:
        raise SecretManagerAccessError(
            f'Could not retrieve secret <{secret_name}>. Error: {err}'
        ) from err
    return response.payload.data.decode('UTF-8')


@cachetools.cached(cache=cachetools.LRUCache(maxsize=1))
def _client() -> secretmanager.SecretManagerServiceClient:
    return secretmanager.SecretManagerServiceClient()
