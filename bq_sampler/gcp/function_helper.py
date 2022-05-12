# vim: ai:sw=4:ts=4:sta:et:fo=croql
"""
GCP CloudFunction helper functions
"""
import urllib.request

import cachetools

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
    req = urllib.request.Request(_PROJECT_ID_METADATA_URL)
    req.add_header("Metadata-Flavor", "Google")
    return urllib.request.urlopen(req).read().decode()
