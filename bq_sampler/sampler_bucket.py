# vim: ai:sw=4:ts=4:sta:et:fo=croql
"""
List all project IDs that should be sampled. It assumes the following structure in GCS bucket::
  /
    default_policy.json - the default and assumed always right JSON file
    <PROJECT_ID>/
      <DATASET_ID>/
        <TABLE_ID>.json - contains the specific policy for this specific
                          table that overwrites the default, if valid.

"""
from typing import Any, Callable, Generator, Tuple

from bq_sampler import const
from bq_sampler.gcp import gcs
from bq_sampler.entity import policy
from bq_sampler.entity import table
from bq_sampler import logger

_LOGGER = logger.get(__name__)


def all_policies(
    bucket_name: str, default_policy_object_path: str
) -> Generator[policy.TablePolicy, None, None]:
    """
    The output is already containing the realized policies, i.e.,
    merged with the default policy, if needed.

    :param bucket_name:
    :param default_policy_object_path:
    :return:
    """
    _LOGGER.info(
        'Retrieving all policies from bucket <%s> and using default policy from <%s>',
        bucket_name,
        default_policy_object_path,
    )
    # default policy
    default_policy = _default_policy(bucket_name, default_policy_object_path)
    # all policies
    for table_policy in _retrieve_all_table_policies(bucket_name, default_policy):
        yield table_policy


def _default_policy(bucket_name: str, default_policy_object_path: str):
    result = _overwritten_policy_from_gcs(
        bucket_name, default_policy_object_path, policy.FALLBACK_GENERIC_POLICY
    )
    _LOGGER.info(
        'Default policy read from gs://%s/%s with: %s',
        bucket_name,
        default_policy_object_path,
        result,
    )
    return result


def _overwritten_policy_from_gcs(
    bucket_name: str, policy_object_path: str, fallback_policy: policy.Policy
) -> policy.Policy:
    policy_json_string: str = _fetch_gcs_object_as_string(bucket_name, policy_object_path)
    result = _overwrite_policy(policy.Policy.from_json(policy_json_string), fallback_policy)
    return result


def _fetch_gcs_object_as_string(bucket_name: str, object_path: str) -> str:
    result = None
    try:
        content = gcs.read_object(bucket_name, object_path)
        if content is not None:
            result = content.decode('utf-8')
        else:
            _LOGGER.warning(
                'No content to decode for bucket %s and object %s', bucket_name, object_path
            )
    except Exception as err:  # pylint: disable=broad-except
        _LOGGER.warning(
            'Could not load content as string from <%s> in bucket <%s>. Ignoring. Error: %s',
            object_path,
            bucket_name,
            err,
        )
    return result


def _overwrite_policy(
    specific_policy: policy.Policy, fallback_policy: policy.Policy
) -> policy.Policy:
    """
    So CLI can use the exact same strategy.

    :param specific_policy:
    :param fallback_policy:
    :return:
    """
    return specific_policy.patch_with(fallback_policy)


def _retrieve_all_table_policies(
    bucket_name: str, default_policy: policy.Policy
) -> Generator[policy.TablePolicy, None, None]:
    def convert_fn(table_reference, obj_path) -> policy.TablePolicy:
        actual_policy = _overwritten_policy_from_gcs(bucket_name, obj_path, default_policy)
        result = policy.TablePolicy(table_reference=table_reference, policy=actual_policy)
        return result

    for table_policy in _retrieve_all_with_table_reference(bucket_name, convert_fn):
        yield table_policy


def _retrieve_all_with_table_reference(
    bucket_name: str, convert_fn: Callable[[table.TableReference, str], Any]
) -> Generator[Any, None, None]:

    for table_reference, obj_path in _list_all_table_references_obj_path(bucket_name):
        yield convert_fn(table_reference, obj_path)


def _list_all_table_references_obj_path(
    bucket_name: str,
) -> Generator[Tuple[table.TableReference, str], None, None]:
    def filter_fn(value: str) -> bool:
        return value.endswith(const.JSON_EXT) and len(value.split('/')) == 3

    for obj_path in gcs.list_objects(bucket_name, filter_fn):
        project_id, dataset_id, table_id_file = obj_path.split('/')
        table_id = table_id_file[: -len(const.JSON_EXT)]
        table_reference = table.TableReference(
            project_id=project_id, dataset_id=dataset_id, table_id=table_id
        )
        yield table_reference, obj_path


def all_sample_requests(bucket_name: str) -> Generator[table.TableSample, None, None]:
    """
    The output is already containing the requested samples

    :param bucket_name:
    :return:
    """
    _LOGGER.info(
        'Retrieving all sample requests from bucket <%s>',
        bucket_name,
    )
    # all requests
    for request in _retrieve_all_sample_requests(bucket_name):
        yield request


def _retrieve_all_sample_requests(bucket_name: str) -> Generator[table.TableSample, None, None]:
    def convert_fn(table_reference, obj_path) -> table.TableSample:
        table_sample = _sample_request(bucket_name, obj_path)
        result = table.TableSample(table_reference=table_reference, sample=table_sample)
        return result

    for request in _retrieve_all_with_table_reference(bucket_name, convert_fn):
        yield request


def _sample_request(bucket_name: str, request_filename: str) -> table.Sample:
    sample_json_string: str = _fetch_gcs_object_as_string(bucket_name, request_filename)
    result = table.Sample.from_json(sample_json_string)
    return result


def sample_request_from_policy(
    bucket_name: str, table_policy: policy.TablePolicy
) -> table.TableSample:
    """
    For a given :py:class:`policy.TablePolicy` create a corresponding
    :py:class:`sample.TableSample`, where the sample is overwritten, if necessary,
    with the policy default sample.

    :param bucket_name:
    :param table_policy:
    :return:
    """
    # get overwritten request with policy default sample
    req_sample = _sample_request(bucket_name, _json_object_path(table_policy.table_reference))
    effective_sample = _overwrite_request(req_sample, table_policy.policy)
    result = table.TableSample(
        table_reference=table_policy.table_reference, sample=effective_sample
    )
    return result


def _overwrite_request(request: table.Sample, request_policy: policy.Policy) -> table.Sample:
    return request.patch_with(request_policy.default_sample)


def _json_object_path(table_reference: table.TableReference) -> str:
    return '/'.join(
        [
            table_reference.project_id,
            table_reference.dataset_id,
            table_reference.table_id,
            const.JSON_EXT,
        ]
    )
