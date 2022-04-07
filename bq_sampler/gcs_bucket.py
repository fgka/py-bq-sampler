# vim: ai:sw=4:ts=4:sta:et:fo=croql
"""
List all project IDs that should be sampled. It assumes the following structure in GCS bucket::
  /
    sampling-policy.json - the generic and assumed always right JSON file
    <PROJECT_ID>/
      <DATASET_ID>/
        <TABLE_ID>.json - contains the specific policy for this specific
                          table that overwrites the generic, if valid.

"""
import logging
from typing import Any, Callable, Dict, List

import json

from bq_sampler import gcp_storage
from bq_sampler.dto import policy
from bq_sampler.dto import sample


_JSON_EXT: str = ".json"


def get_all_policies(bucket_name: str, generic_policy_filename: str) -> List[policy.TablePolicy]:
    """
    The output is already containing the realized policies, i.e.,
    merged with the generic policy, if needed.

    :param bucket_name:
    :param generic_policy_filename:
    :return:
    """
    logging.info(
        'Retrieving all policies from bucket <%s> and using generic policy from <%s>',
        bucket_name,
        generic_policy_filename,
    )
    # generic policy
    generic_policy = _get_overwriten_policy(
        bucket_name, generic_policy_filename, policy.FALLBACK_GENERIC_POLICY
    )
    logging.info(
        'Generic policy read from gs://%s/%s with: %s',
        bucket_name,
        generic_policy_filename,
        generic_policy,
    )
    # all policies
    result = _retrieve_all_table_policies(bucket_name, generic_policy)
    return result


def _get_overwriten_policy(
    bucket_name: str, policy_filename: str, fallback_policy: policy.Policy
) -> policy.Policy:
    policy_dict: Dict[str, Any] = _fetch_gcs_object_as_dict(bucket_name, policy_filename)
    result = policy.Policy.from_dict(policy_dict)
    if result:
        result = result.overwrite_with(fallback_policy)
    return result


def _fetch_gcs_object_as_dict(bucket_name: str, object_path: str) -> Dict[str, Any]:
    result = None
    try:
        content = gcp_storage.read_object(bucket_name, object_path)
        result = json.loads(content.decode('utf-8'))
    except Exception as err:  # pylint: disable=broad-except
        logging.warning(
            'Could not load JSON content from <%s> in bucket <%s>. Ignoring. Error: %s',
            object_path,
            bucket_name,
            err,
        )
    return result


def _retrieve_all_table_policies(
    bucket_name: str, generic_policy: policy.Policy
) -> List[policy.TablePolicy]:
    def convert_fn(table_reference, obj_path) -> policy.TablePolicy:
        actual_policy = _get_overwriten_policy(bucket_name, obj_path, generic_policy)
        result = policy.TablePolicy(table_reference=table_reference, policy=actual_policy)
        return result

    return _retrieve_all_with_table_reference(bucket_name, convert_fn)


def _retrieve_all_with_table_reference(
    bucket_name: str, convert_fn: Callable[[sample.TableReference, str], Any]
) -> List[Any]:
    result = []

    def filter_fn(value: str) -> bool:
        return value.endswith(_JSON_EXT) and len(value.split('/')) == 3

    for obj_path in gcp_storage.list_objects(bucket_name, filter_fn):
        project_id, dataset_id, table_id_file = obj_path.split('/')
        table_id = table_id_file.rstrip(_JSON_EXT)
        table_reference = sample.TableReference(
            project_id=project_id, dataset_id=dataset_id, table_id=table_id
        )
        result.append(convert_fn(table_reference, obj_path))
    return result


def get_all_sample_requests(bucket_name: str) -> List[sample.TableSample]:
    """
    The output is already containing the requested samples

    :param bucket_name:
    :return:
    """
    logging.info(
        'Retrieving all sample requests from bucket <%s>',
        bucket_name,
    )
    # all policies
    result = _retrieve_all_sample_requests(bucket_name)
    return result


def _retrieve_all_sample_requests(bucket_name: str) -> List[sample.TableSample]:
    def convert_fn(table_reference, obj_path) -> sample.TableSample:
        table_sample = _get_sample_request(bucket_name, obj_path)
        result = sample.TableSample(table_reference=table_reference, sample=table_sample)
        return result

    return _retrieve_all_with_table_reference(bucket_name, convert_fn)


def _get_sample_request(bucket_name: str, request_filename: str) -> policy.Policy:
    sample_dict: Dict[str, Any] = _fetch_gcs_object_as_dict(bucket_name, request_filename)
    result = sample.Sample.from_dict(sample_dict)
    return result


def get_sample_request_from_policy(
    bucket_name: str, table_policy: policy.TablePolicy
) -> sample.TableSample:
    """
    For a given :py:class:`policy.TablePolicy` create a corresponding
    :py:class:`sample.TableSample`, where the sample is overwritten, if necessary,
    with the policy default sample.

    :param bucket_name:
    :param table_policy:
    :return:
    """
    # get overwritten request with policy default sample
    req_sample = _get_sample_request(bucket_name, _json_object_path(table_policy.table_reference))
    harmonized_sample = req_sample.overwrite_with(table_policy.policy.default_sample)
    result = sample.TableSample(
        table_reference=table_policy.table_reference, sample=harmonized_sample
    )
    return result


def _json_object_path(table_reference: sample.TableReference) -> str:
    return '/'.join(
        [
            table_reference.project_id,
            table_reference.dataset_id,
            table_reference.table_id,
            _JSON_EXT,
        ]
    )
