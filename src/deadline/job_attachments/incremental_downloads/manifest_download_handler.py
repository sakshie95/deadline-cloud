# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from deadline.job_attachments.download import (
    get_output_manifests_by_asset_root,
    merge_asset_manifests,
)
from deadline.job_attachments.api.attachment import _attachment_download_with_root_manifests
import boto3
from deadline.job_attachments.models import JobAttachmentS3Settings, FileConflictResolution
from deadline.client.api._session import _get_queue_user_boto3_session
from deadline.job_attachments.asset_manifests.base_manifest import BaseAssetManifest
from deadline.job_attachments.incremental_downloads.session_action_processor import (
    SessionActionMapping,
)
from typing import List, Optional, Dict, Callable


def aggregate_manifest_and_download_outputs(
    boto3_session: boto3.Session,
    session_action_mappings: List[SessionActionMapping],
    farm_id: str,
    queue_id: str,
    file_conflict_resolution: FileConflictResolution,
    path_mapping_rules: Optional[str] = None,
    print_function_callback: Callable[[str], None] = lambda msg: None,
) -> list[str]:
    """
    Aggregate manifests and download outputs for the given session actions.

    :param file_conflict_resolution: Method of file conflict resolution
    :param boto3_session: The boto3 session to use for API calls
    :param session_action_mappings: List of SessionActionMapping objects
    :param path_mapping_rules: Optional path mapping rules for downloads
    :param print_function_callback: Function for logging messages
    :param farm_id: The farm ID
    :param queue_id: The queue ID
    :return: Returns list of downloaded session action ids.
    """
    # Get queue for assuming queue role session to access JA bucket
    deadline = boto3_session.client("deadline")
    queue = deadline.get_queue(farmId=farm_id, queueId=queue_id)
    queue_role_session: boto3.Session = _get_queue_user_boto3_session(
        deadline=deadline,
        base_session=boto3_session,
        farm_id=farm_id,
        queue_id=queue_id,
        queue_display_name=queue["displayName"],
    )

    downloaded_session_action_ids = []

    # Group session actions by job ID for more efficient processing
    job_to_actions: Dict[str, List[SessionActionMapping]] = {}
    for mapping in session_action_mappings:
        if mapping.job_id not in job_to_actions:
            job_to_actions[mapping.job_id] = []
        job_to_actions[mapping.job_id].append(mapping)

    # Process each job's session actions
    for job_id, mappings in job_to_actions.items():
        print_function_callback(f"Processing job {job_id} with {len(mappings)} session actions")

        for mapping in mappings:
            session_action_id = mapping.session_action_id
            print_function_callback(
                f"Processing session action {session_action_id} (Step: {mapping.step_id}, Task: {mapping.task_id})"
            )

            output_manifests_by_root: Dict[str, List[BaseAssetManifest]] = (
                get_output_manifests_by_asset_root(
                    s3_settings=JobAttachmentS3Settings(**queue["jobAttachmentSettings"]),
                    farm_id=farm_id,
                    queue_id=queue_id,
                    job_id=mapping.job_id,
                    session_action_id=session_action_id,
                    task_id=mapping.task_id,
                    step_id=mapping.step_id,
                    session=queue_role_session,
                )
            )

            merged_output_manifests: Dict[str, BaseAssetManifest] = {}
            for root in output_manifests_by_root.keys():
                merged_output_manifest = merge_asset_manifests(output_manifests_by_root[root])
                if merged_output_manifest:
                    merged_output_manifests[root] = merged_output_manifest

            # If no output paths were found, log a message and continue to next session action.
            if merged_output_manifests == {}:
                print_function_callback(
                    f"Found no output paths for job {job_id}, session action {session_action_id}"
                )
                continue

            print_function_callback(
                f"Found output paths for job {job_id}, session action {session_action_id}: {merged_output_manifests}"
            )

            # Download attachments using merged output manifests
            _attachment_download_with_root_manifests(
                boto3_session=queue_role_session,
                manifests_by_root=merged_output_manifests,
                s3_root_uri=JobAttachmentS3Settings(
                    **queue["jobAttachmentSettings"]
                ).to_s3_root_uri(),
                path_mapping_rules=path_mapping_rules,
                conflict_resolution=file_conflict_resolution,
            )

            # Add the session action ID to the list of downloaded session action IDs
            downloaded_session_action_ids.append(session_action_id)

    return downloaded_session_action_ids
