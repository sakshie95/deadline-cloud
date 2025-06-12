# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import time
import threading
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
from deadline.job_attachments.download import (
    get_output_manifests_by_asset_root,
    merge_asset_manifests,
)
from deadline.job_attachments.api.attachment import _attachment_download_with_root_manifests
import boto3
from deadline.job_attachments.models import JobAttachmentS3Settings, FileConflictResolution
from deadline.client.api._session import _get_queue_user_boto3_session
from deadline.job_attachments.incremental_downloads.session_action_processor import (
    SessionActionMapping,
)
from typing import List, Optional, Dict, Callable
from deadline.job_attachments.asset_manifests.base_manifest import BaseAssetManifest


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
    Optimized with parallel processing by session.

    :param file_conflict_resolution: Method of file conflict resolution
    :param boto3_session: The boto3 session to use for API calls
    :param session_action_mappings: List of SessionActionMapping objects
    :param path_mapping_rules: Optional path mapping rules for downloads
    :param print_function_callback: Function for logging messages
    :param farm_id: The farm ID
    :param queue_id: The queue ID
    :return: Returns list of downloaded session action ids.
    """
    # Start timing
    start_time = time.time()

    # Get queue for assuming queue role session to access JA bucket
    queue_start_time = time.time()
    deadline = boto3_session.client("deadline")
    queue = deadline.get_queue(farmId=farm_id, queueId=queue_id)
    queue_role_session: boto3.Session = _get_queue_user_boto3_session(
        deadline=deadline,
        base_session=boto3_session,
        farm_id=farm_id,
        queue_id=queue_id,
        queue_display_name=queue["displayName"],
    )
    queue_time = time.time() - queue_start_time
    print_function_callback(f"Getting queue and role session took {queue_time:.2f} seconds")

    # Get job attachment settings once
    ja_settings = JobAttachmentS3Settings(**queue["jobAttachmentSettings"])
    s3_root_uri = ja_settings.to_s3_root_uri()

    # Group session actions by session ID for more efficient processing
    # Extract session ID from session action ID (e.g., "sessionaction-12345-1" -> "session-12345")
    session_to_actions: Dict[str, List[SessionActionMapping]] = {}

    for mapping in session_action_mappings:
        session_action_id = mapping.session_action_id
        parts = session_action_id.split("-")
        if len(parts) >= 3:
            # Convert "sessionaction-12345-1" to "session-12345"
            if parts[0] == "sessionaction":
                parts[0] = "session"
                session_id = "-".join(parts[:-1])  # Remove the last part (action number)

                if session_id not in session_to_actions:
                    session_to_actions[session_id] = []
                session_to_actions[session_id].append(mapping)

    # Create a thread-safe list for downloaded session action IDs
    downloaded_session_action_ids = []
    download_lock = threading.Lock()

    # Function to process all session actions for a single session
    def process_session(session_id, mappings):
        session_downloaded_ids = []

        # Collect all output manifests for this session's actions
        all_output_manifests_by_root: Dict[str, List[BaseAssetManifest]] = {}

        for mapping in mappings:
            session_action_id = mapping.session_action_id
            job_id = mapping.job_id

            try:
                # Get output manifests
                output_manifests_by_root = get_output_manifests_by_asset_root(
                    s3_settings=ja_settings,
                    farm_id=farm_id,
                    queue_id=queue_id,
                    job_id=job_id,
                    session_action_id=session_action_id,
                    task_id=mapping.task_id,
                    step_id=mapping.step_id,
                    session=queue_role_session,
                )

                # If no output manifests, mark as downloaded and continue
                if not output_manifests_by_root:
                    session_downloaded_ids.append(session_action_id)
                    continue

                # Merge with existing manifests
                for root, manifests in output_manifests_by_root.items():
                    if root not in all_output_manifests_by_root:
                        all_output_manifests_by_root[root] = []
                    all_output_manifests_by_root[root].extend(manifests)

                # Mark as processed
                session_downloaded_ids.append(session_action_id)

            except Exception as e:
                print_function_callback(
                    f"Error getting manifests for session action {session_action_id}: {str(e)}"
                )

        # Merge all manifests for each root
        merged_output_manifests = {}
        for root, manifests in all_output_manifests_by_root.items():
            merged_manifest = merge_asset_manifests(manifests)
            if merged_manifest:
                merged_output_manifests[root] = merged_manifest

        # If we have merged manifests, download them
        if merged_output_manifests:
            print_function_callback(f"Found output paths for session {session_id}")

            try:
                # Download attachments
                download_start_time = time.time()
                _attachment_download_with_root_manifests(
                    boto3_session=queue_role_session,
                    manifests_by_root=merged_output_manifests,
                    s3_root_uri=s3_root_uri,
                    path_mapping_rules=path_mapping_rules,
                    conflict_resolution=file_conflict_resolution,
                )
                download_time = time.time() - download_start_time
                print_function_callback(
                    f"Downloading attachments for session {session_id} took {download_time:.2f} seconds"
                )
            except Exception as e:
                print_function_callback(
                    f"Error downloading attachments for session {session_id}: {str(e)}"
                )

        # Add all downloaded IDs to the global list with proper locking
        with download_lock:
            downloaded_session_action_ids.extend(session_downloaded_ids)

    # Process sessions in parallel
    parallel_start_time = time.time()
    with ThreadPoolExecutor(max_workers=5) as executor:
        # Submit all sessions to the executor
        futures = [
            executor.submit(process_session, session_id, mappings)
            for session_id, mappings in session_to_actions.items()
        ]

        # Wait for all sessions to complete
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()  # This will raise any exceptions from the thread
            except Exception as e:
                print_function_callback(f"Error processing session: {str(e)}")

    parallel_time = time.time() - parallel_start_time
    print_function_callback(f"Parallel processing of all sessions took {parallel_time:.2f} seconds")

    # End timing and print the elapsed time
    end_time = time.time()
    elapsed_time = end_time - start_time
    print_function_callback(
        f"Time taken to aggregate manifests and download outputs: {elapsed_time:.2f} seconds"
    )

    return downloaded_session_action_ids
