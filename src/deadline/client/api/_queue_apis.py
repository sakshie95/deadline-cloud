# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
from __future__ import annotations

__all__ = ["_incremental_output_download"]

from .. import api
from typing import Optional, Callable, Set
import boto3
from deadline.client import _pid_utils
from deadline.job_attachments.incremental_downloads.incremental_download_state import (
    IncrementalDownloadState,
    bootstrap_fresh_state,
    load_progress_from_state_file,
    save_progress_to_state_file,
)
import datetime

import os
from deadline.job_attachments.incremental_downloads.exceptions import PidLockAlreadyHeld
from deadline.job_attachments.incremental_downloads.job_processor import (
    get_list_of_ongoing_jobs_on_queue,
)

PID_FILE_NAME = "incremental_output_download.pid"
DOWNLOAD_PROGRESS_FILE_NAME = "download_progress.json"


@api.record_function_latency_telemetry_event()
def _incremental_output_download(
    farm_id: str,
    queue_id: str,
    boto3_session: boto3.Session,
    saved_progress_checkpoint_location: str,
    bootstrap_lookback_in_minutes: Optional[int] = 0,
    force_bootstrap: bool = False,
    path_mapping_rules: Optional[str] = None,
    print_function_callback: Callable[[str], None] = lambda msg: None,
) -> None:
    """
    Download Job Output data incrementally for all jobs running on a queue as session actions finish.
    The command bootstraps once using a bootstrap lookback specified in minutes and
    continues downloading from the last saved progress thereafter until bootstrap is forced

    :param farm_id: farm id for the output download
    :param queue_id: queue for scoping output download
    :param bootstrap_lookback_in_minutes: Downloads outputs for job-session-actions that have been completed
    since these many minutes at bootstrap. Default value is 0 minutes.
    :param saved_progress_checkpoint_location: location of the download progress file
    :param force_bootstrap: force bootstrap and ignore current download progress. Default value is False.
    :param path_mapping_rules: path mapping rules for cross OS path mapping
    :param boto3_session: boto3 session
    :param print_function_callback: Callback to print messages produced in this function.
                Used in the CLI to print to stdout using click.echo. By default, ignores messages.
    :return: None
    """
    # 1. Construct pid file full path
    pid_file_full_path = os.path.join(
        saved_progress_checkpoint_location, f"{queue_id}_{PID_FILE_NAME}"
    )

    try:
        # 2. Check if a download is already ongoing with pid lock checking mechanism
        _pid_utils.try_acquire_pid_lock(pid_file_full_path, print_function_callback)

        # Construct the saved progress checkpoint full path
        saved_progress_checkpoint_full_path: str = os.path.join(
            saved_progress_checkpoint_location, f"{queue_id}_{DOWNLOAD_PROGRESS_FILE_NAME}"
        )
        current_download_progress: IncrementalDownloadState = IncrementalDownloadState()

        # 3. If bootstrap is required, then bootstrap using bootstrap_lookback_in_minutes
        if force_bootstrap:
            current_download_progress = bootstrap_fresh_state(
                bootstrap_lookback_in_minutes,
                print_function_callback,
            )

        # 4. If download progress is available, load from the incremental download state file
        else:
            current_download_progress = load_progress_from_state_file(
                saved_progress_checkpoint_full_path, print_function_callback
            )

        # 5. Get list of ongoing jobs using jobs from current download progress & any updated jobs from deadline
        ongoing_jobs: Set[str] = get_list_of_ongoing_jobs_on_queue(
            boto3_session=boto3_session,
            last_known_set_of_ongoing_jobs=current_download_progress.get_job_ids(),
            farm_id=farm_id,
            queue_id=queue_id,
            last_lookback_time=current_download_progress.get_last_lookback_time(),
            print_function_callback=print_function_callback,
        )

        print_function_callback(f"Got the set of ongoing jobs: {ongoing_jobs} on queue {queue_id}")

        # 6. Download outputs for ongoing jobs using current download progress
        # Right now it is set to no change in progress except setting the last lookback time to now
        updated_download_progress: IncrementalDownloadState = current_download_progress
        updated_download_progress.last_lookback_time = datetime.datetime.utcnow().isoformat()

        # 7. Save progress to incremental download state file
        save_progress_to_state_file(
            saved_progress_checkpoint_location,
            saved_progress_checkpoint_full_path,
            updated_download_progress,
            print_function_callback,
        )

    except PidLockAlreadyHeld:
        print_function_callback(
            f"Another download is in progress at {saved_progress_checkpoint_location}, wait for previous download to finish"
        )
        return
    except Exception as e:
        print_function_callback(
            f"Failed to obtain lock for download progress at {saved_progress_checkpoint_location} due to unexpected exception : {e}"
        )
        return
    finally:
        # 4. Release pid lock since operation is complete
        _pid_utils.release_pid_lock(pid_file_full_path, print_function_callback)


def _validate_file_inputs_for_incremental_output_download(
    saved_progress_checkpoint_location: str, path_mapping_rules: Optional[str] = None
) -> bool:
    """
    Validate inputs for incremental output download
    :param saved_progress_checkpoint_location: location of the download progress file
    :param path_mapping_rules: path mapping rules for cross OS path mapping
    :return:
    """

    # Check if download progress location is a valid directory on the os
    if not os.path.isdir(saved_progress_checkpoint_location):
        raise RuntimeError(
            f"Download progress location {saved_progress_checkpoint_location} is not a valid directory"
        )

    # Check that download progress location is writable
    if not os.access(saved_progress_checkpoint_location, os.W_OK):
        raise RuntimeError(
            f"Download progress location {saved_progress_checkpoint_location} exists but is not writable, please provide write permissions"
        )

    # Check that the path mapping rules file exists on the os
    if path_mapping_rules is not None and not os.path.isfile(path_mapping_rules):
        raise RuntimeError(f"Path mapping rules file {path_mapping_rules} does not exist")

    # Check that the path mapping rules file is readable
    elif path_mapping_rules is not None and not os.access(path_mapping_rules, os.R_OK):
        raise RuntimeError(
            f"Path mapping rules file {path_mapping_rules} exists but is not readable, please provide read permissions"
        )

    return True
