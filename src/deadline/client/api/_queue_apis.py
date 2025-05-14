# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
from __future__ import annotations

__all__ = ["_incremental_output_download"]

from .. import api
from typing import Optional
import boto3
from deadline.client.cli._groups.click_logger import ClickLogger
from deadline.client import _pid_utils
from deadline.job_attachments.incremental_downloads.orchestrator import (
    IncrementalDownloadsOrchestrator,
)

import os


@api.record_function_latency_telemetry_event()
def _incremental_output_download(
    farm_id: str,
    queue_id: str,
    boto3_session: boto3.Session,
    saved_progress_checkpoint_location: str,
    bootstrap_lookback_in_minutes: Optional[int] = 0,
    force_bootstrap: Optional[bool] = False,
    path_mapping_rules: Optional[str] = None,
    logger: ClickLogger = ClickLogger(False),
):
    """
    Incrementally download outputs for jobs in a queue.

    Args:
        farm_id (str): The farm ID
        queue_id (str): The queue ID
        boto3_session (boto3.Session): The boto3 session
        saved_progress_checkpoint_location (str): Location to save progress checkpoints
        bootstrap_lookback_in_minutes (int, optional): Bootstrap lookback in minutes, default is 0
        force_bootstrap (bool, optional): option to force bootstrap the command
        path_mapping_rules (str, optional): Path mapping rules for cross-OS path mapping
        logger: Logger instance for logging messages
    """
    try:
        # 1. First get the current process's pid
        current_process_pid: str = str(os.getpid())

        # 2. Check if a download is already ongoing with pid lock checking mechanism
        _pid_utils.check_and_obtain_pid_lock_if_available(
            saved_progress_checkpoint_location, current_process_pid, logger
        )
    except RuntimeError as e:
        logger.echo(f"Download failed because of error : {e}")
        return
    except Exception as e:
        logger.echo(
            f"Failed to obtain lock for download progress at {saved_progress_checkpoint_location} due to unexpected exception : {e}"
        )
        return

    # 3. Orchestrate the download workflow for outputs of all jobs running on queue
    return IncrementalDownloadsOrchestrator.orchestrate_download_outputs_workflow(
        boto3_session,
        farm_id,
        logger,
        path_mapping_rules,
        queue_id,
        saved_progress_checkpoint_location,
        bootstrap_lookback_in_minutes,
        force_bootstrap,
        current_process_pid,
    )


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
