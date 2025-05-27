# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import boto3
from deadline.client.cli._groups.click_logger import ClickLogger

from deadline.job_attachments.incremental_downloads.models import StateFileModel
import json
import os
import datetime
from typing import Optional


DOWNLOAD_PROGRESS_FILE_NAME = "download_progress.json"


class IncrementalDownloadsOrchestrator:
    @classmethod
    def orchestrate_download_outputs_workflow(
        cls,
        boto3_session: boto3.Session,
        farm_id: str,
        logger: ClickLogger,
        path_mapping_rules: Optional[str],
        queue_id: str,
        saved_progress_checkpoint_location: str,
        bootstrap_lookback_in_minutes: Optional[int],
        force_bootstrap: bool,
    ) -> bool:
        """
        Orchestrates the download outputs workflow.

        Args:
            boto3_session (boto3.Session): The boto3 session
            farm_id (str): The farm ID
            logger (ClickLogger): ClickLogger instance for logging messages
            path_mapping_rules (str, optional): Path mapping rules for cross-OS path mapping
            queue_id (str): The queue ID
            saved_progress_checkpoint_location (str): Location to save progress checkpoints
            bootstrap_lookback_in_minutes (int, optional): Bootstrap lookback in minutes, default is 0
            force_bootstrap (bool): option to force bootstrap the command
        """

        saved_progress_checkpoint_full_path: str = (
            f"{saved_progress_checkpoint_location}/{DOWNLOAD_PROGRESS_FILE_NAME}"
        )
        current_download_progress: StateFileModel = StateFileModel()

        # 1. Bootstrap if required
        if force_bootstrap or not os.path.exists(saved_progress_checkpoint_full_path):
            logger.echo(
                "Bootstrapping command. Ignoring download progress location and creating new"
            )
            current_download_progress.last_lookback_time = (
                datetime.datetime.utcnow()
                - datetime.timedelta(minutes=float(bootstrap_lookback_in_minutes or 0))
            )

        # 2. Load progress from current download progress state file
        else:
            try:
                with open(saved_progress_checkpoint_full_path, "r") as file:
                    state_data = json.load(file)
                    current_download_progress = StateFileModel.from_dict(state_data)
                    logger.echo(
                        f"Loaded existing download progress from {saved_progress_checkpoint_full_path}"
                    )

            except Exception as e:
                logger.echo(
                    f"Failed to load download progress from {saved_progress_checkpoint_full_path}: {str(e)}"
                )
                return False

        # TODO 3. Download outputs of ongoing jobs, sessions, session actions using current progress
        # Right now it is set to no change in progress
        updated_download_progress: StateFileModel = current_download_progress

        # 4. Save progress back to state file
        cls._save_download_progress_to_state_file(
            saved_progress_checkpoint_location,
            saved_progress_checkpoint_full_path,
            updated_download_progress,
            logger,
        )

        return True

    @classmethod
    def _save_download_progress_to_state_file(
        cls,
        saved_progress_checkpoint_location: str,
        current_saved_progress_checkpoint_full_path: str,
        current_download_progress: StateFileModel,
        logger: ClickLogger,
    ):
        """
        Save the current download progress to a state file atomically.

        Args:
            saved_progress_checkpoint_location (str): Location to save the download progress file
            current_saved_progress_checkpoint_full_path (str): Absolute path of file with saved progress
            current_download_progress (StateFileModel): The current download progress state
            logger (ClickLogger): Logger instance for logging messages
        """
        try:
            # 1. Create directory if it doesn't exist
            os.makedirs(os.path.dirname(saved_progress_checkpoint_location), exist_ok=True)

            # 2. Convert the StateFileModel to a dictionary
            state_data = current_download_progress.to_dict()

            # 3. Get a unique id for atomic file update and create a temporary file in the given directory
            unique_process_identifier = str(os.getpid())
            temp_file_path = f"{saved_progress_checkpoint_location}/{DOWNLOAD_PROGRESS_FILE_NAME}_{unique_process_identifier}.tmp"

            # 4. Write the state data to the temporary file
            with open(temp_file_path, "w") as file:
                json.dump(state_data, file, indent=2)
                # Ensure data is written to disk
                file.flush()
                os.fsync(file.fileno())

            # 5. Atomically replace the target file with the temporary file
            os.replace(temp_file_path, current_saved_progress_checkpoint_full_path)

            logger.echo(
                f"Successfully saved download progress to {current_saved_progress_checkpoint_full_path}"
            )
        except Exception as e:
            logger.echo(
                f"Failed to save download progress to {current_saved_progress_checkpoint_full_path}: {str(e)}"
            )
