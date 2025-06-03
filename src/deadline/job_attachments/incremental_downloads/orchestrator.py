# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import boto3

from deadline.job_attachments.incremental_downloads.models import StateFileModel
import json
import os
import datetime
from typing import Optional, Callable


DOWNLOAD_PROGRESS_FILE_NAME = "download_progress.json"


class IncrementalDownloadsOrchestrator:
    @classmethod
    def orchestrate_download_outputs_workflow(
        cls,
        boto3_session: boto3.Session,
        farm_id: str,
        print_function_callback: Callable[[str], None],
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
            print_function_callback: Callback to print messages produced in this function.
                Used in the CLI to print to stdout using click.echo. By default, ignores messages.
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

        # 1. Bootstrap to ignore current download progress & set lookback based on bootstrap_lookback_in_minutes if:
        # this is the first run of the command & save progress checkpoint file does not exist
        # OR
        # if force bootstrap option is provided by customer
        if force_bootstrap or not os.path.exists(saved_progress_checkpoint_full_path):
            print_function_callback(
                "Bootstrapping command. Ignoring download progress location and creating new"
            )
            current_download_progress.last_lookback_time = (
                datetime.datetime.utcnow()
                - datetime.timedelta(minutes=float(bootstrap_lookback_in_minutes or 0))
            )

        # 2. Load progress from current download progress state file, throws an exception if there is unexpected failure
        else:
            current_download_progress = cls.load_state_file(
                saved_progress_checkpoint_full_path, print_function_callback
            )

        # 3. Download and update download progress.
        # Right now it is set to no change in progress except setting the last lookback time to now
        updated_download_progress: StateFileModel = current_download_progress
        updated_download_progress.last_lookback_time = datetime.datetime.utcnow().isoformat() + "Z"

        # 4. Save updated download progress back to state file
        cls._save_download_progress_to_state_file(
            saved_progress_checkpoint_location,
            saved_progress_checkpoint_full_path,
            updated_download_progress,
            print_function_callback,
        )

        return True

    @classmethod
    def load_state_file(
        cls, saved_progress_checkpoint_full_path: str, print_function_callback
    ) -> StateFileModel:
        """
        Loads state file from saved progress full path
        :param saved_progress_checkpoint_full_path: full path of the saved progress checkpoint file
        :param print_function_callback: Callback to print messages produced in this function.
                Used in the CLI to print to stdout using click.echo. By default, ignores messages.
        :return: Returns the loaded state file,
        or throws an exception if we're unable to read it as we already validated its existence
        """
        try:
            with open(saved_progress_checkpoint_full_path, "r") as file:
                state_data = json.load(file)
                current_download_progress: StateFileModel = StateFileModel.from_dict(state_data)
                print_function_callback(
                    f"Loaded existing state file from download progress checkpoint location {saved_progress_checkpoint_full_path}"
                )
            return current_download_progress

        except Exception as e:
            print_function_callback(
                f"Failed to load existing state file from download progress checkpoint location {saved_progress_checkpoint_full_path}: {str(e)}"
            )
            # Raise as this is an unexpected exception in reading state file, we already checked its existence earlier
            raise

    @classmethod
    def _save_download_progress_to_state_file(
        cls,
        saved_progress_checkpoint_location: str,
        current_saved_progress_checkpoint_full_path: str,
        current_download_progress: StateFileModel,
        print_function_callback: Callable[[str], None],
    ) -> None:
        """
        Save the current download progress to a state file atomically.

        :param saved_progress_checkpoint_location: Location to save the download progress file
        :param current_saved_progress_checkpoint_full_path: Absolute path of file with saved progress
        :param current_download_progress: The current download progress state
        :param print_function_callback: Callback to print messages produced in this function.
                Used in the CLI to print to stdout using click.echo. By default, ignores messages
        :return: None if save was successful,
        or throws an exception if we're unable to save progress file to download location.
            Hard fail for customer's assets to be downloaded EXACTLY once.
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

            print_function_callback(
                f"Successfully saved state file to {current_saved_progress_checkpoint_full_path}"
            )
        except Exception as e:
            print_function_callback(
                f"Failed to save state file to {current_saved_progress_checkpoint_full_path}: {str(e)}"
            )

            # Raise as this is an unexpected exception in saving state file,
            # Hard fail for customer's files to be downloaded EXACTLY once.
            raise
