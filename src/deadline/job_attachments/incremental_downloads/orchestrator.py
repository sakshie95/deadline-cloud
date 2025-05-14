# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import boto3
from deadline.client.cli._groups.click_logger import ClickLogger
from deadline.client import _pid_utils

from deadline.job_attachments.incremental_downloads.models import StateFileModel
import json
import os
import datetime
from deadline.job_attachments.incremental_downloads.state_checkpoint_hydrator import (
    StateCheckpointHydrator,
)
from deadline.job_attachments.incremental_downloads._job_processor import JobProcessor
from deadline.job_attachments.incremental_downloads._session_processor import SessionProcessor
from deadline.job_attachments.incremental_downloads._session_action_processor import (
    SessionActionProcessor,
)
from deadline.job_attachments.incremental_downloads.models import HydrationState
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
        force_bootstrap: Optional[bool],
        current_process_id: str,
    ):
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
            force_bootstrap (bool, optional): option to force bootstrap the command
            current_process_id (str): The current process ID
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
                # TODO throw exception because this is an unexpected error

        # 3. Download outputs of ongoing jobs, sessions, session actions using current progress
        updated_download_progress: StateFileModel = cls._download_outputs_from_current_progress(
            farm_id, queue_id, boto3_session, current_download_progress, path_mapping_rules, logger
        )
        # 4. Save progress to state file
        cls._save_download_progress_to_state_file(
            saved_progress_checkpoint_location,
            saved_progress_checkpoint_full_path,
            updated_download_progress,
            current_process_id,
            logger,
        )

        # 5. Release pid lock since operation is complete
        _pid_utils.release_pid_lock(saved_progress_checkpoint_location, current_process_id, logger)

        return True

    @classmethod
    def _download_outputs_from_current_progress(
        cls,
        farm_id: str,
        queue_id: str,
        boto3_session: boto3.Session,
        current_download_progress: StateFileModel,
        path_mapping_rules: Optional[str],
        logger: ClickLogger,
    ) -> StateFileModel:
        """
        Download outputs from the current progress state.

        Args:
            farm_id (str): The farm ID
            queue_id (str): The queue ID
            boto3_session (boto3.Session): The boto3 session
            current_download_progress (StateFileModel): The current download progress state
            path_mapping_rules (str, optional): Path mapping rules for cross-OS path mapping
            logger: Logger instance for logging messages
        """

        # 1. Set command start time to now - to save to download progress at the end.
        command_start_time = datetime.datetime.utcnow().isoformat() + "Z"

        # 2. Extract lastLookbackTime from the current_download_progress state.
        last_lookback_time = current_download_progress.last_lookback_time

        # 3. Initialize all in-memory maps from current download progress if it's not empty
        hydration_state: HydrationState = HydrationState()

        if len(current_download_progress.jobs) > 0:
            hydration_state = (
                StateCheckpointHydrator.initialize_in_memory_maps_from_current_progress(
                    current_download_progress, logger
                )
            )

        # Get all in-memory maps from in-memory hydration state
        ongoing_jobs = hydration_state.ongoing_jobs
        session_action_index_map = hydration_state.session_action_index_map
        session_to_job_map = hydration_state.session_to_job_map
        session_to_lifecycle_status_map = hydration_state.session_to_lifecycle_status_map
        auxiliary_session_action_status_mapping = (
            hydration_state.auxiliary_session_action_status_mapping
        )
        session_to_last_finished_action_id_map = (
            hydration_state.session_to_last_finished_action_id_map
        )

        try:
            # 4. Query for search jobs api to get all jobs in the queue updated since the last lookback time
            # 5. Hydrate ongoing jobs using current state and newer jobs from search api call
            ongoing_jobs = JobProcessor.hydrate_and_process_jobs(
                ongoing_jobs, farm_id, queue_id, last_lookback_time, logger
            )

            # 6. Process each job. For each job, we would query for list-sessions API to get all sessions in the job
            ongoing_sessions = SessionProcessor.hydrate_and_process_sessions(
                ongoing_jobs, farm_id, queue_id, last_lookback_time, logger
            )

            # 7. Process each session by looking at last downloaded session action for it and downloading remaining
            session_action_processor: SessionActionProcessor = SessionActionProcessor(
                auxiliary_session_action_status_mapping,
                logger,
                ongoing_sessions,
                session_action_index_map,
                session_to_last_finished_action_id_map,
            )

            session_action_processor.hydrate_and_process_session_actions()

            # 8. Update current progress state with ongoing lists and auxiliary maps
            updated_jobs = StateCheckpointHydrator.update_download_progress_state(
                logger,
                ongoing_sessions,
                session_action_index_map,
                session_to_job_map,
                session_to_last_finished_action_id_map,
                session_to_lifecycle_status_map,
            )

            # 9. Update the current download progress with the new jobs list
            updated_download_progress: StateFileModel = StateFileModel()
            updated_download_progress.jobs = updated_jobs

            # 10. Update the last lookback time to the current time
            updated_download_progress.last_lookback_time = command_start_time

            # 11. Return the updated download progress
            return updated_download_progress

        except Exception as e:
            logger.echo(f"Error downloading outputs: {str(e)}")
            raise

    @classmethod
    def _save_download_progress_to_state_file(
        cls,
        saved_progress_checkpoint_location: str,
        current_saved_progress_checkpoint_full_path: str,
        current_download_progress: StateFileModel,
        unique_process_identifier: str,
        logger: ClickLogger,
    ):
        """
        Save the current download progress to a state file atomically.

        Args:
            saved_progress_checkpoint_location (str): Location to save the download progress file
            current_saved_progress_checkpoint_full_path (str): Absolute path of file with saved progress
            current_download_progress (StateFileModel): The current download progress state
            unique_process_identifier (str): unique process identifier for atomic updates to shared state file
            logger (ClickLogger): Logger instance for logging messages
        """
        try:
            # 1. Create directory if it doesn't exist
            os.makedirs(os.path.dirname(saved_progress_checkpoint_location), exist_ok=True)

            # 2. Convert the StateFileModel to a dictionary
            state_data = current_download_progress.to_dict()

            # 3. Create a temporary file in the same directory
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
