# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
from deadline.job_attachments.incremental_downloads.models import HydrationState
from deadline.job_attachments.incremental_downloads.models import StateFileModel
from deadline.client.cli._groups.click_logger import ClickLogger
from typing import Dict


class StateCheckpointHydrator:
    @classmethod
    def initialize_in_memory_maps_from_current_progress(
        cls, current_download_progress: StateFileModel, logger: ClickLogger
    ) -> HydrationState:
        """
        Initialize in-memory maps from the current download progress.
        :param current_download_progress: Current download progress
        :param logger: ClickLogger instance for logging messages
        :return: HydrationState instance
        """

        # 1. Initialize set of ongoing jobs from current download progress
        ongoing_jobs = set()
        for job in current_download_progress.jobs:
            job_id = job.get("jobId")
            if job_id:
                ongoing_jobs.add(job_id)
        logger.echo(f"Found {len(ongoing_jobs)} ongoing jobs in current progress")

        # 2. Initialize set of ongoing sessions from current download progress
        ongoing_sessions = set()
        session_to_job_map = {}  # Maps session_id to job_id for quick lookup
        session_to_lifecycle_status_map = {}  # Maps session_id to lifecycle status for quick lookup
        for job in current_download_progress.jobs:
            job_id = job.get("jobId")
            if job_id:
                for session in job.get("sessions", []):
                    session_id = session.get("sessionId")
                    if session_id:
                        ongoing_sessions.add(session_id)
                        session_to_job_map[session_id] = job_id
                        session_to_lifecycle_status_map[session_id] = session.get("lifecycleStatus")
        logger.echo(f"Found {len(ongoing_sessions)} ongoing sessions in current progress")

        # 3. Initialize map of last downloaded session action index to a session id
        session_action_index_map = {}
        for job in current_download_progress.jobs:
            for session in job.get("sessions", []):
                session_id = session.get("sessionId")
                last_action_id = session.get("lastDownloadedSessActionId")
                if session_id and last_action_id is not None:
                    session_action_index_map[session_id] = last_action_id
        logger.echo(
            f"Initialized session action index map with {len(session_action_index_map)} entries"
        )

        # 4. Populate an auxiliary_session_to_job_mapping and an auxiliary_session_lifecycle_status.
        auxiliary_session_action_status_mapping: Dict[str, str] = {}
        session_to_last_finished_action_id_map: Dict[str, int] = {}

        # Create and return a HydrationState instance
        return HydrationState(
            ongoing_jobs=ongoing_jobs,
            session_action_index_map=session_action_index_map,
            session_to_job_map=session_to_job_map,
            session_to_lifecycle_status_map=session_to_lifecycle_status_map,
            auxiliary_session_action_status_mapping=auxiliary_session_action_status_mapping,
            session_to_last_finished_action_id_map=session_to_last_finished_action_id_map,
        )

    @classmethod
    def update_download_progress_state(
        cls,
        logger: ClickLogger,
        ongoing_sessions: set,
        session_action_index_map: dict,
        session_to_job_map: dict,
        session_to_last_finished_action_id_map: dict,
        session_to_lifecycle_status_map: dict,
    ) -> list:
        """
        Updates the download progress state based on the current ongoing sessions and session action index map.

        :param logger: logger instance
        :param ongoing_sessions: set of ongoing sessions
        :param session_action_index_map: session action to last downloaded index map
        :param session_to_job_map: session to job id mapping
        :param session_to_last_finished_action_id_map: session to last finished session action map
        :param session_to_lifecycle_status_map: session to it's lifecycle status map
        :return: list of updated ongoing jobs for storing back to state file
        """

        # 1. Create a new jobs list for the updated progress
        updated_jobs: list[dict] = []

        # 2. Track jobs that have at least one session
        jobs_with_sessions = set()

        # 3. Process each ongoing session
        for session_id in ongoing_sessions:
            # a. Get the last downloaded session action ID for this session
            last_downloaded_action_id = session_action_index_map.get(session_id, -1)

            # b. Skip sessions with "ENDED" lifecycle status that are completely downloaded
            # Verify that the last session action ID from the list call matches the last downloaded one
            if (
                session_to_lifecycle_status_map.get(session_id) == "ENDED"
                and session_to_last_finished_action_id_map[session_id] == last_downloaded_action_id
            ):
                logger.echo(
                    f"Skipping session {session_id} with ENDED lifecycle status and no session actions remaining to be downloaded"
                )
                continue

            # c. Get job ID for this session using the existing session_to_job_map
            job_id = session_to_job_map.get(session_id)

            # d. Mark this job as having at least one session
            jobs_with_sessions.add(job_id)

            # e. Find or create job entry
            job_entry = None
            for job in updated_jobs:
                if job.get("jobId") == job_id:
                    job_entry = job
                    break

            if not job_entry:
                job_entry = {"jobId": job_id, "sessions": []}
                updated_jobs.append(job_entry)

            # f. Add session to job entry
            session_entry = {
                "sessionId": session_id,
                "lastDownloadedSessActionId": last_downloaded_action_id,
            }

            # g. Add lifecycle status if available
            session_lifecycle_status = session_to_lifecycle_status_map.get(session_id)
            if session_lifecycle_status:
                session_entry["sessionLifecycleStatus"] = session_lifecycle_status

            job_entry["sessions"].append(session_entry)

        # 4. Filter out jobs that don't have any sessions
        final_jobs = [job for job in updated_jobs if job.get("jobId") in jobs_with_sessions]
        return final_jobs
