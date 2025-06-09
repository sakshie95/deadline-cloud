# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

from typing import Dict, List, Set, Any, Callable
import boto3
from datetime import datetime, timezone

from deadline.client.api._session import get_default_client_config
from botocore.exceptions import ClientError
from deadline.client.exceptions import DeadlineOperationError
from deadline.job_attachments.incremental_downloads.incremental_download_state import (
    IncrementalDownloadState,
)


class SessionActionProcessor:
    """
    Processor for handling session actions and tracking download progress.

    This class is responsible for:
    1. Fetching sessions updated since the last lookback time
    2. Fetching session actions for those sessions
    3. Determining which session actions need to be downloaded
    4. Maintaining in-memory maps for tracking download progress
    """

    def __init__(
        self,
        boto3_session: boto3.Session,
        download_progress: IncrementalDownloadState,
        print_function_callback: Callable[[str], None] = lambda msg: None,
    ):
        """
        Initialize the SessionActionProcessor.

        :param boto3_session: boto3 session for making API calls
        :param download_progress: Current download progress data
        :param print_function_callback: Function for logging messages
        """
        self.boto3_session = boto3_session
        self.download_progress = download_progress
        self.print_function_callback = print_function_callback

        # Initialize in-memory maps for tracking download progress
        self.session_to_job_map: Dict[str, str] = {}
        self.session_to_lifecycle_status_map: Dict[str, str] = {}
        self.auxiliary_session_action_status_mapping: Dict[
            str, str
        ] = {}  # Maps session action ID to status
        self.session_to_last_downloaded_action_id: Dict[
            str, str
        ] = {}  # Maps session ID to last downloaded action ID
        self.session_to_last_finished_action_id: Dict[
            str, str
        ] = {}  # Maps session ID to last finished action ID (any terminal status)

        # Extract existing session action data from download progress if available
        for job in self.download_progress.jobs:
            for session in job.sessions:
                self.session_to_job_map[session.session_id] = job.job_id
                self.session_to_lifecycle_status_map[session.session_id] = (
                    session.session_lifecycle_status
                )

                # Convert the numeric action ID to a string format for tracking
                if session.last_downloaded_sess_action_id > 0:
                    action_id = f"{session.session_id}-{session.last_downloaded_sess_action_id}"
                    self.session_to_last_downloaded_action_id[session.session_id] = action_id

    def get_session_to_job_map(self) -> Dict[str, str]:
        """
        Get the current session to job mapping.

        :return: Dictionary mapping session IDs to job IDs
        """
        return self.session_to_job_map

    def get_session_to_lifecycle_status_map(self) -> Dict[str, str]:
        """
        Get the current session to lifecycle status mapping.

        :return: Dictionary mapping session IDs to lifecycle statuses
        """
        return self.session_to_lifecycle_status_map

    def get_auxiliary_session_action_status_mapping(self) -> Dict[str, str]:
        """
        Get the current session action to status mapping.

        :return: Dictionary mapping session action IDs to their statuses
        """
        return self.auxiliary_session_action_status_mapping

    def get_session_to_last_downloaded_action_id(self) -> Dict[str, str]:
        """
        Get the current session to last downloaded action ID mapping.

        :return: Dictionary mapping session IDs to their last downloaded action IDs
        """
        return self.session_to_last_downloaded_action_id

    def get_session_to_last_finished_action_id(self) -> Dict[str, str]:
        """
        Get the current session to last finished action ID mapping.

        :return: Dictionary mapping session IDs to their last finished action IDs (any terminal status)
        """
        return self.session_to_last_finished_action_id

    def get_list_of_ongoing_session_action_ids_for_jobs(
        self, job_ids: Set[str], farm_id: str, queue_id: str, last_lookback_time: str
    ) -> Dict[str, List[str]]:
        """
        Get map of job IDs to session action IDs that have been updated since the last lookback time
        and haven't been downloaded yet.

        :param job_ids: Set of job IDs to check for sessions
        :param farm_id: Farm ID
        :param queue_id: Queue ID
        :param last_lookback_time: Last lookback time in ISO format
        :return: Dictionary mapping job IDs to lists of session action IDs that need to be downloaded
        """
        self.print_function_callback(
            f"Getting ongoing session actions for {len(job_ids)} jobs since {last_lookback_time}"
        )

        # Step 1: Get all sessions updated since last lookback time
        updated_sessions: List[str] = self._get_updated_sessions_since_lookback_from_deadline(
            job_ids, farm_id, queue_id, last_lookback_time
        )
        self.print_function_callback(f"Found {len(updated_sessions)} updated sessions from API")

        # Step 2: Add sessions from download progress that are associated with the job IDs
        existing_sessions: List[str] = self._get_sessions_from_download_progress(job_ids)
        all_sessions: List[str] = list(set(updated_sessions + existing_sessions))
        self.print_function_callback(
            f"Total sessions to process (API + download progress): {len(all_sessions)}"
        )

        # Step 3: Get session actions for each session and filter for those that need downloading
        job_to_session_actions: Dict[str, List[str]] = {}

        for session_id in all_sessions:
            # Get the job ID for this session
            job_id = self.session_to_job_map.get(session_id)
            if not job_id:
                self.print_function_callback(f"No job ID found for session {session_id}, skipping")
                continue

            # Get the last downloaded action ID for this session
            last_downloaded_action_id = self._get_last_downloaded_action_id(session_id)

            # Get all actions for this session
            session_actions = self._get_session_actions_from_deadline(
                session_id, farm_id, queue_id, job_id
            )

            # Update tracking for all terminal status actions
            terminal_statuses = ["SUCCEEDED", "FAILED", "INTERRUPTED", "CANCELED"]
            for action in session_actions:
                action_id = action["sessionActionId"]
                status = action.get("status", "")

                # Track the status in our auxiliary mapping
                if status:
                    self.auxiliary_session_action_status_mapping[action_id] = status

                # For terminal statuses, update the last finished action ID if it's newer
                if status in terminal_statuses:
                    action_num = self._get_action_id_number_from_session_action_id(action_id)
                    last_finished_action_id = self._get_action_id_number_from_session_action_id(
                        self.session_to_last_finished_action_id.get(session_id, "")
                    )
                    if action_num > last_finished_action_id:
                        self.session_to_last_finished_action_id[session_id] = action_id

            # Filter for SUCCEEDED actions that haven't been downloaded yet and are taskRun session actions
            new_actions = [
                action
                for action in session_actions
                if (
                    self._get_action_id_number_from_session_action_id(action["sessionActionId"])
                    > last_downloaded_action_id
                    and action.get("status") == "SUCCEEDED"
                    and action.get("definition", {}).get("taskRun") is not None
                )
            ]

            if new_actions:
                # Add the session action IDs to our result map
                action_ids = [action["sessionActionId"] for action in new_actions]
                if job_id in job_to_session_actions:
                    # Use a set to ensure uniqueness before extending the list
                    existing_ids = set(job_to_session_actions[job_id])
                    unique_new_ids = [id for id in action_ids if id not in existing_ids]
                    job_to_session_actions[job_id].extend(unique_new_ids)
                else:
                    job_to_session_actions[job_id] = action_ids
            else:
                self.print_function_callback(
                    f"No new SUCCEEDED actions for session {session_id} (Job: {job_id})"
                )

        return job_to_session_actions

    def _get_updated_sessions_since_lookback_from_deadline(
        self, job_ids: Set[str], farm_id: str, queue_id: str, last_lookback_time: str
    ) -> List[str]:
        """
        Get sessions that have been updated since the last lookback time.

        :param job_ids: Set of job IDs to check for sessions
        :param farm_id: Farm ID
        :param queue_id: Queue ID
        :param last_lookback_time: Last lookback time in ISO format
        :return: List of session IDs that have been updated
        """
        updated_sessions: List[str] = []
        deadline_client = self.boto3_session.client("deadline", config=get_default_client_config())

        try:
            # Convert last_lookback_time to datetime for comparison
            lookback_datetime = datetime.fromisoformat(last_lookback_time)
            # Ensure lookback_datetime is timezone-aware (UTC)
            if lookback_datetime.tzinfo is None:
                lookback_datetime = lookback_datetime.replace(tzinfo=timezone.utc)

            # For each job, list its sessions
            for job_id in job_ids:
                try:
                    # Initial request
                    response = deadline_client.list_sessions(
                        farmId=farm_id, jobId=job_id, queueId=queue_id
                    )

                    # Process the first page of results
                    self._process_sessions_response(
                        response, job_id, lookback_datetime, updated_sessions
                    )

                    # Continue paginating if there are more results
                    while "nextToken" in response:
                        response = deadline_client.list_sessions(
                            farmId=farm_id,
                            jobId=job_id,
                            queueId=queue_id,
                            nextToken=response["nextToken"],
                        )
                        self._process_sessions_response(
                            response, job_id, lookback_datetime, updated_sessions
                        )

                except ClientError as e:
                    self.print_function_callback(
                        f"Error listing sessions for job {job_id}: {str(e)}"
                    )
                    continue

        except ClientError as e:
            raise DeadlineOperationError(f"Failed to get sessions from Deadline: {str(e)}") from e

        return updated_sessions

    def _process_sessions_response(self, response, job_id, lookback_datetime, updated_sessions):
        """
        Process a page of sessions response and update the updated_sessions list.

        :param response: API response from list_sessions
        :param job_id: Current job ID
        :param lookback_datetime: Datetime to compare against
        :param updated_sessions: List to append updated session IDs to
        """
        for session in response.get("sessions", []):
            session_id = session.get("sessionId")

            # TODO: Switch to using updatedAt once Deadline Cloud List API returns it consistently for all sessions
            # For now, we're using startedAt as a fallback since not all sessions have updatedAt in the response
            started_at = session.get("startedAt")

            # Check if the session was started after the lookback time
            if started_at and started_at >= lookback_datetime:
                updated_sessions.append(session_id)

                # Update our tracking maps
                self.session_to_job_map[session_id] = job_id
                self.session_to_lifecycle_status_map[session_id] = session["lifecycleStatus"]

    def _get_sessions_from_download_progress(self, job_ids: Set[str]) -> List[str]:
        """
        Get sessions from download progress that are associated with the given job IDs.

        :param job_ids: Set of job IDs to check for sessions
        :return: List of session IDs from download progress
        """
        sessions_from_progress = []

        for job in self.download_progress.jobs:
            if job.job_id in job_ids:
                for session in job.sessions:
                    sessions_from_progress.append(session.session_id)

        return sessions_from_progress

    def _get_session_actions_from_deadline(
        self, session_id: str, farm_id: str, queue_id: str, job_id: str
    ) -> List[Dict[str, Any]]:
        """
        Get all actions for a session.

        :param job_id:
        :param queue_id: QueueID
        :param session_id: Session ID
        :param farm_id: Farm ID
        :return: List of session actions
        """
        deadline_client = self.boto3_session.client("deadline", config=get_default_client_config())
        session_actions = []

        try:
            # Initial request
            response = deadline_client.list_session_actions(
                farmId=farm_id,
                queueId=queue_id,
                jobId=job_id,
                sessionId=session_id,
            )

            # Process the first page of results
            session_actions = response.get("sessionActions", [])

            # Continue paginating if there are more results
            while "nextToken" in response:
                response = deadline_client.list_session_actions(
                    farmId=farm_id,
                    queueId=queue_id,
                    jobId=job_id,
                    sessionId=session_id,
                    nextToken=response["nextToken"],
                )
                self.print_function_callback(
                    f"List session actions next page response for session {session_id}: {response}"
                )
                session_actions.extend(response.get("sessionActions", []))

            # Update the auxiliary_session_action_status_mapping with status of each action
            for action in session_actions:
                if "sessionActionId" in action and "status" in action:
                    self.auxiliary_session_action_status_mapping[action["sessionActionId"]] = (
                        action["status"]
                    )

        except ClientError as e:
            self.print_function_callback(
                f"Error listing actions for session {session_id}: {str(e)}"
            )

        return session_actions

    def _get_last_downloaded_action_id(self, session_id: str) -> int:
        """
        Get the last downloaded action ID for a session.

        :param session_id: Session ID
        :return: Last downloaded action ID number, or -1 if none
        """
        # Check if we have this information in our session_to_last_downloaded_action_id map
        if session_id in self.session_to_last_downloaded_action_id:
            return self._get_action_id_number_from_session_action_id(
                self.session_to_last_downloaded_action_id[session_id]
            )

        # No record of downloaded actions for this session
        return -1

    def _get_action_id_number_from_session_action_id(self, action_id: str) -> int:
        """
        Extract the numeric part from a session action ID.

        :param action_id: Session action ID (e.g., "Session-12345-1")
        :return: Numeric part of the action ID, or -1 if invalid format
        """
        try:
            # Extract the last part of the ID which should be the numeric index
            parts = action_id.split("-")
            if len(parts) >= 3:
                return int(parts[-1])
            return -1
        except (ValueError, IndexError):
            return -1
