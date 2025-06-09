# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

from typing import Dict, List, Set, Any, Callable, Optional
import boto3
from datetime import datetime, timezone

from deadline.client.api._session import get_default_client_config
from botocore.exceptions import ClientError
from deadline.client.exceptions import DeadlineOperationError
from deadline.job_attachments.incremental_downloads.incremental_download_state import (
    IncrementalDownloadState,
    JobSession,
)


class SessionActionMapping:
    """
    Model class representing a session action with its associated job, step, and task IDs.
    """

    def __init__(
            self,
            session_action_id: str,
            job_id: str,
            step_id: Optional[str] = None,
            task_id: Optional[str] = None,
            status: Optional[str] = None,
    ):
        self.session_action_id = session_action_id
        self.job_id = job_id
        self.step_id = step_id
        self.task_id = task_id
        self.status = status

    @classmethod
    def from_api_response(cls, action: Dict[str, Any], job_id: str) -> "SessionActionMapping":
        """
        Create a SessionActionMapping from an API response.

        :param action: The session action API response
        :param job_id: The job ID associated with this session action
        :return: A SessionActionMapping instance
        """
        session_action_id = action.get("sessionActionId", "")
        status = action.get("status")

        # Extract step_id and task_id from taskRun if available
        step_id = None
        task_id = None
        task_run = action.get("definition", {}).get("taskRun")
        if task_run:
            step_id = task_run.get("stepId")
            task_id = task_run.get("taskId")

        return cls(
            session_action_id=session_action_id,
            job_id=job_id,
            step_id=step_id,
            task_id=task_id,
            status=status,
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
        ] = {}  # Maps session ID to last finished action ID

        # Add a cache for parsed action IDs to avoid repeated parsing
        self._action_id_number_cache: Dict[str, int] = {}

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

    def get_list_of_ongoing_session_action_ids_for_jobs(
            self, job_ids: Set[str], farm_id: str, queue_id: str, last_lookback_time: str
    ) -> List[SessionActionMapping]:
        """
        Get list of session action mappings that have been updated since the last lookback time
        and haven't been downloaded yet.

        Optimized to process data in a single pass where possible and reduce nested loops.

        :param job_ids: Set of job IDs to check for sessions
        :param farm_id: Farm ID
        :param queue_id: Queue ID
        :param last_lookback_time: Last lookback time in ISO format
        :return: List of SessionActionMapping objects that need to be downloaded
        """
        self.print_function_callback(
            f"Getting ongoing sessions for {len(job_ids)} jobs since {last_lookback_time}"
        )

        # Step 1: Get all sessions updated since last lookback time
        updated_sessions = self._get_sessions_started_since_lookback_from_deadline(
            job_ids, farm_id, queue_id, last_lookback_time
        )
        self.print_function_callback(
            f"Found {len(updated_sessions)} newly created sessions from API"
        )

        # Step 2: Add sessions from download progress that are associated with the job IDs
        existing_sessions = self._get_sessions_from_download_progress(job_ids)

        # Use set operations for faster union and deduplication
        all_sessions = set(updated_sessions) | set(existing_sessions)
        self.print_function_callback(
            f"Total sessions to process (API + download progress): {len(all_sessions)}"
        )

        # Step 3: Get session actions for each session and filter for those that need downloading
        session_action_mappings = []
        seen_action_ids = set()  # For fast duplicate checking

        self.print_function_callback("Fetching session actions for all ongoing sessions...")

        # Process each session once
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

            # Track the highest finished action ID in a single pass
            highest_finished_action_num = -1
            highest_finished_action_id = None

            # Process all actions in a single pass
            for action in session_actions:
                action_id = action["sessionActionId"]
                status = action.get("status", "")

                # Update status tracking in the same pass
                if status:
                    self.auxiliary_session_action_status_mapping[action_id] = status

                # Process only SUCCEEDED task run actions
                if (
                        status == "SUCCEEDED"
                        and action.get("definition", {}).get("taskRun") is not None
                ):
                    # Get action number once
                    action_num = self._get_action_id_number_from_session_action_id(action_id)

                    # Track highest finished action in the same pass
                    if action_num > highest_finished_action_num:
                        highest_finished_action_num = action_num
                        highest_finished_action_id = action_id

                    # Check if this action needs to be downloaded
                    if action_num > last_downloaded_action_id:
                        # Convert to SessionActionMapping and add to the list if not a duplicate
                        if action_id not in seen_action_ids:
                            mapping = SessionActionMapping.from_api_response(action, job_id)
                            session_action_mappings.append(mapping)
                            seen_action_ids.add(action_id)

            # Update the last finished action ID for this session after processing all actions
            if highest_finished_action_id:
                self.session_to_last_finished_action_id[session_id] = highest_finished_action_id

        return session_action_mappings

    def get_updated_list_of_ongoing_sessions_pending_download(
            self, downloaded_session_action_ids: List[str]
    ) -> List[JobSession]:
        """
        Get an updated list of ongoing sessions that are pending download using the in-memory maps
        and successfully downloaded session action ids

        This method combines the current state of downloads with the downloaded session action IDs
        to determine which sessions still need to be processed.

        Args:
            downloaded_session_action_ids: List of session action IDs that have been downloaded
                                          (source of truth for what has been downloaded)

        Returns:
            List of JobSession objects for sessions that need further processing
        """
        # Process downloaded action IDs more efficiently
        session_to_max_action_num: Dict[str, int] = {}

        # Process all downloaded action IDs in a single pass
        for action_id in downloaded_session_action_ids:
            # Extract session ID and action number from the action ID
            parts = action_id.split("-")
            if len(parts) >= 3:
                action_num = int(parts[-1])
                session_id_parts = parts[:-1]

                if session_id_parts[0] == "sessionaction":
                    session_id_parts[0] = "session"

                session_id = "-".join(session_id_parts)

                # Update max action number for this session
                current_max = session_to_max_action_num.get(session_id, -1)
                if action_num > current_max:
                    session_to_max_action_num[session_id] = action_num
                    self.session_to_last_downloaded_action_id[session_id] = action_id

        # Create session mappings in a single pass
        session_mappings = []

        for session_id, job_id in self.session_to_job_map.items():
            # Get session status
            session_status = self.session_to_lifecycle_status_map.get(session_id) or ""

            # Get last downloaded and finished action numbers
            last_downloaded_action_id_str = (
                    self.session_to_last_downloaded_action_id.get(session_id) or ""
            )
            last_downloaded_action_num = self._get_action_id_number_from_session_action_id(
                last_downloaded_action_id_str
            )

            last_finished_action_id = self.session_to_last_finished_action_id.get(session_id) or ""
            last_finished_action_num = self._get_action_id_number_from_session_action_id(
                last_finished_action_id
            )

            # Logic for determining if session needs further processing
            # If the session is not ENDED it always needs further processing
            # If the session is ENDED it needs further processing if:
            # There is a last finished action id - so it is a session with atleast one task run action AND
            # The last finished action id was not the last downloaded action id, so there's more to download
            include_session = session_status != "ENDED" or (
                    session_status == "ENDED"
                    and last_finished_action_id
                    and last_finished_action_id != ""
                    and (
                            last_finished_action_num < 0
                            or last_downloaded_action_num < last_finished_action_num
                    )
            )

            if include_session:
                session_mappings.append(
                    JobSession(
                        session_id=session_id,
                        session_lifecycle_status=session_status,
                        last_downloaded_sess_action_id=last_downloaded_action_num,
                        job_id=job_id,
                    )
                )

        return session_mappings

    def _get_sessions_started_since_lookback_from_deadline(
            self, job_ids: Set[str], farm_id: str, queue_id: str, last_lookback_time: str
    ) -> List[str]:
        """
        Get newer sessions that have been started since the last lookback time.
        Optimized to request maximum results per page.

        :param job_ids: Set of job IDs to check for sessions
        :param farm_id: Farm ID
        :param queue_id: Queue ID
        :param last_lookback_time: Last lookback time in ISO format
        :return: List of session IDs that have been updated
        """
        newer_sessions: List[str] = []
        deadline_client = self.boto3_session.client("deadline", config=get_default_client_config())

        try:
            # Convert last_lookback_time to datetime for comparison
            lookback_datetime = datetime.fromisoformat(last_lookback_time)
            if lookback_datetime.tzinfo is None:
                lookback_datetime = lookback_datetime.replace(tzinfo=timezone.utc)

            # For each job, list its sessions
            for job_id in job_ids:
                try:
                    # Request maximum results per page (100 is the API limit)
                    response = deadline_client.list_sessions(
                        farmId=farm_id,
                        jobId=job_id,
                        queueId=queue_id,
                        maxResults=100,  # Maximum allowed by API
                    )

                    # Process the first page of results
                    self._process_sessions_response(
                        response, job_id, lookback_datetime, newer_sessions
                    )

                    # Continue paginating if there are more results
                    while "nextToken" in response and response["nextToken"]:
                        response = deadline_client.list_sessions(
                            farmId=farm_id,
                            jobId=job_id,
                            queueId=queue_id,
                            nextToken=response["nextToken"],
                            maxResults=100,  # Maximum allowed by API
                        )
                        self._process_sessions_response(
                            response, job_id, lookback_datetime, newer_sessions
                        )

                except ClientError as e:
                    self.print_function_callback(
                        f"Error listing sessions for job {job_id}: {str(e)}"
                    )
                    continue

        except ClientError as e:
            raise DeadlineOperationError(f"Failed to get sessions from Deadline: {str(e)}") from e

        return newer_sessions

    def _process_sessions_response(self, response, job_id, lookback_datetime, newer_sessions):
        """
        Process a page of sessions response and update the newer_sessions list.
        Optimized to reduce redundant operations.

        :param response: API response from list_sessions
        :param job_id: Current job ID
        :param lookback_datetime: Datetime to compare against
        :param newer_sessions: List to append newer session IDs to
        """
        for session in response.get("sessions", []):
            session_id = session.get("sessionId")

            # Update tracking maps in a single operation
            self.session_to_job_map[session_id] = job_id
            self.session_to_lifecycle_status_map[session_id] = session["lifecycleStatus"]

            # Check if the session was started after the lookback time
            started_at = session.get("startedAt")
            if started_at and started_at >= lookback_datetime:
                newer_sessions.append(session_id)

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
        Get all session actions for a session.
        Optimized to request maximum results per page.

        :param job_id: Job ID
        :param queue_id: Queue ID
        :param session_id: Session ID
        :param farm_id: Farm ID
        :return: List of session actions
        """
        deadline_client = self.boto3_session.client("deadline", config=get_default_client_config())
        session_actions = []

        try:
            # Request maximum results per page (100 is the API limit)
            response = deadline_client.list_session_actions(
                farmId=farm_id,
                queueId=queue_id,
                jobId=job_id,
                sessionId=session_id,
                maxResults=100,  # Maximum allowed by API
            )

            # Process the first page of results
            session_actions.extend(response.get("sessionActions", []))

            # Continue paginating if there are more results
            while "nextToken" in response and response["nextToken"]:
                response = deadline_client.list_session_actions(
                    farmId=farm_id,
                    queueId=queue_id,
                    jobId=job_id,
                    sessionId=session_id,
                    nextToken=response["nextToken"],
                    maxResults=100,  # Maximum allowed by API
                )
                session_actions.extend(response.get("sessionActions", []))

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
        Extract the numeric part from a session action ID with caching.

        :param action_id: Session action ID (e.g., "sessionaction-12345-1")
        :return: Numeric part of the action ID, or -1 if invalid format
        """
        # Return from cache if available
        if action_id in self._action_id_number_cache:
            return self._action_id_number_cache[action_id]

        # Default value for invalid/empty IDs
        if not action_id or action_id == "":
            self._action_id_number_cache[action_id] = -1
            return -1

        try:
            # Extract the last part of the ID which should be the numeric index
            parts = action_id.split("-")
            if len(parts) >= 3:
                result = int(parts[-1])
            else:
                result = -1
        except (ValueError, IndexError):
            result = -1

        # Cache the result
        self._action_id_number_cache[action_id] = result
        return result
