# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import json
import os
import datetime
from typing import Optional, Callable, List, Dict, Any, Set


class JobSession:
    """
    Model representing a job session in the download progress state.
    """

    def __init__(
        self,
        session_id: str,
        session_lifecycle_status: str,
        last_downloaded_sess_action_id: int,
        job_id: str = "",
    ):
        """
        Initialize a JobSession instance.
        Args:
            session_id (str): The ID of the session
            session_lifecycle_status (str): The lifecycle status of the session
            last_downloaded_sess_action_id (int): The ID of the last downloaded session action
            job_id (str): The ID of the job this session belongs to (optional)
        """
        self.session_id = session_id
        self.session_lifecycle_status = session_lifecycle_status
        self.last_downloaded_sess_action_id = last_downloaded_sess_action_id
        self.job_id = job_id

    @classmethod
    def from_dict(cls, data: Dict[str, Any]):
        """
        Create a JobSession instance from a dictionary.
        Args:
            data (dict): Dictionary containing session data
        Returns:
            JobSession: A new instance populated with the data
        """
        return cls(
            session_id=str(data.get("sessionId", "")),
            session_lifecycle_status=str(data.get("sessionLifecycleStatus", "")),
            last_downloaded_sess_action_id=int(data.get("lastDownloadedSessActionId", 0)),
            job_id=str(data.get("jobId", "")),
        )

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the JobSession to a dictionary.
        Returns:
            dict: Dictionary representation of the session
        """
        result = {
            "sessionId": self.session_id,
            "sessionLifecycleStatus": self.session_lifecycle_status,
            "lastDownloadedSessActionId": self.last_downloaded_sess_action_id,
        }
        if self.job_id:
            result["jobId"] = self.job_id
        return result


class Job:
    """
    Model representing a job in the download progress state.
    """

    def __init__(self, job_id: str, sessions: Optional[List[JobSession]] = None):
        """
        Initialize a Job instance.
        Args:
            job_id (str): The ID of the job
            sessions (list): List of JobSession objects
        """
        self.job_id = job_id
        self.sessions = sessions or []

    @classmethod
    def from_dict(cls, data: Dict[str, Any]):
        """
        Create a Job instance from a dictionary.
        Args:
            data (dict): Dictionary containing job data
        Returns:
            Job: A new instance populated with the data
        """
        sessions = [JobSession.from_dict(session) for session in data.get("sessions", [])]
        return cls(job_id=str(data.get("jobId", "")), sessions=sessions)

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the Job to a dictionary.
        Returns:
            dict: Dictionary representation of the job
        """
        return {"jobId": self.job_id, "sessions": [session.to_dict() for session in self.sessions]}


class IncrementalDownloadState:
    """
    Model representing the download progress state file structure.
    Incremental download state file structure:
    {
        "lastLookbackTime": "2025-04-04T05:30:00",
        "jobs":
        [
            {
                "jobId": "job-1234353453443",
                "sessions": [
                {
                    "sessionId": "session-1324324354354",
                    "sessionLifecycleStatus": "SUCCESSFUL",
                    "lastDownloadedSessActionId": 3
                },
                {
                    "sessionId": "session-3423435435454",
                    "sessionLifecycleStatus": "RUNNING",
                    "lastDownloadedSessActionId": 6
                }
                ]
            },
            {
                "jobId": "job-3234324354345",
                "sessions": [
                {
                    "sessionId": "session-4235435434345",
                    "sessionLifecycleStatus": "FAILED",
                    "lastDownloadedSessActionId": 3
                }
                ]
            }
        ]
    }
    """

    last_lookback_time: str = ""
    jobs: List[Job] = []

    def __init__(self, last_lookback_time: str = "", jobs: List[Job] = []):
        """
        Initialize a IncrementalDownloadState instance.
        Args:
            last_lookback_time (str): ISO format timestamp of the last lookback time
            jobs (list): List of Jobs containing job_id and sessions information
        """
        self.last_lookback_time = last_lookback_time
        self.jobs = jobs

    @classmethod
    def from_dict(cls, data):
        """
        Create a IncrementalDownloadState instance from a dictionary.
        Args:
            data (dict): Dictionary containing state file data
        Returns:
            IncrementalDownloadState: A new instance populated with the data
        """
        if not data:
            return cls()

        jobs_data = data.get("jobs", [])
        jobs = [Job.from_dict(job) for job in jobs_data]

        return cls(last_lookback_time=data.get("lastLookbackTime"), jobs=jobs)

    def to_dict(self):
        """
        Convert the IncrementalDownloadState to a dictionary.
        Returns:
            dict: Dictionary representation of the state file model
        """
        return {
            "lastLookbackTime": self.last_lookback_time,
            "jobs": [job.to_dict() for job in self.jobs],
        }

    def get_job_ids(self) -> Set[str]:
        """
        Get a set of all job IDs in the state.
        Returns:
            Set[str]: Set of job IDs
        """
        return {job.job_id for job in self.jobs}

    def get_last_lookback_time(self) -> str:
        return self.last_lookback_time


def update_download_state_using_ongoing_sessions(
    ongoing_sessions: List[JobSession],
    command_start_time: str,
) -> IncrementalDownloadState:
    """
    Update the download state using the list of ongoing sessions.

    Args:
        ongoing_sessions: List of JobSession objects for sessions that need processing
        command_start_time: The start time of the command, to be used as the new lookback time

    Returns:
        Updated IncrementalDownloadState object
    """
    updated_state = IncrementalDownloadState()

    # Update the last lookback time to the command start time
    updated_state.last_lookback_time = command_start_time

    # Create a set of jobs that have at least one session in the current download progress
    job_ids_with_sessions: Set[str] = set()

    # Process each ongoing session
    for job_session in ongoing_sessions:
        job_id = job_session.job_id
        session_id = job_session.session_id

        # Add the job ID to the set of jobs with sessions
        job_ids_with_sessions.add(job_id)

        # Find or create the job in the state
        job_state = next((job for job in updated_state.jobs if job.job_id == job_id), None)
        if job_state is None:
            job_state = Job(job_id=job_id, sessions=[])
            updated_state.jobs.append(job_state)

        # Find or create the session in the job state
        session_state = next((s for s in job_state.sessions if s.session_id == session_id), None)

        if session_state is None:
            # Create a new JobSession without the job_id field to avoid duplication
            new_session = JobSession(
                session_id=job_session.session_id,
                session_lifecycle_status=job_session.session_lifecycle_status,
                last_downloaded_sess_action_id=job_session.last_downloaded_sess_action_id,
            )
            job_state.sessions.append(new_session)
        else:
            # Update the existing session state
            session_state.session_lifecycle_status = job_session.session_lifecycle_status

            # Update the last downloaded action ID if it's higher
            if (
                job_session.last_downloaded_sess_action_id
                > session_state.last_downloaded_sess_action_id
            ):
                session_state.last_downloaded_sess_action_id = (
                    job_session.last_downloaded_sess_action_id
                )

    return updated_state


def bootstrap_fresh_state(
    bootstrap_lookback_in_minutes: Optional[int], print_function_callback: Callable[[str], None]
) -> IncrementalDownloadState:
    """
    Bootstraps fresh download progress state using bootstrap lookback if provided or now
    :param bootstrap_lookback_in_minutes: lookback period in minutes
    :param print_function_callback: Callback to print messages produced in this function.
            Used in the CLI to print to stdout using click.echo. By default, ignores messages.
    :return: returns a fresh download progress state
    """
    current_download_progress: IncrementalDownloadState = IncrementalDownloadState()

    print_function_callback(
        "Bootstrapping command. Ignoring download progress location and creating new"
    )
    current_download_progress.last_lookback_time = (
        datetime.datetime.utcnow()
        - datetime.timedelta(minutes=float(bootstrap_lookback_in_minutes or 0))
    ).isoformat()
    return current_download_progress


def load_progress_from_state_file(
    saved_progress_checkpoint_full_path: str,
    print_function_callback: Callable[[str], None],
) -> IncrementalDownloadState:
    """
    Loads progress from state file saved at saved_progress_checkpoint_full_path
    :param saved_progress_checkpoint_full_path: full path of the saved progress checkpoint file
    :param print_function_callback: Callback to print messages produced in this function.
            Used in the CLI to print to stdout using click.echo. By default, ignores messages.
    :return: Returns the loaded state file,
    or throws an exception if we're unable to read it as we already validated its existence
    """
    try:
        with open(saved_progress_checkpoint_full_path, "r") as file:
            state_data = json.load(file)
            current_download_progress: IncrementalDownloadState = (
                IncrementalDownloadState.from_dict(state_data)
            )
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


def save_progress_to_state_file(
    saved_progress_checkpoint_location: str,
    current_saved_progress_checkpoint_full_path: str,
    current_download_progress: IncrementalDownloadState,
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

        # 2. Convert the IncrementalDownloadState to a dictionary
        state_data = current_download_progress.to_dict()

        # 3. Get a unique id for atomic file update and create a temporary file in the given directory
        unique_process_identifier = str(os.getpid())
        temp_file_path = (
            f"{current_saved_progress_checkpoint_full_path}_{unique_process_identifier}.tmp"
        )

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
