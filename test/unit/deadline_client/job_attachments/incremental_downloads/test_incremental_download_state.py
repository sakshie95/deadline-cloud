# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import os
import json
import pytest
from unittest.mock import patch, MagicMock, mock_open
from typing import Dict, Any, List, cast

from deadline.job_attachments.incremental_downloads.incremental_download_state import (
    IncrementalDownloadState,
    bootstrap_fresh_state,
    Job,
    JobSession,
    load_progress_from_state_file,
    save_progress_to_state_file,
    update_download_state_using_ongoing_sessions,
)
from freezegun import freeze_time


@pytest.fixture
def mock_logger():
    """
    Fixture to create a mock logger.
    """
    mock_logger = MagicMock()
    mock_logger.echo = MagicMock()
    return mock_logger


@pytest.fixture
def test_paths():
    """
    Fixture to create test paths.
    """
    return {
        "checkpoint_location": "/tmp/checkpoint",
        "checkpoint_full_path": "/tmp/checkpoint/state.json",
    }


@pytest.fixture
def mock_state():
    """
    Fixture to create a mock IncrementalDownloadState.
    """
    model = IncrementalDownloadState()
    model.last_lookback_time = "2023-01-01T00:00:00"

    job = Job(job_id="job-123")
    session = JobSession(
        session_id="session-123",
        session_lifecycle_status="RUNNING",
        last_downloaded_sess_action_id=5,
    )
    job.sessions = [session]
    model.jobs = [job]
    return model


class TestIncrementalDownloadState:
    """Test cases for IncrementalDownloadState."""
    
    @pytest.fixture(autouse=True)
    def setup_method(self):
        """Reset any global state before each test."""
        # This ensures each test starts with a clean state
        pass

    @patch("datetime.datetime")
    @freeze_time("2025-05-26 12:00:00")
    def test_bootstrap_fresh_state(self, mock_datetime, mock_logger):
        """
        Test bootstrap_fresh_state with lookback minutes.
        """
        # Arrange
        bootstrap_lookback_in_minutes = 60

        # Act
        result = bootstrap_fresh_state(
            bootstrap_lookback_in_minutes,
            mock_logger.echo,
        )

        # Assert
        assert result.last_lookback_time == "2025-05-26T11:00:00"
        assert result.jobs == []

    @freeze_time("2025-05-26 12:00:00")
    def test_bootstrap_fresh_state_no_lookback(self, mock_logger):
        """
        Test bootstrap_fresh_state without lookback minutes.
        """
        # Act
        result = bootstrap_fresh_state(
            None,
            mock_logger.echo,
        )

        # Assert
        assert result.last_lookback_time == "2025-05-26T12:00:00"
        assert result.jobs == []

    def test_load_progress_from_state_file(self, mock_logger, mock_state, test_paths):
        """
        Test load_progress_from_state_file successfully loads the state file.
        """
        # Arrange
        mock_open_obj = mock_open(read_data=json.dumps(mock_state.to_dict()))

        # Act
        with patch("builtins.open", mock_open_obj):
            result = load_progress_from_state_file(
                test_paths["checkpoint_full_path"],
                mock_logger.echo,
            )

        # Assert
        mock_open_obj.assert_called_once_with(test_paths["checkpoint_full_path"], "r")
        assert result.last_lookback_time == mock_state.last_lookback_time
        assert len(result.jobs) == len(mock_state.jobs)

    def test_load_progress_from_state_file_exception(self, mock_logger, test_paths):
        """
        Test load_progress_from_state_file raises exception when file cannot be read.
        """
        # Arrange
        mock_open_obj = mock_open()
        mock_open_obj.side_effect = Exception("Failed to open file")

        # Act & Assert
        with patch("builtins.open", mock_open_obj):
            with pytest.raises(Exception):
                load_progress_from_state_file(
                    test_paths["checkpoint_full_path"],
                    mock_logger.echo,
                )

        mock_logger.echo.assert_called_once()

    def test_save_progress_to_state_file(self, mock_logger, mock_state, test_paths):
        """
        Test save_progress_to_state_file successfully saves the state file.
        """
        # Arrange
        mock_open_obj = mock_open()
        mock_makedirs = MagicMock()
        mock_fsync = MagicMock()
        mock_replace = MagicMock()
        mock_getpid = MagicMock(return_value=12345)

        # Act
        with patch("builtins.open", mock_open_obj), patch("os.makedirs", mock_makedirs), patch(
            "os.fsync", mock_fsync
        ), patch("os.replace", mock_replace), patch("os.getpid", mock_getpid):
            save_progress_to_state_file(
                test_paths["checkpoint_location"],
                test_paths["checkpoint_full_path"],
                mock_state,
                mock_logger.echo,
            )

        # Assert
        mock_makedirs.assert_called_once_with(
            os.path.dirname(test_paths["checkpoint_location"]), exist_ok=True
        )
        mock_open_obj.assert_called_once()
        mock_fsync.assert_called_once()
        mock_replace.assert_called_once()
        mock_logger.echo.assert_called_once()

    def test_save_progress_to_state_file_exception(self, mock_logger, mock_state, test_paths):
        """
        Test save_progress_to_state_file raises exception when file cannot be saved.
        """
        # Arrange
        mock_open_obj = mock_open()
        mock_open_obj.side_effect = Exception("Failed to open file")

        # Act & Assert
        with patch("builtins.open", mock_open_obj):
            with pytest.raises(Exception):
                save_progress_to_state_file(
                    test_paths["checkpoint_location"],
                    test_paths["checkpoint_full_path"],
                    mock_state,
                    mock_logger.echo,
                )

        mock_logger.echo.assert_called_once()

    def test_update_download_state_using_ongoing_sessions(self):
        """
        Test update_download_state_using_ongoing_sessions function.
        """
        # Create test data
        ongoing_sessions = [
            JobSession(
                session_id="session-123",
                session_lifecycle_status="RUNNING",
                last_downloaded_sess_action_id=5,
                job_id="job-123",
            ),
            JobSession(
                session_id="session-456",
                session_lifecycle_status="ENDED",
                last_downloaded_sess_action_id=3,
                job_id="job-123",
            ),
            JobSession(
                session_id="session-789",
                session_lifecycle_status="FAILED",
                last_downloaded_sess_action_id=7,
                job_id="job-456",
            ),
        ]
        command_start_time = "2025-06-12T15:00:00"

        # Mock the IncrementalDownloadState constructor to ensure a clean state
        with patch('deadline.job_attachments.incremental_downloads.incremental_download_state.IncrementalDownloadState') as mock_state_class:
            # Set up the mock to return a fresh state
            mock_instance = IncrementalDownloadState()
            mock_instance.jobs = []
            mock_state_class.return_value = mock_instance
            # Call the function
            result = update_download_state_using_ongoing_sessions(ongoing_sessions, command_start_time)

            # Verify the result
            assert result.last_lookback_time == command_start_time
            assert len(result.jobs) == 2  # Two unique job IDs

            # Check job-123
            job_123 = next((job for job in result.jobs if job.job_id == "job-123"), None)
            assert job_123 is not None
            assert len(job_123.sessions) == 2

            # Check sessions in job-123
            session_123 = next((s for s in job_123.sessions if s.session_id == "session-123"), None)
            assert session_123 is not None
            assert session_123.session_lifecycle_status == "RUNNING"
            assert session_123.last_downloaded_sess_action_id == 5

        session_456 = next((s for s in job_123.sessions if s.session_id == "session-456"), None)
        assert session_456 is not None
        assert session_456.session_lifecycle_status == "ENDED"
        assert session_456.last_downloaded_sess_action_id == 3

        # Check job-456
        job_456 = next((job for job in result.jobs if job.job_id == "job-456"), None)
        assert job_456 is not None
        assert len(job_456.sessions) == 1

        # Check session in job-456
        session_789 = next((s for s in job_456.sessions if s.session_id == "session-789"), None)
        assert session_789 is not None
        assert session_789.session_lifecycle_status == "FAILED"
        assert session_789.last_downloaded_sess_action_id == 7

    def test_update_download_state_with_existing_sessions(self):
        """
        Test update_download_state_using_ongoing_sessions with existing sessions.
        """
        # Create test data with updated session information
        ongoing_sessions = [
            JobSession(
                session_id="session-123",
                session_lifecycle_status="ENDED",
                last_downloaded_sess_action_id=8,
                job_id="job-123",
            ),
            JobSession(
                session_id="session-new",
                session_lifecycle_status="RUNNING",
                last_downloaded_sess_action_id=2,
                job_id="job-123",
            ),
        ]
        command_start_time = "2025-06-12T16:00:00"

        # Mock the IncrementalDownloadState constructor to ensure a clean state
        with patch('deadline.job_attachments.incremental_downloads.incremental_download_state.IncrementalDownloadState') as mock_state_class:
            # Set up the mock to return a fresh state
            mock_instance = IncrementalDownloadState()
            mock_instance.jobs = []
            mock_state_class.return_value = mock_instance
            
            # Call the function
            result = update_download_state_using_ongoing_sessions(ongoing_sessions, command_start_time)

            # Verify the result
            assert result.last_lookback_time == command_start_time
            assert len(result.jobs) == 1  # One unique job ID

            # Check job-123
            job_123 = result.jobs[0]
            assert job_123.job_id == "job-123"
            assert len(job_123.sessions) == 2

        # Check updated session
        session_123 = next((s for s in job_123.sessions if s.session_id == "session-123"), None)
        assert session_123 is not None
        assert session_123.session_lifecycle_status == "ENDED"  # Updated status
        assert session_123.last_downloaded_sess_action_id == 8  # Updated ID

        # Check new session
        session_new = next((s for s in job_123.sessions if s.session_id == "session-new"), None)
        assert session_new is not None
        assert session_new.session_lifecycle_status == "RUNNING"
        assert session_new.last_downloaded_sess_action_id == 2

    def test_to_dict(self):
        """
        Test to_dict method of IncrementalDownloadState.
        """
        # Create a state
        last_lookback_time = "2023-01-01T00:00:00"
        jobs_data: List[Dict[str, Any]] = [
            {
                "jobId": "job-123",
                "sessions": [
                    {
                        "sessionId": "session-123",
                        "sessionLifecycleStatus": "RUNNING",
                        "lastDownloadedSessActionId": 5,
                    }
                ],
            }
        ]

        # Create proper Job objects
        job_objects = []
        for job_dict in jobs_data:
            job = Job(job_id=str(job_dict["jobId"]))
            sessions = []
            for session_dict in cast(List[Dict[str, Any]], job_dict["sessions"]):
                session = JobSession(
                    session_id=str(session_dict["sessionId"]),
                    session_lifecycle_status=str(session_dict["sessionLifecycleStatus"]),
                    last_downloaded_sess_action_id=int(session_dict["lastDownloadedSessActionId"]),
                )
                sessions.append(session)
            job.sessions = sessions
            job_objects.append(job)

        state = IncrementalDownloadState(last_lookback_time=last_lookback_time, jobs=job_objects)

        # Convert to dict
        result = state.to_dict()

        # Expected result
        expected_jobs = []
        for job in job_objects:
            expected_jobs.append(job.to_dict())
        expected_result = {
            "lastLookbackTime": last_lookback_time,
            "jobs": expected_jobs,
        }

        # Assert
        assert result == expected_result

    def test_update_download_state_empty_sessions(self):
        """
        Test update_download_state_using_ongoing_sessions with empty sessions list.
        """
        # Create empty sessions list
        ongoing_sessions: List[JobSession] = []
        command_start_time = "2025-06-12T17:00:00"

        # Mock the IncrementalDownloadState constructor to ensure a clean state
        with patch('deadline.job_attachments.incremental_downloads.incremental_download_state.IncrementalDownloadState') as mock_state_class:
            # Set up the mock to return a fresh state
            mock_instance = IncrementalDownloadState()
            mock_instance.jobs = []
            mock_state_class.return_value = mock_instance
            
            # Call the function
            result = update_download_state_using_ongoing_sessions(ongoing_sessions, command_start_time)

            # Verify the result
            assert result.last_lookback_time == command_start_time
            assert len(result.jobs) == 0  # No jobs
