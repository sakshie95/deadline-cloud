# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import pytest
from unittest.mock import MagicMock

from deadline.job_attachments.incremental_downloads.state_checkpoint_hydrator import (
    StateCheckpointHydrator,
)
from deadline.job_attachments.incremental_downloads.models import StateFileModel, HydrationState


class TestStateCheckpointHydrator:
    @pytest.fixture
    def mock_logger(self):
        """
        Fixture to create a mock logger object.
        """
        logger = MagicMock()
        logger.echo = MagicMock()
        return logger

    @pytest.fixture
    def empty_state_file_model(self):
        """
        Fixture to create an empty StateFileModel.
        """
        return StateFileModel(last_lookback_time="2023-01-01T00:00:00Z", jobs=[])

    @pytest.fixture
    def populated_state_file_model(self):
        """
        Fixture to create a populated StateFileModel with jobs and sessions.
        """
        model = StateFileModel(last_lookback_time="2023-01-01T00:00:00Z")
        model.jobs = [
            {
                "jobId": "job-123",
                "sessions": [
                    {
                        "sessionId": "session-123",
                        "lifecycleStatus": "RUNNING",
                        "lastDownloadedSessActionId": 5,
                    },
                    {
                        "sessionId": "session-456",
                        "lifecycleStatus": "ENDED",
                        "lastDownloadedSessActionId": 10,
                    },
                ],
            },
            {
                "jobId": "job-456",
                "sessions": [
                    {
                        "sessionId": "session-789",
                        "lifecycleStatus": "FAILED",
                        "lastDownloadedSessActionId": 3,
                    }
                ],
            },
        ]
        return model

    def test_initialize_in_memory_maps_from_current_progress_empty(
        self, mock_logger, empty_state_file_model
    ):
        """
        Test initialize_in_memory_maps_from_current_progress with empty state file.
        """
        # Execute
        result = StateCheckpointHydrator.initialize_in_memory_maps_from_current_progress(
            empty_state_file_model, mock_logger
        )

        # Assert
        assert isinstance(result, HydrationState)
        assert len(result.ongoing_jobs) == 0
        assert len(result.session_action_index_map) == 0
        assert len(result.session_to_job_map) == 0
        assert len(result.session_to_lifecycle_status_map) == 0
        assert len(result.auxiliary_session_action_status_mapping) == 0
        assert len(result.session_to_last_finished_action_id_map) == 0
        assert mock_logger.echo.call_count == 3

    def test_initialize_in_memory_maps_from_current_progress_populated(
        self, mock_logger, populated_state_file_model
    ):
        """
        Test initialize_in_memory_maps_from_current_progress with populated state file.
        """
        # Execute
        result = StateCheckpointHydrator.initialize_in_memory_maps_from_current_progress(
            populated_state_file_model, mock_logger
        )

        # Assert
        assert isinstance(result, HydrationState)
        assert len(result.ongoing_jobs) == 2
        assert "job-123" in result.ongoing_jobs
        assert "job-456" in result.ongoing_jobs

        assert len(result.session_action_index_map) == 3
        assert result.session_action_index_map["session-123"] == 5
        assert result.session_action_index_map["session-456"] == 10
        assert result.session_action_index_map["session-789"] == 3

        assert len(result.session_to_job_map) == 3
        assert result.session_to_job_map["session-123"] == "job-123"
        assert result.session_to_job_map["session-456"] == "job-123"
        assert result.session_to_job_map["session-789"] == "job-456"

        assert len(result.session_to_lifecycle_status_map) == 3
        assert result.session_to_lifecycle_status_map["session-123"] == "RUNNING"
        assert result.session_to_lifecycle_status_map["session-456"] == "ENDED"
        assert result.session_to_lifecycle_status_map["session-789"] == "FAILED"

        assert len(result.auxiliary_session_action_status_mapping) == 0
        assert len(result.session_to_last_finished_action_id_map) == 0

        assert mock_logger.echo.call_count == 3

    def test_initialize_in_memory_maps_from_current_progress_missing_fields(self, mock_logger):
        """
        Test initialize_in_memory_maps_from_current_progress with missing fields in state file.
        """
        # Create a state file model with missing fields
        model = StateFileModel(last_lookback_time="2023-01-01T00:00:00Z")
        model.jobs = [
            {
                "jobId": "job-123",
                "sessions": [
                    {
                        "sessionId": "session-123",
                        # Missing lifecycleStatus
                        "lastDownloadedSessActionId": 5,
                    },
                    {
                        # Missing sessionId
                        "lifecycleStatus": "ENDED",
                        "lastDownloadedSessActionId": 10,
                    },
                ],
            },
            {
                # Missing jobId
                "sessions": [
                    {
                        "sessionId": "session-789",
                        "lifecycleStatus": "FAILED",
                        # Missing lastDownloadedSessActionId
                    }
                ]
            },
        ]

        # Execute
        result = StateCheckpointHydrator.initialize_in_memory_maps_from_current_progress(
            model, mock_logger
        )

        # Assert
        assert isinstance(result, HydrationState)
        assert len(result.ongoing_jobs) == 1
        assert "job-123" in result.ongoing_jobs

        assert len(result.session_action_index_map) == 1
        assert result.session_action_index_map["session-123"] == 5

        assert len(result.session_to_job_map) == 1
        assert result.session_to_job_map["session-123"] == "job-123"

        assert len(result.session_to_lifecycle_status_map) == 1
        assert "session-123" in result.session_to_lifecycle_status_map
        assert result.session_to_lifecycle_status_map["session-123"] is None

        assert mock_logger.echo.call_count == 3

    def test_update_download_progress_state_empty(self, mock_logger):
        """
        Test update_download_progress_state with empty inputs.
        """
        # Execute
        result = StateCheckpointHydrator.update_download_progress_state(
            mock_logger, set(), {}, {}, {}, {}
        )

        # Assert
        assert isinstance(result, list)
        assert len(result) == 0

    def test_update_download_progress_state_populated(self, mock_logger):
        """
        Test update_download_progress_state with populated inputs.
        """
        # Setup
        ongoing_sessions = {"session-123", "session-456", "session-789"}
        session_action_index_map = {"session-123": 5, "session-456": 10, "session-789": 3}
        session_to_job_map = {
            "session-123": "job-123",
            "session-456": "job-123",
            "session-789": "job-456",
        }
        session_to_last_finished_action_id_map = {
            "session-123": 5,
            "session-456": 10,  # Same as session_action_index_map - should be skipped
            "session-789": 3,
        }
        session_to_lifecycle_status_map = {
            "session-123": "RUNNING",
            "session-456": "ENDED",  # This session should be skipped
            "session-789": "FAILED",
        }

        # Execute
        result = StateCheckpointHydrator.update_download_progress_state(
            mock_logger,
            ongoing_sessions,
            session_action_index_map,
            session_to_job_map,
            session_to_last_finished_action_id_map,
            session_to_lifecycle_status_map,
        )

        # Assert
        assert isinstance(result, list)
        assert len(result) == 2  # Two jobs

        # Check job-123
        job_123 = next((job for job in result if job["jobId"] == "job-123"), None)
        assert job_123 is not None
        assert (
            len(job_123["sessions"]) == 1
        )  # Only session-123 should be included (session-456 is skipped)

        # Check session-123 in job-123
        session_123 = next(
            (session for session in job_123["sessions"] if session["sessionId"] == "session-123"),
            None,
        )
        assert session_123 is not None
        assert session_123["lastDownloadedSessActionId"] == 5
        assert session_123["sessionLifecycleStatus"] == "RUNNING"

        # Check session-456 in job-123 (should be skipped because it's ENDED and fully downloaded)
        session_456 = next(
            (session for session in job_123["sessions"] if session["sessionId"] == "session-456"),
            None,
        )
        assert session_456 is None

        # Check job-456
        job_456 = next((job for job in result if job["jobId"] == "job-456"), None)
        assert job_456 is not None
        assert len(job_456["sessions"]) == 1

        # Check session-789 in job-456
        session_789 = next(
            (session for session in job_456["sessions"] if session["sessionId"] == "session-789"),
            None,
        )
        assert session_789 is not None
        assert session_789["lastDownloadedSessActionId"] == 3
        assert session_789["sessionLifecycleStatus"] == "FAILED"

    def test_update_download_progress_state_with_ended_sessions(self, mock_logger):
        """
        Test update_download_progress_state with ENDED sessions that have different action IDs.
        """
        # Setup
        ongoing_sessions = {"session-123", "session-456"}
        session_action_index_map = {"session-123": 5, "session-456": 10}
        session_to_job_map = {"session-123": "job-123", "session-456": "job-123"}
        session_to_last_finished_action_id_map = {
            "session-123": 5,  # Same as session_action_index_map - should be skipped
            "session-456": 12,  # Different from session_action_index_map - should be included
        }
        session_to_lifecycle_status_map = {"session-123": "ENDED", "session-456": "ENDED"}

        # Execute
        result = StateCheckpointHydrator.update_download_progress_state(
            mock_logger,
            ongoing_sessions,
            session_action_index_map,
            session_to_job_map,
            session_to_last_finished_action_id_map,
            session_to_lifecycle_status_map,
        )

        # Assert
        assert isinstance(result, list)
        assert len(result) == 1  # One job

        # Check job-123
        job_123 = result[0]
        assert job_123["jobId"] == "job-123"
        assert len(job_123["sessions"]) == 1  # Only session-456 should be included

        # Check session-456 in job-123
        session_456 = job_123["sessions"][0]
        assert session_456["sessionId"] == "session-456"
        assert session_456["lastDownloadedSessActionId"] == 10
        assert session_456["sessionLifecycleStatus"] == "ENDED"

        # Verify that session-123 was skipped
        assert mock_logger.echo.called
        assert "Skipping session session-123" in mock_logger.echo.call_args_list[0][0][0]
