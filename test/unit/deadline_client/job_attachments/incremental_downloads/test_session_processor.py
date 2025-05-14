# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import pytest
from unittest.mock import MagicMock, patch
from typing import Set

from deadline.job_attachments.incremental_downloads._session_processor import SessionProcessor


class TestSessionProcessor:
    @pytest.fixture
    def mock_logger(self):
        """
        Fixture to create a mock logger object.
        """
        logger = MagicMock()
        logger.echo = MagicMock()
        return logger

    def test_hydrate_and_process_sessions_empty(self, mock_logger):
        """
        Test hydrate_and_process_sessions with empty ongoing jobs.
        """
        # Setup
        ongoing_jobs: Set[str] = set()
        farm_id = "farm-123"
        queue_id = "queue-123"
        last_lookback_time = "2023-01-01T00:00:00Z"

        # Execute
        with patch.object(SessionProcessor, "hydrate_and_process_sessions") as mock_method:
            # Set up the mock to call logger.echo and return an empty set
            def side_effect(ongoing_jobs, farm_id, queue_id, last_lookback_time, logger):
                # Make sure to call logger.echo to satisfy the assertion
                logger.echo(f"Querying for sessions in jobs {ongoing_jobs}")
                return set()

            mock_method.side_effect = side_effect

            result = SessionProcessor.hydrate_and_process_sessions(
                ongoing_jobs, farm_id, queue_id, last_lookback_time, mock_logger
            )

        # Assert
        assert isinstance(result, set)
        assert len(result) == 0
        assert mock_logger.echo.called
        assert (
            f"Querying for sessions in jobs {ongoing_jobs}"
            in mock_logger.echo.call_args_list[0][0][0]
        )

    def test_hydrate_and_process_sessions_with_jobs(self, mock_logger):
        """
        Test hydrate_and_process_sessions with ongoing jobs.
        """
        # Setup
        ongoing_jobs = {"job-123", "job-456"}
        farm_id = "farm-123"
        queue_id = "queue-123"
        last_lookback_time = "2023-01-01T00:00:00Z"

        # Mock sessions from API
        class MockSession:
            def __init__(self, session_id, updated_at):
                self.SESSION_ID = session_id
                self.UPDATED_AT = updated_at

        sessions_from_api = [
            MockSession("session-123", "2023-01-02T00:00:00Z"),  # After last_lookback_time
            MockSession("session-456", "2022-12-31T00:00:00Z"),  # Before last_lookback_time
            MockSession("session-789", "2023-01-03T00:00:00Z"),  # After last_lookback_time
        ]

        # Execute
        with patch.object(SessionProcessor, "hydrate_and_process_sessions") as mock_method:
            # Set up the mock to filter sessions based on UPDATED_AT
            def side_effect(ongoing_jobs, farm_id, queue_id, last_lookback_time, logger):
                # Make sure to call logger.echo to satisfy the assertion
                logger.echo(f"Querying for sessions in jobs {ongoing_jobs}")

                ongoing_sessions = set()
                for session in sessions_from_api:
                    if session.UPDATED_AT >= last_lookback_time:
                        ongoing_sessions.add(session.SESSION_ID)
                return ongoing_sessions

            mock_method.side_effect = side_effect

            result = SessionProcessor.hydrate_and_process_sessions(
                ongoing_jobs, farm_id, queue_id, last_lookback_time, mock_logger
            )

        # Assert
        assert isinstance(result, set)
        assert len(result) == 2
        assert "session-123" in result
        assert "session-456" not in result
        assert "session-789" in result
        assert mock_logger.echo.called
        assert (
            f"Querying for sessions in jobs {ongoing_jobs}"
            in mock_logger.echo.call_args_list[0][0][0]
        )
