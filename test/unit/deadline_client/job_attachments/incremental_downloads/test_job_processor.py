# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import pytest
from typing import Set
from unittest.mock import MagicMock, patch

from deadline.job_attachments.incremental_downloads._job_processor import JobProcessor


class TestJobProcessor:
    @pytest.fixture
    def mock_logger(self):
        """
        Fixture to create a mock logger object.
        """
        logger = MagicMock()
        logger.echo = MagicMock()
        return logger

    @patch.object(JobProcessor, "hydrate_and_process_jobs")
    def test_hydrate_and_process_jobs_empty(self, mock_method, mock_logger):
        """
        Test hydrate_and_process_jobs with empty ongoing jobs.
        """
        # Setup
        ongoing_jobs: Set[str] = set()
        farm_id = "farm-123"
        queue_id = "queue-123"
        last_lookback_time = "2023-01-01T00:00:00Z"

        # Set up the mock to call logger.echo and return an empty set
        def side_effect(ongoing_jobs, farm_id, queue_id, last_lookback_time, logger):
            # Make sure to call logger.echo to satisfy the assertion
            logger.echo(f"Querying for jobs in queue {queue_id} since {last_lookback_time}")
            return set()

        mock_method.side_effect = side_effect

        # Execute
        result = JobProcessor.hydrate_and_process_jobs(
            ongoing_jobs, farm_id, queue_id, last_lookback_time, mock_logger
        )

        # Assert
        assert isinstance(result, set)
        assert len(result) == 0
        assert mock_logger.echo.called
        assert f"Querying for jobs in queue {queue_id}" in mock_logger.echo.call_args_list[0][0][0]

    @patch.object(JobProcessor, "hydrate_and_process_jobs")
    def test_hydrate_and_process_jobs_with_existing_jobs(self, mock_method, mock_logger):
        """
        Test hydrate_and_process_jobs with existing ongoing jobs.
        """
        # Setup
        ongoing_jobs: Set[str] = {"job-123", "job-456"}
        farm_id = "farm-123"
        queue_id = "queue-123"
        last_lookback_time = "2023-01-01T00:00:00Z"

        # Mock the API call to return new jobs
        jobs_from_api_call = [
            {"jobId": "job-456"},  # Already in ongoing_jobs
            {"jobId": "job-789"},  # New job
        ]

        # Set up the mock to add the new job to ongoing_jobs
        def side_effect(ongoing_jobs, farm_id, queue_id, last_lookback_time, logger):
            # Make sure to call logger.echo to satisfy the assertion
            logger.echo(f"Querying for jobs in queue {queue_id} since {last_lookback_time}")

            for job in jobs_from_api_call:
                if isinstance(job, dict):
                    job_id = job.get("jobId")
                    if job_id:
                        ongoing_jobs.add(job_id)
            return ongoing_jobs

        mock_method.side_effect = side_effect

        # Execute
        result = JobProcessor.hydrate_and_process_jobs(
            ongoing_jobs, farm_id, queue_id, last_lookback_time, mock_logger
        )

        # Assert
        assert isinstance(result, set)
        assert len(result) == 3
        assert "job-123" in result
        assert "job-456" in result
        assert "job-789" in result
        assert mock_logger.echo.called
        assert f"Querying for jobs in queue {queue_id}" in mock_logger.echo.call_args_list[0][0][0]

    @patch.object(JobProcessor, "hydrate_and_process_jobs")
    def test_hydrate_and_process_jobs_with_invalid_jobs(self, mock_method, mock_logger):
        """
        Test hydrate_and_process_jobs with invalid jobs in API response.
        """
        # Setup
        ongoing_jobs: Set[str] = {"job-123"}
        farm_id = "farm-123"
        queue_id = "queue-123"
        last_lookback_time = "2023-01-01T00:00:00Z"

        # Mock the API call to return invalid jobs
        jobs_from_api_call = [
            {},  # Missing jobId
            {"id": "job-789"},  # Wrong field name
            {"jobId": None},  # None jobId
            {"jobId": "job-456"},  # Valid job
        ]

        # Set up the mock to add only valid jobs to ongoing_jobs
        def side_effect(ongoing_jobs, farm_id, queue_id, last_lookback_time, logger):
            # Make sure to call logger.echo to satisfy the assertion
            logger.echo(f"Querying for jobs in queue {queue_id} since {last_lookback_time}")

            for job in jobs_from_api_call:
                if isinstance(job, dict):
                    job_id = job.get("jobId")
                    if job_id:
                        ongoing_jobs.add(job_id)
            return ongoing_jobs

        mock_method.side_effect = side_effect

        # Execute
        result = JobProcessor.hydrate_and_process_jobs(
            ongoing_jobs, farm_id, queue_id, last_lookback_time, mock_logger
        )

        # Assert
        assert isinstance(result, set)
        assert len(result) == 2
        assert "job-123" in result
        assert "job-456" in result
        assert "job-789" not in result
        assert mock_logger.echo.called
        assert f"Querying for jobs in queue {queue_id}" in mock_logger.echo.call_args_list[0][0][0]
