# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import unittest
from unittest.mock import MagicMock
from typing import List

from deadline.job_attachments.incremental_downloads.job_processor import (
    get_list_of_ongoing_jobs_on_queue,
)


class TestJobProcessor(unittest.TestCase):
    """Test cases for job processor functions."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_boto3_session = MagicMock()
        self.mock_deadline_client = MagicMock()
        self.mock_boto3_session.client.return_value = self.mock_deadline_client

        self.farm_id = "farm-12345"
        self.queue_id = "queue-67890"
        self.last_lookback_time = "2023-01-01T00:00:00"
        self.last_known_jobs = set(["job-1", "job-2", "job-3"])

        # Mock print function
        self.print_messages: List[str] = []
        self.mock_print = lambda msg: self.print_messages.append(msg)

    def test_get_list_of_ongoing_jobs_single_page(self):
        """Test getting ongoing jobs with a single page of results."""
        # Mock the search_jobs response for a single page
        self.mock_deadline_client.search_jobs.return_value = {
            "jobs": [{"jobId": "job-4"}, {"jobId": "job-5"}, {"jobId": "job-6"}],
            "nextItemOffset": None,
        }

        # Call the function
        result = get_list_of_ongoing_jobs_on_queue(
            boto3_session=self.mock_boto3_session,
            last_known_set_of_ongoing_jobs=self.last_known_jobs,
            farm_id=self.farm_id,
            queue_id=self.queue_id,
            last_lookback_time=self.last_lookback_time,
            print_function_callback=self.mock_print,
        )

        # Verify results
        expected_result = {"job-1", "job-2", "job-3", "job-4", "job-5", "job-6"}
        self.assertEqual(result, expected_result)

        # Verify API call
        self.mock_deadline_client.search_jobs.assert_called_once()
        call_args = self.mock_deadline_client.search_jobs.call_args[1]
        self.assertEqual(call_args["farmId"], self.farm_id)
        self.assertEqual(call_args["queueIds"], [self.queue_id])
        self.assertEqual(call_args["pageSize"], 100)
        self.assertEqual(call_args["itemOffset"], 0)

    def test_get_list_of_ongoing_jobs_multiple_pages(self):
        """Test getting ongoing jobs with multiple pages of results."""
        # Set up mock responses for multiple pages
        self.mock_deadline_client.search_jobs.side_effect = [
            # First page - jobs 4-103
            {"jobs": [{"jobId": f"job-{i}"} for i in range(4, 104)], "nextItemOffset": 100},
            # Second page - jobs 94-193 (with 10 overlapping jobs: 94-103)
            {
                "jobs": [{"jobId": f"job-{i}"} for i in range(94, 194)],
                "nextItemOffset": 190,
            },
            # Third page - jobs 184-249 (with 10 overlapping jobs: 184-193)
            {
                "jobs": [{"jobId": f"job-{i}"} for i in range(184, 250)],
                "nextItemOffset": None,
            },
        ]

        # Call the function
        result = get_list_of_ongoing_jobs_on_queue(
            boto3_session=self.mock_boto3_session,
            last_known_set_of_ongoing_jobs=self.last_known_jobs,
            farm_id=self.farm_id,
            queue_id=self.queue_id,
            last_lookback_time=self.last_lookback_time,
            print_function_callback=self.mock_print,
        )

        expected_job_count = 249
        self.assertEqual(len(result), expected_job_count)

        # Verify API calls
        self.assertEqual(self.mock_deadline_client.search_jobs.call_count, 3)

        # Check first call
        first_call_args = self.mock_deadline_client.search_jobs.call_args_list[0][1]
        self.assertEqual(first_call_args["farmId"], self.farm_id)
        self.assertEqual(first_call_args["queueIds"], [self.queue_id])
        self.assertEqual(first_call_args["pageSize"], 100)
        self.assertEqual(first_call_args["itemOffset"], 0)

        # Check second call
        second_call_args = self.mock_deadline_client.search_jobs.call_args_list[1][1]
        self.assertEqual(second_call_args["itemOffset"], 90)  # 100 - OVERLAP_SIZE

        # Check third call
        third_call_args = self.mock_deadline_client.search_jobs.call_args_list[2][1]
        self.assertEqual(third_call_args["itemOffset"], 180)  # 190 - OVERLAP_SIZE

    def test_get_list_of_ongoing_jobs_with_inconsistency(self):
        """Test handling of inconsistency in pagination overlap."""
        # Set up mock responses with an inconsistency
        self.mock_deadline_client.search_jobs.side_effect = [
            # First page
            {"jobs": [{"jobId": f"job-{i}"} for i in range(4, 104)], "nextItemOffset": 100},
            # Second page with inconsistent overlap
            {
                "jobs": [{"jobId": f"job-{i + 1000}"} for i in range(94, 194)],  # Different IDs
                "nextItemOffset": 190,
            },
            # First page again after restart
            {"jobs": [{"jobId": f"job-{i}"} for i in range(4, 104)], "nextItemOffset": 100},
            # Second page with consistent overlap
            {"jobs": [{"jobId": f"job-{i}"} for i in range(94, 194)], "nextItemOffset": 190},
            # Third page
            {"jobs": [{"jobId": f"job-{i}"} for i in range(184, 250)], "nextItemOffset": None},
        ]

        # Call the function
        result = get_list_of_ongoing_jobs_on_queue(
            boto3_session=self.mock_boto3_session,
            last_known_set_of_ongoing_jobs=self.last_known_jobs,
            farm_id=self.farm_id,
            queue_id=self.queue_id,
            last_lookback_time=self.last_lookback_time,
            print_function_callback=self.mock_print,
        )

        # Calculate expected unique job IDs:
        # - 3 original jobs: job-1, job-2, job-3
        # - First page after restart: 100 jobs (job-4 to job-103)
        # - Second page after restart: 90 new jobs (job-104 to job-193) after removing 10 overlapping jobs
        # - Third page after restart: 56 new jobs (job-194 to job-249) after removing 10 overlapping jobs
        # Total: 3 + 100 + 90 + 56 = 249
        expected_job_count = 249

        # Debug output to help diagnose the issue
        if len(result) != expected_job_count:
            for msg in self.print_messages:
                print(f"DEBUG: {msg}")
            print(f"DEBUG: Expected {expected_job_count} jobs, got {len(result)}")
            print(f"DEBUG: Result contains {len(result)} jobs: {sorted(list(result))[:10]}...")

        self.assertEqual(len(result), expected_job_count)

        # Verify API calls - should be 5 calls due to the restart
        self.assertEqual(self.mock_deadline_client.search_jobs.call_count, 5)

        # Check for restart message
        restart_messages = [msg for msg in self.print_messages if "inconsistency" in msg.lower()]
        self.assertEqual(len(restart_messages), 1)

        # Check that after restart, we start from offset 0
        restart_call_args = self.mock_deadline_client.search_jobs.call_args_list[2][1]
        self.assertEqual(restart_call_args["itemOffset"], 0)

    def test_get_list_of_ongoing_jobs_empty_response(self):
        """Test handling of empty response."""
        # Mock an empty response
        self.mock_deadline_client.search_jobs.return_value = {"jobs": [], "nextItemOffset": None}

        # Call the function
        result = get_list_of_ongoing_jobs_on_queue(
            boto3_session=self.mock_boto3_session,
            last_known_set_of_ongoing_jobs=self.last_known_jobs,
            farm_id=self.farm_id,
            queue_id=self.queue_id,
            last_lookback_time=self.last_lookback_time,
            print_function_callback=self.mock_print,
        )

        # Verify results - should just be the original jobs
        self.assertEqual(result, self.last_known_jobs)

        # Verify API call
        self.mock_deadline_client.search_jobs.assert_called_once()

    def test_get_list_of_ongoing_jobs_api_error(self):
        """Test handling of API errors."""
        # Mock an API error
        from botocore.exceptions import ClientError

        self.mock_deadline_client.search_jobs.side_effect = ClientError(
            {"Error": {"Code": "SomeError", "Message": "API Error"}}, "search_jobs"
        )

        # Call the function and expect an exception
        with self.assertRaises(Exception):
            get_list_of_ongoing_jobs_on_queue(
                boto3_session=self.mock_boto3_session,
                last_known_set_of_ongoing_jobs=self.last_known_jobs,
                farm_id=self.farm_id,
                queue_id=self.queue_id,
                last_lookback_time=self.last_lookback_time,
                print_function_callback=self.mock_print,
            )
