# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import datetime
from unittest.mock import MagicMock
import pytest
from botocore.exceptions import ClientError
from typing import cast

from deadline.client.exceptions import DeadlineOperationError
from deadline.job_attachments.incremental_downloads.updated_jobs_fetcher import (
    get_jobs_updated_since_timestamp,
    PAGE_SIZE,
    OVERLAP_SIZE,
    JobFetchFailure,
)

# Constants for testing
MOCK_FARM_ID = "farm-0123456789abcdef"
MOCK_QUEUE_ID = "queue-0123456789abcdef"
MOCK_TIMESTAMP = datetime.datetime(2023, 1, 1, 0, 0, 0)


@pytest.fixture
def mock_boto3_session():
    """Create a mock boto3 session for tests."""
    session = MagicMock()
    deadline_client = MagicMock()
    session.client.return_value = deadline_client
    return session


@pytest.fixture
def mock_logger():
    """
    Fixture to create a mock logger.
    """
    mock_logger = MagicMock()
    mock_logger.echo = MagicMock()
    return mock_logger


def create_mock_jobs_response(start_id, end_id, next_offset=None, modify_ids=False):
    """Helper to create a mock jobs response with the given range of job IDs."""
    job_ids = range(start_id, end_id + 1)
    jobs = [{"jobId": f"job-{i}-modified" if modify_ids else f"job-{i}"} for i in job_ids]
    response = {"jobs": jobs}
    if next_offset is not None:
        response["nextItemOffset"] = next_offset
    return response


def verify_search_jobs_call(mock_session, call_index, expected_offset):
    """Helper to verify a search_jobs call with the expected parameters."""
    call_args = mock_session.client().search_jobs.call_args_list[call_index][1]
    assert call_args["farmId"] == MOCK_FARM_ID
    assert call_args["queueIds"] == [MOCK_QUEUE_ID]
    assert call_args["pageSize"] == PAGE_SIZE
    assert call_args["itemOffset"] == expected_offset

    # Verify filter expressions
    filter_expressions = call_args["filterExpressions"]
    assert filter_expressions["operator"] == "AND"
    assert len(filter_expressions["filters"]) == 1

    date_time_filter = filter_expressions["filters"][0]["dateTimeFilter"]
    assert date_time_filter["name"] == "UPDATED_AT"
    assert date_time_filter["dateTime"] == MOCK_TIMESTAMP
    assert date_time_filter["operator"] == "GREATER_THAN"


def test_get_list_of_ongoing_jobs_single_page(mock_boto3_session, mock_logger):
    """Test getting ongoing jobs with a single page of results."""
    # Setup mock response for a single page
    mock_boto3_session.client().search_jobs.return_value = create_mock_jobs_response(1, 10)

    # Call the function
    result = get_jobs_updated_since_timestamp(
        boto3_session=mock_boto3_session,
        farm_id=MOCK_FARM_ID,
        queue_id=MOCK_QUEUE_ID,
        timestamp=MOCK_TIMESTAMP,
        print_function_callback=mock_logger.echo,
    )

    # Verify results
    expected_result = {f"job-{i}" for i in range(1, 11)}
    assert result == expected_result


def test_get_list_of_ongoing_jobs_empty_response(mock_boto3_session, mock_logger):
    """Test handling of empty response."""
    # Setup mock response with no jobs
    mock_boto3_session.client().search_jobs.return_value = {"jobs": [], "nextItemOffset": None}

    # Call the function
    result = get_jobs_updated_since_timestamp(
        boto3_session=mock_boto3_session,
        farm_id=MOCK_FARM_ID,
        queue_id=MOCK_QUEUE_ID,
        timestamp=MOCK_TIMESTAMP,
        print_function_callback=mock_logger.echo,
    )

    # Verify results - should be an empty set
    assert result == set()
    assert len(result) == 0


def test_get_list_of_ongoing_jobs_api_error(mock_boto3_session, mock_logger):
    """Test handling of API errors."""
    # Setup mock to raise ClientError
    error_response = {"Error": {"Code": "SomeError", "Message": "API Error"}}
    mock_boto3_session.client().search_jobs.side_effect = ClientError(error_response, "search_jobs")

    # Call the function and expect a DeadlineOperationError
    with pytest.raises(DeadlineOperationError) as excinfo:
        get_jobs_updated_since_timestamp(
            boto3_session=mock_boto3_session,
            farm_id=MOCK_FARM_ID,
            queue_id=MOCK_QUEUE_ID,
            timestamp=MOCK_TIMESTAMP,
            print_function_callback=mock_logger.echo,
        )

    # Verify the error message
    assert "Failed to get Jobs from Deadline" in str(excinfo.value)
    assert "API Error" in str(excinfo.value)


def test_get_list_of_ongoing_jobs_multiple_restarts(mock_boto3_session, mock_logger):
    """Test handling of multiple inconsistencies requiring multiple restarts."""
    # Create base timestamp
    base_timestamp = datetime.datetime(2023, 1, 2, 0, 0, 0)

    # Create first batch with increasing timestamps
    batch1 = []
    for i in range(1, PAGE_SIZE + 1):
        job_timestamp = base_timestamp.replace(minute=i % 60, second=i % 60)
        batch1.append({"jobId": f"job-{i}", "updatedAt": job_timestamp})

    # Create inconsistent batch 1
    batch2_inconsistent1 = []
    # First OVERLAP_SIZE jobs are different (inconsistent with batch1)
    for i in range(1, OVERLAP_SIZE + 1):
        job_timestamp = base_timestamp.replace(hour=1, minute=i % 60, second=i % 60)
        batch2_inconsistent1.append(
            {
                "jobId": f"job-{i}-different1",  # Different job IDs
                "updatedAt": job_timestamp,
            }
        )

    # Add the rest of batch2_inconsistent1
    for i in range(OVERLAP_SIZE + 1, PAGE_SIZE + 1):
        job_timestamp = base_timestamp.replace(hour=1, minute=i % 60, second=i % 60)
        batch2_inconsistent1.append({"jobId": f"job-{PAGE_SIZE + i}-1", "updatedAt": job_timestamp})

    # Create inconsistent batch 2
    batch2_inconsistent2 = []
    # First OVERLAP_SIZE jobs are different (inconsistent with batch1)
    for i in range(1, OVERLAP_SIZE + 1):
        job_timestamp = base_timestamp.replace(hour=1, minute=i % 60, second=i % 60)
        batch2_inconsistent2.append(
            {
                "jobId": f"job-{i}-different2",  # Different job IDs
                "updatedAt": job_timestamp,
            }
        )

    # Add the rest of batch2_inconsistent2
    for i in range(OVERLAP_SIZE + 1, PAGE_SIZE + 1):
        job_timestamp = base_timestamp.replace(hour=1, minute=i % 60, second=i % 60)
        batch2_inconsistent2.append({"jobId": f"job-{PAGE_SIZE + i}-2", "updatedAt": job_timestamp})

    # Create consistent batch
    batch2_consistent = []
    # First OVERLAP_SIZE jobs should match the last OVERLAP_SIZE jobs from batch1
    for i in range(PAGE_SIZE - OVERLAP_SIZE + 1, PAGE_SIZE + 1):
        batch2_consistent.append({"jobId": f"job-{i}", "updatedAt": batch1[i - 1]["updatedAt"]})

    # Add the rest of batch2_consistent
    for i in range(PAGE_SIZE + 1, 2 * PAGE_SIZE - OVERLAP_SIZE + 1):
        job_timestamp = base_timestamp.replace(hour=1, minute=i % 60, second=i % 60)
        batch2_consistent.append({"jobId": f"job-{i}", "updatedAt": job_timestamp})

    # Create final batch
    batch3 = []
    # First OVERLAP_SIZE jobs should match the last OVERLAP_SIZE jobs from batch2_consistent
    for i in range(0, OVERLAP_SIZE):
        original_index = len(batch2_consistent) - OVERLAP_SIZE + i
        batch3.append(
            {
                "jobId": batch2_consistent[original_index]["jobId"],
                "updatedAt": batch2_consistent[original_index]["updatedAt"],
            }
        )

    # Add the rest of batch3
    for i in range(2 * PAGE_SIZE - OVERLAP_SIZE + 1, 251):
        job_timestamp = base_timestamp.replace(hour=2, minute=i % 60, second=i % 60)
        batch3.append({"jobId": f"job-{i}", "updatedAt": job_timestamp})

    # Setup mock responses with multiple inconsistencies
    mock_boto3_session.client().search_jobs.side_effect = [
        {"jobs": batch1, "totalResults": 250},
        {"jobs": batch2_inconsistent1, "totalResults": 250},  # First inconsistent batch
        {"jobs": batch1, "totalResults": 250},  # Restart 1
        {"jobs": batch2_inconsistent2, "totalResults": 250},  # Second inconsistent batch
        {"jobs": batch1, "totalResults": 250},  # Restart 2
        {"jobs": batch2_consistent, "totalResults": 250},  # Finally consistent batch
        {"jobs": batch3, "totalResults": 250},
        {"jobs": [], "totalResults": 0},
    ]

    # Call the function
    result = get_jobs_updated_since_timestamp(
        boto3_session=mock_boto3_session,
        farm_id=MOCK_FARM_ID,
        queue_id=MOCK_QUEUE_ID,
        timestamp=MOCK_TIMESTAMP,
        print_function_callback=mock_logger.echo,
    )

    # Collect all expected job IDs
    expected_ids = set()
    for job in batch1:
        expected_ids.add(job["jobId"])
    for job in batch2_consistent[OVERLAP_SIZE:]:  # Skip overlap jobs
        expected_ids.add(job["jobId"])
    for job in batch3[OVERLAP_SIZE:]:  # Skip overlap jobs
        expected_ids.add(job["jobId"])

    # Verify results - should have 250 unique job IDs after restarts
    assert result == expected_ids
    assert len(result) == 250


def test_get_jobs_same_updated_at_timestamp(mock_boto3_session, mock_logger):
    """Test handling when all jobs in a page have the same updatedAt timestamp."""
    # Create a timestamp for all jobs
    same_timestamp = datetime.datetime(2023, 1, 2, 0, 0, 0)

    # Setup mock response with all jobs having the same updatedAt timestamp
    # Make sure the last job has the same timestamp as the threshold_timestamp
    jobs = []
    for i in range(1, PAGE_SIZE + 1):
        jobs.append({"jobId": f"job-{i}", "updatedAt": same_timestamp})

    # Make sure the search_jobs is only called once to avoid infinite loop
    mock_boto3_session.client().search_jobs.return_value = {"jobs": jobs, "totalResults": PAGE_SIZE}

    # Call the function and expect a JobFetchFailure
    with pytest.raises(JobFetchFailure) as excinfo:
        get_jobs_updated_since_timestamp(
            boto3_session=mock_boto3_session,
            farm_id=MOCK_FARM_ID,
            queue_id=MOCK_QUEUE_ID,
            timestamp=same_timestamp,  # Use the same timestamp as the jobs
            print_function_callback=mock_logger.echo,
        )

    # Verify the error message
    assert "Failure fetching jobs as updatedAt is the same for all jobs in page" in str(
        excinfo.value
    )


def test_get_jobs_with_total_results(mock_boto3_session, mock_logger):
    """Test handling of totalResults field in the response."""
    # Setup mock response with totalResults
    mock_boto3_session.client().search_jobs.return_value = {
        "jobs": [{"jobId": f"job-{i}"} for i in range(1, 11)],
        "totalResults": 10,
    }

    # Call the function
    result = get_jobs_updated_since_timestamp(
        boto3_session=mock_boto3_session,
        farm_id=MOCK_FARM_ID,
        queue_id=MOCK_QUEUE_ID,
        timestamp=MOCK_TIMESTAMP,
        print_function_callback=mock_logger.echo,
    )

    # Verify results
    expected_result = {f"job-{i}" for i in range(1, 11)}
    assert result == expected_result
    assert len(result) == 10


def test_get_all_jobs_without_inconsistency(mock_boto3_session, mock_logger):
    """
    Test that all jobs are fetched correctly when there are more than PAGE_SIZE jobs,
    without any inconsistency between batches.
    """
    # Create timestamps for different batches - ensure they are different and increasing
    base_timestamp = datetime.datetime(2023, 1, 2, 0, 0, 0)

    # First batch - 100 jobs with strictly increasing timestamps
    batch1 = []
    for i in range(1, PAGE_SIZE + 1):
        # Each job has a unique timestamp, strictly increasing
        job_timestamp = base_timestamp + datetime.timedelta(minutes=i)
        batch1.append({"jobId": f"job-{i}", "updatedAt": job_timestamp})

    # Last timestamp from batch1 - this is crucial for the pagination to work
    batch1_last_timestamp: datetime.datetime = cast(datetime.datetime, batch1[-1]["updatedAt"])

    # Second batch - 100 jobs with strictly increasing timestamps
    batch2 = []
    # First OVERLAP_SIZE jobs should match the last OVERLAP_SIZE jobs from batch1
    for i in range(PAGE_SIZE - OVERLAP_SIZE + 1, PAGE_SIZE + 1):
        original_index = i - 1
        batch2.append(
            {
                "jobId": batch1[original_index]["jobId"],
                "updatedAt": batch1[original_index]["updatedAt"],
            }
        )

    # Add the rest of batch2 with timestamps greater than batch1's last job
    for i in range(PAGE_SIZE + 1, 2 * PAGE_SIZE - OVERLAP_SIZE + 1):
        # Each job has a unique timestamp, strictly increasing
        job_timestamp = batch1_last_timestamp + datetime.timedelta(minutes=i - PAGE_SIZE + 1)
        batch2.append({"jobId": f"job-{i}", "updatedAt": job_timestamp})

    # Last timestamp from batch2
    batch2_last_timestamp: datetime.datetime = cast(datetime.datetime, batch2[-1]["updatedAt"])

    # Third batch - remaining jobs with strictly increasing timestamps
    batch3 = []
    # First OVERLAP_SIZE jobs should match the last OVERLAP_SIZE jobs from batch2
    for i in range(0, OVERLAP_SIZE):
        original_index = len(batch2) - OVERLAP_SIZE + i
        batch3.append(
            {
                "jobId": batch2[original_index]["jobId"],
                "updatedAt": batch2[original_index]["updatedAt"],
            }
        )

    # Add the rest of batch3 with timestamps greater than batch2's last job
    for i in range(2 * PAGE_SIZE - OVERLAP_SIZE + 1, 3 * PAGE_SIZE - 2 * OVERLAP_SIZE + 1):
        # Each job has a unique timestamp, strictly increasing
        job_timestamp = batch2_last_timestamp + datetime.timedelta(
            minutes=i - (2 * PAGE_SIZE - OVERLAP_SIZE) + 1
        )
        batch3.append({"jobId": f"job-{i}", "updatedAt": job_timestamp})

    # Setup mock responses with empty final response to end the loop
    mock_boto3_session.client().search_jobs.side_effect = [
        {"jobs": batch1, "totalResults": 300},
        {"jobs": batch2, "totalResults": 300},
        {"jobs": batch3, "totalResults": 300},
        {"jobs": [], "totalResults": 0},
    ]

    # Call the function
    result = get_jobs_updated_since_timestamp(
        boto3_session=mock_boto3_session,
        farm_id=MOCK_FARM_ID,
        queue_id=MOCK_QUEUE_ID,
        timestamp=MOCK_TIMESTAMP,
        print_function_callback=mock_logger.echo,
    )

    # Collect all expected job IDs
    expected_ids = set()
    for job in batch1:
        expected_ids.add(job["jobId"])
    for job in batch2[OVERLAP_SIZE:]:  # Skip overlap jobs
        expected_ids.add(job["jobId"])
    for job in batch3[OVERLAP_SIZE:]:  # Skip overlap jobs
        expected_ids.add(job["jobId"])

    # Verify results - should have all unique job IDs
    assert result == expected_ids

    # Verify that we got the expected number of jobs
    expected_job_count = PAGE_SIZE + (PAGE_SIZE - OVERLAP_SIZE) + (PAGE_SIZE - OVERLAP_SIZE)
    assert len(result) == expected_job_count


def test_get_all_jobs_with_inconsistency(mock_boto3_session, mock_logger):
    """
    Test that all jobs are fetched correctly when there are more than PAGE_SIZE jobs,
    with inconsistency between batches that triggers a restart.
    """
    # Create timestamps for different batches - ensure they are different and increasing
    base_timestamp = datetime.datetime(2023, 1, 2, 0, 0, 0)

    # Create batches of jobs with increasing timestamps
    total_jobs = PAGE_SIZE * 3  # 300 jobs total

    # First batch - 100 jobs with increasing timestamps
    batch1_original = []
    for i in range(1, PAGE_SIZE + 1):
        # Each job has a unique timestamp, increasing by minutes
        job_timestamp = base_timestamp.replace(minute=i % 60, second=i % 60)
        batch1_original.append({"jobId": f"job-{i}", "updatedAt": job_timestamp})

    # Second batch with inconsistent overlap - 100 jobs
    batch2_inconsistent = []
    # First OVERLAP_SIZE jobs are different (inconsistent with batch1)
    for i in range(1, OVERLAP_SIZE + 1):
        # Timestamps after batch1's last job
        job_timestamp = base_timestamp.replace(hour=1, minute=i % 60, second=i % 60)
        batch2_inconsistent.append(
            {
                "jobId": f"job-{i}-different",  # Different job IDs
                "updatedAt": job_timestamp,
            }
        )

    # Add the rest of batch2_inconsistent
    for i in range(OVERLAP_SIZE + 1, PAGE_SIZE + 1):
        # Timestamps after batch1's last job
        job_timestamp = base_timestamp.replace(hour=1, minute=i % 60, second=i % 60)
        batch2_inconsistent.append({"jobId": f"job-{PAGE_SIZE + i}", "updatedAt": job_timestamp})

    # First batch after restart - same as original batch1
    batch1_restart = batch1_original.copy()

    # Second batch after restart - now with consistent overlap
    batch2_consistent = []
    # First OVERLAP_SIZE jobs should match the last OVERLAP_SIZE jobs from batch1
    for i in range(PAGE_SIZE - OVERLAP_SIZE + 1, PAGE_SIZE + 1):
        batch2_consistent.append(batch1_original[i - 1])  # Use the same job objects for overlap

    # Add the rest of batch2_consistent with timestamps greater than batch1_last_timestamp
    for i in range(PAGE_SIZE + 1, 2 * PAGE_SIZE - OVERLAP_SIZE + 1):
        # Each job has a unique timestamp, increasing by minutes and hours
        job_timestamp = base_timestamp.replace(hour=1, minute=i % 60, second=i % 60)
        batch2_consistent.append({"jobId": f"job-{i}", "updatedAt": job_timestamp})

    # Third batch - 100 jobs with increasing timestamps
    batch3 = []
    # First OVERLAP_SIZE jobs should match the last OVERLAP_SIZE jobs from batch2_consistent
    for i in range(2 * PAGE_SIZE - 2 * OVERLAP_SIZE + 1, 2 * PAGE_SIZE - OVERLAP_SIZE + 1):
        overlap_index = i - (2 * PAGE_SIZE - 2 * OVERLAP_SIZE + 1)
        batch3.append(
            batch2_consistent[-(OVERLAP_SIZE - overlap_index)]
        )  # Use the same job objects for overlap

    # Add the rest of batch3 with timestamps greater than batch2_last_timestamp
    for i in range(2 * PAGE_SIZE - OVERLAP_SIZE + 1, 3 * PAGE_SIZE - 2 * OVERLAP_SIZE + 1):
        # Each job has a unique timestamp, increasing by minutes and hours
        job_timestamp = base_timestamp.replace(hour=2, minute=i % 60, second=i % 60)
        batch3.append({"jobId": f"job-{i}", "updatedAt": job_timestamp})

    # Collect all unique jobs that should be in the final result
    all_jobs = []
    all_jobs.extend(batch1_original)
    all_jobs.extend(batch2_consistent[OVERLAP_SIZE:])  # Don't double-count overlap jobs
    all_jobs.extend(batch3[OVERLAP_SIZE:])  # Don't double-count overlap jobs

    # Setup mock responses with inconsistency and restart
    mock_boto3_session.client().search_jobs.side_effect = [
        {"jobs": batch1_original, "totalResults": total_jobs},
        {"jobs": batch2_inconsistent, "totalResults": total_jobs},  # Inconsistent batch
        {"jobs": batch1_restart, "totalResults": total_jobs},  # Restart from first batch
        {"jobs": batch2_consistent, "totalResults": total_jobs},  # Consistent batch
        {"jobs": batch3, "totalResults": total_jobs},
        {"jobs": [], "totalResults": 0},  # End the loop
    ]

    # Call the function
    result = get_jobs_updated_since_timestamp(
        boto3_session=mock_boto3_session,
        farm_id=MOCK_FARM_ID,
        queue_id=MOCK_QUEUE_ID,
        timestamp=MOCK_TIMESTAMP,
        print_function_callback=mock_logger.echo,
    )

    # Verify results - should have all unique job IDs
    expected_ids = {job["jobId"] for job in all_jobs}
    assert result == expected_ids
    assert len(result) == len(expected_ids)

    # Verify that we got the expected number of jobs
    expected_job_count = PAGE_SIZE + (PAGE_SIZE - OVERLAP_SIZE) + (PAGE_SIZE - OVERLAP_SIZE)
    assert len(result) == expected_job_count

    # Verify that none of the inconsistent job IDs are in the result
    inconsistent_ids = {job["jobId"] for job in batch2_inconsistent}
    assert not any(job_id in result for job_id in inconsistent_ids if job_id not in expected_ids)
