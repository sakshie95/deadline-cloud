# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import datetime
from unittest.mock import MagicMock
import pytest
from botocore.exceptions import ClientError

from deadline.client.exceptions import DeadlineOperationError
from deadline.job_attachments.incremental_downloads.updated_jobs_fetcher import (
    get_list_of_ongoing_jobs_on_queue,
    PAGE_SIZE,
    OVERLAP_SIZE,
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
    result = get_list_of_ongoing_jobs_on_queue(
        boto3_session=mock_boto3_session,
        farm_id=MOCK_FARM_ID,
        queue_id=MOCK_QUEUE_ID,
        timestamp=MOCK_TIMESTAMP,
        print_function_callback=mock_logger.echo,
    )

    # Verify results
    expected_result = {f"job-{i}" for i in range(1, 11)}
    assert result == expected_result

    # Verify API call
    mock_boto3_session.client().search_jobs.assert_called_once()
    verify_search_jobs_call(mock_boto3_session, 0, 0)


def test_get_list_of_ongoing_jobs_multiple_pages(mock_boto3_session, mock_logger):
    """Test getting ongoing jobs with multiple pages of results."""
    # Setup mock responses for multiple pages
    mock_boto3_session.client().search_jobs.side_effect = [
        create_mock_jobs_response(1, PAGE_SIZE, PAGE_SIZE),
        create_mock_jobs_response(
            PAGE_SIZE - OVERLAP_SIZE + 1, 2 * PAGE_SIZE - OVERLAP_SIZE, 2 * PAGE_SIZE - OVERLAP_SIZE
        ),
        create_mock_jobs_response(2 * PAGE_SIZE - 2 * OVERLAP_SIZE + 1, 250),
    ]

    # Call the function
    result = get_list_of_ongoing_jobs_on_queue(
        boto3_session=mock_boto3_session,
        farm_id=MOCK_FARM_ID,
        queue_id=MOCK_QUEUE_ID,
        timestamp=MOCK_TIMESTAMP,
        print_function_callback=mock_logger.echo,
    )

    # Verify results - should have 250 unique job IDs
    expected_result = {f"job-{i}" for i in range(1, 251)}
    assert result == expected_result
    assert len(result) == 250

    # Verify API calls
    assert mock_boto3_session.client().search_jobs.call_count == 3

    # Check calls
    verify_search_jobs_call(mock_boto3_session, 0, 0)
    verify_search_jobs_call(mock_boto3_session, 1, PAGE_SIZE - OVERLAP_SIZE)
    verify_search_jobs_call(mock_boto3_session, 2, 2 * PAGE_SIZE - 2 * OVERLAP_SIZE)


def setup_inconsistency_test_responses(mock_boto3_session, num_restarts=1):
    """Helper function to set up mock responses for inconsistency tests.

    Args:
        mock_boto3_session: The mock boto3 session
        num_restarts: Number of restarts to simulate
    """
    # Common response patterns
    first_page = create_mock_jobs_response(1, PAGE_SIZE, PAGE_SIZE)
    inconsistent_page = create_mock_jobs_response(
        PAGE_SIZE - OVERLAP_SIZE + 1,
        2 * PAGE_SIZE - OVERLAP_SIZE,
        2 * PAGE_SIZE - OVERLAP_SIZE,
        modify_ids=True,
    )
    consistent_page = create_mock_jobs_response(
        PAGE_SIZE - OVERLAP_SIZE + 1, 2 * PAGE_SIZE - OVERLAP_SIZE, 2 * PAGE_SIZE - OVERLAP_SIZE
    )
    final_page = create_mock_jobs_response(2 * PAGE_SIZE - 2 * OVERLAP_SIZE + 1, 250)

    # Build the response sequence based on number of restarts
    responses = [first_page, inconsistent_page]  # Initial attempt

    # Add responses for each restart
    for i in range(num_restarts):
        responses.append(first_page)  # Restart from first page

        # For all but the last restart, add an inconsistent page
        if i < num_restarts - 1:
            responses.append(inconsistent_page)
        else:
            # Last restart gets consistent pages
            responses.append(consistent_page)
            responses.append(final_page)

    mock_boto3_session.client().search_jobs.side_effect = responses


def test_get_list_of_ongoing_jobs_with_inconsistency(mock_boto3_session, mock_logger):
    """Test handling of inconsistency in pagination overlap."""
    # Setup mock responses with an inconsistency in the second page
    setup_inconsistency_test_responses(mock_boto3_session, num_restarts=1)

    # Call the function
    result = get_list_of_ongoing_jobs_on_queue(
        boto3_session=mock_boto3_session,
        farm_id=MOCK_FARM_ID,
        queue_id=MOCK_QUEUE_ID,
        timestamp=MOCK_TIMESTAMP,
        print_function_callback=mock_logger.echo,
    )

    # Verify results - should have 250 unique job IDs after restart
    expected_result = {f"job-{i}" for i in range(1, 251)}
    assert result == expected_result
    assert len(result) == 250

    # Verify API calls - should be 5 calls due to the restart
    assert mock_boto3_session.client().search_jobs.call_count == 5

    # Check that after restart, we start from offset 0
    verify_search_jobs_call(mock_boto3_session, 2, 0)


def test_get_list_of_ongoing_jobs_empty_response(mock_boto3_session, mock_logger):
    """Test handling of empty response."""
    # Setup mock response with no jobs
    mock_boto3_session.client().search_jobs.return_value = {"jobs": [], "nextItemOffset": None}

    # Call the function
    result = get_list_of_ongoing_jobs_on_queue(
        boto3_session=mock_boto3_session,
        farm_id=MOCK_FARM_ID,
        queue_id=MOCK_QUEUE_ID,
        timestamp=MOCK_TIMESTAMP,
        print_function_callback=mock_logger.echo,
    )

    # Verify results - should be an empty set
    assert result == set()
    assert len(result) == 0

    # Verify API call
    mock_boto3_session.client().search_jobs.assert_called_once()
    verify_search_jobs_call(mock_boto3_session, 0, 0)


def test_get_list_of_ongoing_jobs_api_error(mock_boto3_session, mock_logger):
    """Test handling of API errors."""
    # Setup mock to raise ClientError
    error_response = {"Error": {"Code": "SomeError", "Message": "API Error"}}
    mock_boto3_session.client().search_jobs.side_effect = ClientError(error_response, "search_jobs")

    # Call the function and expect a DeadlineOperationError
    with pytest.raises(DeadlineOperationError) as excinfo:
        get_list_of_ongoing_jobs_on_queue(
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
    # Setup mock responses with multiple inconsistencies
    setup_inconsistency_test_responses(mock_boto3_session, num_restarts=2)

    # Call the function
    result = get_list_of_ongoing_jobs_on_queue(
        boto3_session=mock_boto3_session,
        farm_id=MOCK_FARM_ID,
        queue_id=MOCK_QUEUE_ID,
        timestamp=MOCK_TIMESTAMP,
        print_function_callback=mock_logger.echo,
    )

    # Verify results - should have 250 unique job IDs after restarts
    expected_result = {f"job-{i}" for i in range(1, 251)}
    assert result == expected_result
    assert len(result) == 250

    # Verify API calls - should be 7 calls due to the two restarts
    assert mock_boto3_session.client().search_jobs.call_count == 7


def test_get_list_of_ongoing_jobs_no_next_item_offset(mock_boto3_session, mock_logger):
    """Test handling when nextItemOffset is missing but there are more pages."""
    # Setup mock response with missing nextItemOffset
    mock_boto3_session.client().search_jobs.return_value = {
        "jobs": [{"jobId": f"job-{i}"} for i in range(1, PAGE_SIZE + 1)]
    }

    # Call the function
    result = get_list_of_ongoing_jobs_on_queue(
        boto3_session=mock_boto3_session,
        farm_id=MOCK_FARM_ID,
        queue_id=MOCK_QUEUE_ID,
        timestamp=MOCK_TIMESTAMP,
        print_function_callback=mock_logger.echo,
    )

    # Verify results - should have PAGE_SIZE job IDs
    expected_result = {f"job-{i}" for i in range(1, PAGE_SIZE + 1)}
    assert result == expected_result
    assert len(result) == PAGE_SIZE

    # Verify API call - should only be called once since nextItemOffset is missing
    mock_boto3_session.client().search_jobs.assert_called_once()
    verify_search_jobs_call(mock_boto3_session, 0, 0)
