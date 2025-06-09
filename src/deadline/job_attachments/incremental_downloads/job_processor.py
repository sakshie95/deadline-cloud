# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

from typing import Set, Callable, List
import boto3

from deadline.client.api._session import get_default_client_config
from botocore.exceptions import ClientError
from datetime import datetime
from deadline.client.exceptions import DeadlineOperationError


# Constants for pagination
PAGE_SIZE = 100
OVERLAP_SIZE = 10


def get_list_of_ongoing_jobs_on_queue(
    boto3_session: boto3.Session,
    last_known_set_of_ongoing_jobs: Set[str],
    farm_id: str,
    queue_id: str,
    last_lookback_time: str,
    print_function_callback: Callable[[str], None] = lambda msg: None,
) -> Set[str]:
    """
    This function retrieves job IDs that have been updated since the last lookback time
    using an overlapping pagination strategy with nextItemOffset to ensure consistency.

    We call SearchJobs with an overlap like 0-99, 90-189, 180-279, ...
    Then, we verify that at every 10 job overlap (configurable number OVERLAP), we have matching job ids.
    If they do not match, it means something changed about the state of jobs in the response while we were polling.
    We restart the polling again for an inconsistent response case.

    :param boto3_session: boto3.Session
    :param last_known_set_of_ongoing_jobs: last known set of ongoing job IDs
    :param farm_id: farm ID
    :param queue_id: queue ID
    :param last_lookback_time: lookback time to look for jobs updated since
    :param print_function_callback: Callback to print messages produced in this function.
                Used in the CLI to print to stdout using click.echo. By default, ignores messages.
    :return: set of ongoing job IDs
    """
    print_function_callback(f"Querying for jobs in queue {queue_id} since {last_lookback_time}")

    # Create a copy of the input set to avoid modifying the original
    result_set = set(last_known_set_of_ongoing_jobs)

    # Set up filters for the search - jobs updated since last lookback time
    filter_expressions: dict = {
        "filters": [
            {
                "dateTimeFilter": {
                    "name": "UPDATED_AT",
                    "dateTime": datetime.fromisoformat(last_lookback_time),
                    "operator": "GREATER_THAN",
                }
            }
        ],
        "operator": "AND",
    }

    # Collect all job IDs from all pages
    all_job_ids: List[str] = []

    # Start with first page
    item_offset = 0
    page_number = 1

    # Flag to track if we need to restart due to inconsistency
    restart_needed = False

    while True:
        print_function_callback(f"Searching jobs from page {page_number}, offset {item_offset}")

        deadline = boto3_session.client("deadline", config=get_default_client_config())

        # Get jobs for the current page range
        try:
            response = deadline.search_jobs(
                farmId=farm_id,
                queueIds=[queue_id],
                itemOffset=item_offset,
                pageSize=PAGE_SIZE,
                filterExpressions=filter_expressions,
            )
        except ClientError as exc:
            raise DeadlineOperationError(f"Failed to get Jobs from Deadline:\n{exc}") from exc

        # Extract job IDs from response
        current_page_job_ids = [job.get("jobId") for job in response.get("jobs", [])]

        # If no jobs returned, we're done
        if not current_page_job_ids:
            break

        # Check for overlap consistency if not the first page and not after a restart
        if item_offset > 0 and not restart_needed:
            # Get job IDs from the overlapping section of the previous page
            previous_overlap_ids = set(all_job_ids[-OVERLAP_SIZE:])

            # Get job IDs from the overlapping section of the current page
            current_overlap_ids = set(current_page_job_ids[:OVERLAP_SIZE])

            # If overlap doesn't match, we need to restart from the beginning
            if previous_overlap_ids != current_overlap_ids:
                print_function_callback(
                    "Detected inconsistency in job pagination overlap. Restarting search."
                )
                # Restart the search from the beginning
                item_offset = 0
                all_job_ids = []
                page_number = 1
                restart_needed = True
                continue

        # Reset the restart flag if we've successfully processed a page after restart
        restart_needed = False

        # For first page, add all job IDs
        if item_offset == 0:
            all_job_ids.extend(current_page_job_ids)
        else:
            # For subsequent pages, skip the overlapping job IDs
            all_job_ids.extend(current_page_job_ids[OVERLAP_SIZE:])

        print_function_callback(
            f"Page {page_number}: Added {len(current_page_job_ids) if item_offset == 0 else len(current_page_job_ids) - OVERLAP_SIZE} new job IDs"
        )
        print_function_callback(f"Current total job IDs: {len(all_job_ids)}")

        # Get the next item offset from the response
        next_item_offset = response.get("nextItemOffset")

        # If no next item offset or we've reached the end, we're done
        if next_item_offset is None:
            break

        # Calculate the next item offset with overlap
        item_offset = next_item_offset - OVERLAP_SIZE
        page_number += 1

    # Add all collected job IDs to the result set
    result_set.update(all_job_ids)

    print_function_callback(f"Found {len(all_job_ids)} jobs updated since {last_lookback_time}")
    print_function_callback(f"Total ongoing jobs: {len(result_set)}")

    return result_set
