# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

from typing import Set, Callable, List, Dict, Any
import boto3

from deadline.client.api._session import get_default_client_config
from botocore.exceptions import ClientError
from datetime import datetime
from deadline.client.exceptions import DeadlineOperationError


# Constants for pagination
PAGE_SIZE = 100
OVERLAP_SIZE = 10


class JobFetchFailure(Exception):
    """
    Failure fetching jobs updated since timestamp
    """


def get_jobs_updated_since_timestamp(
    boto3_session: boto3.Session,
    farm_id: str,
    queue_id: str,
    timestamp: datetime,
    print_function_callback: Callable[[str], None] = lambda msg: None,
) -> Set[str]:
    """
    This function retrieves job IDs that have been updated since the last lookback time
    using a timestamp-based approach with sorting to ensure we get all jobs efficiently.

    The function sorts jobs by increasing updatedAt timestamp, so each search_jobs query gets
    the oldest 100 jobs that are newer than the threshold timestamp. This approach leverages
    the property that when a job is created or updated, it becomes the newest job in the queue.

    We also maintain an overlap check to ensure consistency between batches.

    :param boto3_session: boto3.Session
    :param farm_id: farm ID
    :param queue_id: queue ID
    :param timestamp: timestamp to get jobs updated since then
    :param print_function_callback: Callback to print messages produced in this function.
                Used in the CLI to print to stdout using click.echo. By default, ignores messages.
    :return: set of ongoing job IDs
    """
    print_function_callback(f"Querying for jobs in queue {queue_id} since {timestamp}")

    # Create a result set for the output of jobs to be returned
    result_set: Set[str] = set()

    # Initialize threshold timestamp with the input timestamp
    threshold_timestamp = timestamp
    last_processed_timestamp = timestamp

    # Initialize jobs and total_results
    has_more_jobs = True

    # Previous batch's last OVERLAP_SIZE jobs for consistency checking
    previous_batch_overlap: List[Dict[str, Any]] = []

    # Continue until we've processed all jobs
    while has_more_jobs:
        print_function_callback(f"Searching jobs updated since {threshold_timestamp}")

        deadline = boto3_session.client("deadline", config=get_default_client_config())

        # Set up filters for the search - jobs updated since threshold timestamp
        filter_expressions: dict = {
            "filters": [
                {
                    "dateTimeFilter": {
                        "name": "UPDATED_AT",
                        "dateTime": threshold_timestamp,
                        "operator": "GREATER_THAN",
                    }
                }
            ],
            "operator": "AND",
        }

        # Set up sorting to get jobs in ascending order of updatedAt timestamp
        sort_expressions: list[dict] = [
            {"fieldSort": {"name": "UPDATED_AT", "sortOrder": "ASCENDING"}}
        ]

        try:
            response = deadline.search_jobs(
                farmId=farm_id,
                queueIds=[queue_id],
                pageSize=PAGE_SIZE,
                itemOffset=0,
                filterExpressions=filter_expressions,
                sortExpressions=sort_expressions,
            )
        except ClientError as exc:
            raise DeadlineOperationError(f"Failed to get Jobs from Deadline:\n{exc}") from exc

        # Extract jobs from response
        jobs = response.get("jobs", [])
        total_results = response.get("totalResults", 0)

        print_function_callback(f"Found {len(jobs)} jobs out of {total_results} total results")

        # If no jobs returned, we're done
        if not jobs:
            print_function_callback("No more jobs found. Search complete.")
            break

        # Check for overlap consistency if we have previous jobs
        if previous_batch_overlap:
            # Get the first OVERLAP_SIZE job IDs from the current batch
            current_batch_overlap = jobs[:OVERLAP_SIZE] if len(jobs) >= OVERLAP_SIZE else jobs
            current_overlap_ids = {job.get("jobId") for job in current_batch_overlap}
            previous_overlap_ids = {job.get("jobId") for job in previous_batch_overlap}

            # If overlap doesn't match, we need to restart from the last processed timestamp
            if previous_overlap_ids != current_overlap_ids:
                print_function_callback(
                    "Detected inconsistency in job pagination overlap. Restarting from last processed timestamp."
                )
                # Reset to last processed timestamp and continue
                threshold_timestamp = last_processed_timestamp
                previous_batch_overlap = []
                continue

        # Process jobs and add to result set - add all job IDs that aren't already in the result set
        new_jobs_count = 0
        for job in jobs:
            job_id = job.get("jobId")
            if job_id and job_id not in result_set:
                result_set.add(job_id)
                new_jobs_count += 1

        # Save the last OVERLAP_SIZE jobs for consistency checking in the next batch
        previous_batch_overlap = jobs[-OVERLAP_SIZE:] if len(jobs) >= OVERLAP_SIZE else jobs

        # Update the last processed timestamp to be the current threshold timestamp
        # And update the threshold timestamp to the last job's updatedAt time
        if jobs:
            last_processed_timestamp = threshold_timestamp
            threshold_timestamp = jobs[-1].get("updatedAt")

            # Rare edge case where the updatedAt is the same for all jobs in the page
            if last_processed_timestamp == threshold_timestamp:
                raise JobFetchFailure(
                    "Failure fetching jobs as updatedAt is the same for all jobs in page"
                )

            # Check if we need to continue fetching more jobs
            # If we got fewer jobs than PAGE_SIZE, we've reached the end
            has_more_jobs = len(jobs) >= PAGE_SIZE
        else:
            has_more_jobs = False

    print_function_callback(f"Found {len(result_set)} jobs updated since {timestamp}")

    return result_set
