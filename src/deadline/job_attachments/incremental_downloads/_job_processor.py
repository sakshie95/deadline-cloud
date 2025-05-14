# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
from typing import Set, Dict, List, Any
from deadline.client.cli._groups.click_logger import ClickLogger


class JobProcessor:
    def __init__(self):
        pass

    @classmethod
    def hydrate_and_process_jobs(
        cls,
        ongoing_jobs: Set[str],
        farm_id: str,
        queue_id: str,
        last_lookback_time: str,
        logger: ClickLogger,
    ) -> Set[str]:
        """
        Hydrate and process jobs for downloading outputs
        :param ongoing_jobs: set of ongoing jobs
        :param farm_id: farm ID
        :param queue_id: queue ID
        :param last_lookback_time: last lookback time
        :param logger: ClickLogger instance for logging messages
        :return: set of ongoing jobs
        """

        logger.echo(f"Querying for jobs in queue {queue_id} since {last_lookback_time}")
        # TODO: Implement the actual API call to search jobs updated since last_lookback_time
        jobs_from_api_call: List[Dict[str, Any]] = []

        # Add job from jobs_from_api_call to ongoing_jobs if it doesn't exist already
        for job in jobs_from_api_call:
            job_id = job.get("jobId")
            if job_id:
                ongoing_jobs.add(job_id)
        return ongoing_jobs
