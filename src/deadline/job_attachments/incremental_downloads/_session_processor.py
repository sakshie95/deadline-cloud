# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
from deadline.client.cli._groups.click_logger import ClickLogger
from typing import Set


class SessionProcessor:
    @classmethod
    def hydrate_and_process_sessions(
        cls,
        ongoing_jobs: set,
        farm_id: str,
        queue_id: str,
        last_lookback_time: str,
        logger: ClickLogger,
    ) -> set:
        """
        Hydrate and process ongoing sessions to download outputs
        :param ongoing_jobs: set of ongoing jobs
        :param farm_id: farm id
        :param queue_id: queue id
        :param last_lookback_time: lookback since this time
        :param logger: logger instance
        :return: set of ongoing sessions
        """

        logger.echo(f"Querying for sessions in jobs {ongoing_jobs}")
        sessions_from_api: list = []
        ongoing_sessions: Set[str] = set()
        # For every session check if UPDATED_AT is after last_lookback_time, if yes add to ongoing sessions list
        for session in sessions_from_api:
            if session.UPDATED_AT >= last_lookback_time:
                ongoing_sessions.add(session.SESSION_ID)

        return ongoing_sessions
