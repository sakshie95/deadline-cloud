# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
from __future__ import annotations
from typing import Dict, Set, Optional


class HydrationState:
    """
    Class that encapsulates all the in-memory maps used for tracking download progress.
    """

    def __init__(
        self,
        ongoing_jobs: Optional[Set[str]] = None,
        session_action_index_map: Optional[Dict[str, int]] = None,
        session_to_job_map: Optional[Dict[str, str]] = None,
        session_to_lifecycle_status_map: Optional[Dict[str, str]] = None,
        auxiliary_session_action_status_mapping: Optional[Dict[str, str]] = None,
        session_to_last_finished_action_id_map: Optional[Dict[str, int]] = None,
    ):
        """
        Initialize a HydrationState instance.

        Args:
            ongoing_jobs: Set of job IDs that are currently being processed
            session_action_index_map: Maps session ID to the last downloaded session action ID
            session_to_job_map: Maps session ID to job ID for quick lookup
            session_to_lifecycle_status_map: Maps session ID to lifecycle status
            auxiliary_session_action_status_mapping: Auxiliary mapping for session action status
            session_to_last_finished_action_id_map: Maps session ID to the last finished action ID
        """
        self.ongoing_jobs = ongoing_jobs or set()
        self.session_action_index_map = session_action_index_map or {}
        self.session_to_job_map = session_to_job_map or {}
        self.session_to_lifecycle_status_map = session_to_lifecycle_status_map or {}
        self.auxiliary_session_action_status_mapping = auxiliary_session_action_status_mapping or {}
        self.session_to_last_finished_action_id_map = session_to_last_finished_action_id_map or {}
