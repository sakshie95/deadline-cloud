# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
from __future__ import annotations


class StateFileModel:
    """
    Model representing the download progress state file structure.
    State file structure:
    {
        "lastLookbackTime": "2025-04-04T05:30:00",
        "jobs":
        [
            {
                "jobId": "Job-1234353453443",
                "sessions": [
                {
                    "sessionId": "Session-1324324354354",
                    "sessionLifecycleStatus": "SUCCESSFUL",
                    "lastDownloadedSessActionId": 3
                },
                {
                    "sessionId": "Session-3423435435454",
                    "sessionLifecycleStatus": "RUNNING",
                    "lastDownloadedSessActionId": 6
                }
                ]
            },
            {
                "jobId": "Job-3234324354345",
                "sessions": [
                {
                    "sessionId": "Session-4235435434345",
                    "sessionLifecycleStatus": "FAILED",
                    "lastDownloadedSessActionId": 3
                }
                ]
            }
        ]
    }
    """

    def __init__(self, last_lookback_time=None, jobs=None):
        """
        Initialize a StateFileModel instance.
        Args:
            last_lookback_time (str): ISO format timestamp of the last lookback time
            jobs (list): List of job dictionaries containing job_id and sessions information
        """
        self.last_lookback_time = last_lookback_time
        self.jobs = jobs or []

    @classmethod
    def from_dict(cls, data):
        """
        Create a StateFileModel instance from a dictionary.
        Args:
            data (dict): Dictionary containing state file data
        Returns:
            StateFileModel: A new instance populated with the data
        """
        if not data:
            return cls()

        return cls(last_lookback_time=data.get("lastLookbackTime"), jobs=data.get("jobs", []))

    def to_dict(self):
        """
        Convert the StateFileModel to a dictionary.
        Returns:
            dict: Dictionary representation of the state file model
        """
        return {"lastLookbackTime": self.last_lookback_time, "jobs": self.jobs}
