# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime
from typing import List

from deadline.job_attachments.incremental_downloads.session_action_processor import (
    SessionActionProcessor,
)
from deadline.job_attachments.incremental_downloads.incremental_download_state import (
    IncrementalDownloadState,
    Job,
    JobSession,
)
from botocore.exceptions import ClientError


@pytest.fixture
def mock_boto3_session():
    """
    Fixture to create a mock boto3 session.
    """
    mock_session = MagicMock()
    mock_client = MagicMock()
    mock_session.client.return_value = mock_client
    return mock_session


@pytest.fixture
def mock_print_function():
    """
    Fixture to create a mock print function.
    """
    return MagicMock()


@pytest.fixture
def sample_download_progress():
    """
    Fixture to create a sample download progress state.
    """
    job1 = Job(
        job_id="job-123",
        sessions=[
            JobSession(
                session_id="session-123",
                session_lifecycle_status="SUCCEEDED",
                last_downloaded_sess_action_id=2,
            ),
            JobSession(
                session_id="session-456",
                session_lifecycle_status="RUNNING",
                last_downloaded_sess_action_id=1,
            ),
        ],
    )

    job2 = Job(
        job_id="job-456",
        sessions=[
            JobSession(
                session_id="session-789",
                session_lifecycle_status="FAILED",
                last_downloaded_sess_action_id=3,
            )
        ],
    )

    return IncrementalDownloadState(
        last_lookback_time="2025-06-09T08:00:00+00:00", jobs=[job1, job2]
    )


class TestSessionActionProcessor:
    """
    Test class for SessionActionProcessor.
    """

    def test_init(self, mock_boto3_session, mock_print_function, sample_download_progress):
        """
        Test initialization of SessionActionProcessor.
        """
        processor = SessionActionProcessor(
            boto3_session=mock_boto3_session,
            download_progress=sample_download_progress,
            print_function_callback=mock_print_function,
        )

        # Verify the session_to_job_map is correctly initialized
        assert processor.session_to_job_map == {
            "session-123": "job-123",
            "session-456": "job-123",
            "session-789": "job-456",
        }

        # Verify the session_to_lifecycle_status_map is correctly initialized
        assert processor.session_to_lifecycle_status_map == {
            "session-123": "SUCCEEDED",
            "session-456": "RUNNING",
            "session-789": "FAILED",
        }

        # Verify the session_to_last_downloaded_action_id is correctly initialized
        assert processor.session_to_last_downloaded_action_id == {
            "session-123": "session-123-2",
            "session-456": "session-456-1",
            "session-789": "session-789-3",
        }

        # Verify the session_to_last_finished_action_id is initialized as empty
        assert processor.session_to_last_finished_action_id == {}

        # Verify the auxiliary_session_action_status_mapping is initialized as empty
        assert processor.auxiliary_session_action_status_mapping == {}

    def test_get_session_to_job_map(
        self, mock_boto3_session, mock_print_function, sample_download_progress
    ):
        """
        Test get_session_to_job_map method.
        """
        processor = SessionActionProcessor(
            boto3_session=mock_boto3_session,
            download_progress=sample_download_progress,
            print_function_callback=mock_print_function,
        )

        result = processor.get_session_to_job_map()

        assert result == {
            "session-123": "job-123",
            "session-456": "job-123",
            "session-789": "job-456",
        }

    def test_get_session_to_lifecycle_status_map(
        self, mock_boto3_session, mock_print_function, sample_download_progress
    ):
        """
        Test get_session_to_lifecycle_status_map method.
        """
        processor = SessionActionProcessor(
            boto3_session=mock_boto3_session,
            download_progress=sample_download_progress,
            print_function_callback=mock_print_function,
        )

        result = processor.get_session_to_lifecycle_status_map()

        assert result == {
            "session-123": "SUCCEEDED",
            "session-456": "RUNNING",
            "session-789": "FAILED",
        }

    def test_get_auxiliary_session_action_status_mapping(
        self, mock_boto3_session, mock_print_function, sample_download_progress
    ):
        """
        Test get_auxiliary_session_action_status_mapping method.
        """
        processor = SessionActionProcessor(
            boto3_session=mock_boto3_session,
            download_progress=sample_download_progress,
            print_function_callback=mock_print_function,
        )

        processor.auxiliary_session_action_status_mapping = {
            "session-123-1": "SUCCEEDED",
            "session-456-1": "RUNNING",
        }

        result = processor.get_auxiliary_session_action_status_mapping()

        assert result == {"session-123-1": "SUCCEEDED", "session-456-1": "RUNNING"}

    def test_get_session_to_last_downloaded_action_id(
        self, mock_boto3_session, mock_print_function, sample_download_progress
    ):
        """
        Test get_session_to_last_downloaded_action_id method.
        """
        processor = SessionActionProcessor(
            boto3_session=mock_boto3_session,
            download_progress=sample_download_progress,
            print_function_callback=mock_print_function,
        )

        result = processor.get_session_to_last_downloaded_action_id()

        assert result == {
            "session-123": "session-123-2",
            "session-456": "session-456-1",
            "session-789": "session-789-3",
        }

    def test_get_session_to_last_finished_action_id(
        self, mock_boto3_session, mock_print_function, sample_download_progress
    ):
        """
        Test get_session_to_last_finished_action_id method.
        """
        processor = SessionActionProcessor(
            boto3_session=mock_boto3_session,
            download_progress=sample_download_progress,
            print_function_callback=mock_print_function,
        )

        processor.session_to_last_finished_action_id = {
            "session-123": "session-123-3",
            "session-456": "session-456-2",
        }

        result = processor.get_session_to_last_finished_action_id()

        assert result == {"session-123": "session-123-3", "session-456": "session-456-2"}

    def test_get_action_id_number_from_session_action_id(
        self, mock_boto3_session, mock_print_function, sample_download_progress
    ):
        """
        Test _get_action_id_number_from_session_action_id method.
        """
        processor = SessionActionProcessor(
            boto3_session=mock_boto3_session,
            download_progress=sample_download_progress,
            print_function_callback=mock_print_function,
        )

        # Test valid action ID
        assert processor._get_action_id_number_from_session_action_id("session-123-5") == 5

        # Test invalid action ID format
        assert processor._get_action_id_number_from_session_action_id("invalid-action-id") == -1

        # Test action ID with non-numeric suffix
        assert processor._get_action_id_number_from_session_action_id("session-123-abc") == -1

        # Test empty action ID
        assert processor._get_action_id_number_from_session_action_id("") == -1

    def test_get_last_downloaded_action_id(
        self, mock_boto3_session, mock_print_function, sample_download_progress
    ):
        """
        Test _get_last_downloaded_action_id method.
        """
        processor = SessionActionProcessor(
            boto3_session=mock_boto3_session,
            download_progress=sample_download_progress,
            print_function_callback=mock_print_function,
        )

        # Test session with last downloaded action ID in session_to_last_downloaded_action_id
        processor.session_to_last_downloaded_action_id = {"session-123": "session-123-5"}
        assert processor._get_last_downloaded_action_id("session-123") == 5

        # Test session without last downloaded action ID
        assert processor._get_last_downloaded_action_id("unknown-session") == -1

    def test_get_sessions_from_download_progress(
        self, mock_boto3_session, mock_print_function, sample_download_progress
    ):
        """
        Test _get_sessions_from_download_progress method.
        """
        processor = SessionActionProcessor(
            boto3_session=mock_boto3_session,
            download_progress=sample_download_progress,
            print_function_callback=mock_print_function,
        )

        # Test with job IDs that exist in download_progress
        result = processor._get_sessions_from_download_progress({"job-123", "job-456"})
        assert set(result) == {"session-123", "session-456", "session-789"}

        # Test with job IDs that partially exist in download_progress
        result = processor._get_sessions_from_download_progress({"job-123", "job-789"})
        assert set(result) == {"session-123", "session-456"}

        # Test with job IDs that don't exist in download_progress
        result = processor._get_sessions_from_download_progress({"job-999"})
        assert result == []

    def test_process_sessions_response(
        self, mock_boto3_session, mock_print_function, sample_download_progress
    ):
        """
        Test _process_sessions_response method.
        """
        processor = SessionActionProcessor(
            boto3_session=mock_boto3_session,
            download_progress=sample_download_progress,
            print_function_callback=mock_print_function,
        )

        # Create a mock response with sessions
        response = {
            "sessions": [
                {
                    "sessionId": "session-new-1",
                    "startedAt": datetime.fromisoformat("2025-06-09T08:30:00+00:00"),
                    "lifecycleStatus": "RUNNING",
                },
                {
                    "sessionId": "session-new-2",
                    "startedAt": datetime.fromisoformat(
                        "2025-06-09T07:30:00+00:00"
                    ),  # Before lookback time
                    "lifecycleStatus": "SUCCEEDED",
                },
                {
                    "sessionId": "session-new-3",
                    "startedAt": datetime.fromisoformat("2025-06-09T08:45:00+00:00"),
                    "lifecycleStatus": "FAILED",
                },
            ]
        }

        job_id = "job-new"
        lookback_datetime = datetime.fromisoformat("2025-06-09T08:00:00+00:00")
        updated_sessions: List[str] = []

        processor._process_sessions_response(response, job_id, lookback_datetime, updated_sessions)

        # Verify that only sessions started after lookback time are added
        assert set(updated_sessions) == {"session-new-1", "session-new-3"}

        # Verify that session_to_job_map is updated
        assert processor.session_to_job_map["session-new-1"] == job_id
        assert processor.session_to_job_map["session-new-3"] == job_id
        assert "session-new-2" not in processor.session_to_job_map

        # Verify that session_to_lifecycle_status_map is updated
        assert processor.session_to_lifecycle_status_map["session-new-1"] == "RUNNING"
        assert processor.session_to_lifecycle_status_map["session-new-3"] == "FAILED"
        assert "session-new-2" not in processor.session_to_lifecycle_status_map

    @patch(
        "deadline.job_attachments.incremental_downloads.session_action_processor.get_default_client_config"
    )
    def test_get_updated_sessions_since_lookback_from_deadline(
        self, mock_get_config, mock_boto3_session, mock_print_function, sample_download_progress
    ):
        """
        Test _get_updated_sessions_since_lookback_from_deadline method.
        """
        processor = SessionActionProcessor(
            boto3_session=mock_boto3_session,
            download_progress=sample_download_progress,
            print_function_callback=mock_print_function,
        )

        # Mock the list_sessions response
        mock_client = mock_boto3_session.client.return_value
        mock_client.list_sessions.side_effect = [
            # First response for job-123
            {
                "sessions": [
                    {
                        "sessionId": "session-new-1",
                        "startedAt": datetime.fromisoformat("2025-06-09T08:30:00+00:00"),
                        "lifecycleStatus": "RUNNING",
                    }
                ],
                "nextToken": "token1",
            },
            # Second response for job-123 (pagination)
            {
                "sessions": [
                    {
                        "sessionId": "session-new-2",
                        "startedAt": datetime.fromisoformat("2025-06-09T08:45:00+00:00"),
                        "lifecycleStatus": "SUCCEEDED",
                    }
                ]
            },
            # First response for job-456
            {
                "sessions": [
                    {
                        "sessionId": "session-new-3",
                        "startedAt": datetime.fromisoformat(
                            "2025-06-09T07:30:00+00:00"
                        ),  # Before lookback time
                        "lifecycleStatus": "FAILED",
                    }
                ]
            },
        ]

        job_ids = {"job-123", "job-456"}
        farm_id = "farm-123"
        queue_id = "queue-123"
        last_lookback_time = "2025-06-09T08:00:00+00:00"

        result = processor._get_updated_sessions_since_lookback_from_deadline(
            job_ids, farm_id, queue_id, last_lookback_time
        )

        # Verify that only sessions started after lookback time are returned
        assert set(result) == {"session-new-1", "session-new-2"}

        # Verify that list_sessions is called with correct parameters
        mock_client.list_sessions.assert_any_call(farmId=farm_id, jobId="job-123", queueId=queue_id)
        mock_client.list_sessions.assert_any_call(
            farmId=farm_id, jobId="job-123", queueId=queue_id, nextToken="token1"
        )
        mock_client.list_sessions.assert_any_call(farmId=farm_id, jobId="job-456", queueId=queue_id)

    @patch(
        "deadline.job_attachments.incremental_downloads.session_action_processor.get_default_client_config"
    )
    def test_get_updated_sessions_since_lookback_from_deadline_client_error(
        self, mock_get_config, mock_boto3_session, mock_print_function, sample_download_progress
    ):
        """
        Test _get_updated_sessions_since_lookback_from_deadline method with ClientError.
        """
        processor = SessionActionProcessor(
            boto3_session=mock_boto3_session,
            download_progress=sample_download_progress,
            print_function_callback=mock_print_function,
        )

        # Mock the list_sessions response with ClientError
        mock_client = mock_boto3_session.client.return_value
        mock_client.list_sessions.side_effect = [
            ClientError(
                {"Error": {"Code": "ResourceNotFoundException", "Message": "Not found"}},
                "list_sessions",
            ),
            {
                "sessions": [
                    {
                        "sessionId": "session-new-1",
                        "startedAt": datetime.fromisoformat("2025-06-09T08:30:00+00:00"),
                        "lifecycleStatus": "RUNNING",
                    }
                ]
            },
        ]

        job_ids = {"job-123", "job-456"}
        farm_id = "farm-123"
        queue_id = "queue-123"
        last_lookback_time = "2025-06-09T08:00:00+00:00"

        result = processor._get_updated_sessions_since_lookback_from_deadline(
            job_ids, farm_id, queue_id, last_lookback_time
        )

        # Verify that only sessions from successful API calls are returned
        assert set(result) == {"session-new-1"}

    @patch(
        "deadline.job_attachments.incremental_downloads.session_action_processor.get_default_client_config"
    )
    def test_get_session_actions_from_deadline(
        self, mock_get_config, mock_boto3_session, mock_print_function, sample_download_progress
    ):
        """
        Test _get_session_actions_from_deadline method.
        """
        processor = SessionActionProcessor(
            boto3_session=mock_boto3_session,
            download_progress=sample_download_progress,
            print_function_callback=mock_print_function,
        )

        # Mock the list_session_actions response
        mock_client = mock_boto3_session.client.return_value
        mock_client.list_session_actions.side_effect = [
            # First response
            {
                "sessionActions": [
                    {
                        "sessionActionId": "session-123-3",
                        "status": "SUCCEEDED",
                        "definition": {"taskRun": {"taskId": "task-123", "stepId": "step-123"}},
                    }
                ],
                "nextToken": "token1",
            },
            # Second response (pagination)
            {
                "sessionActions": [
                    {
                        "sessionActionId": "session-123-4",
                        "status": "FAILED",
                        "definition": {"taskRun": {"taskId": "task-456", "stepId": "step-456"}},
                    }
                ]
            },
        ]

        session_id = "session-123"
        farm_id = "farm-123"
        queue_id = "queue-123"
        job_id = "job-123"

        result = processor._get_session_actions_from_deadline(session_id, farm_id, queue_id, job_id)

        # Verify that all session actions are returned
        assert len(result) == 2
        assert result[0]["sessionActionId"] == "session-123-3"
        assert result[1]["sessionActionId"] == "session-123-4"

        # Verify that list_session_actions is called with correct parameters
        mock_client.list_session_actions.assert_any_call(
            farmId=farm_id, sessionId=session_id, queueId=queue_id, jobId=job_id
        )
        mock_client.list_session_actions.assert_any_call(
            farmId=farm_id, sessionId=session_id, queueId=queue_id, jobId=job_id, nextToken="token1"
        )

        # Verify that auxiliary_session_action_status_mapping is updated
        assert processor.auxiliary_session_action_status_mapping["session-123-3"] == "SUCCEEDED"
        assert processor.auxiliary_session_action_status_mapping["session-123-4"] == "FAILED"

    @patch(
        "deadline.job_attachments.incremental_downloads.session_action_processor.get_default_client_config"
    )
    def test_get_session_actions_from_deadline_client_error(
        self, mock_get_config, mock_boto3_session, mock_print_function, sample_download_progress
    ):
        """
        Test _get_session_actions_from_deadline method with ClientError.
        """
        processor = SessionActionProcessor(
            boto3_session=mock_boto3_session,
            download_progress=sample_download_progress,
            print_function_callback=mock_print_function,
        )

        # Mock the list_session_actions response with ClientError
        mock_client = mock_boto3_session.client.return_value
        mock_client.list_session_actions.side_effect = ClientError(
            {"Error": {"Code": "ResourceNotFoundException", "Message": "Not found"}},
            "list_session_actions",
        )

        session_id = "session-123"
        farm_id = "farm-123"
        queue_id = "queue-123"
        job_id = "job-123"

        result = processor._get_session_actions_from_deadline(session_id, farm_id, queue_id, job_id)

        # Verify that an empty list is returned
        assert result == []

    @patch.object(SessionActionProcessor, "_get_updated_sessions_since_lookback_from_deadline")
    @patch.object(SessionActionProcessor, "_get_sessions_from_download_progress")
    @patch.object(SessionActionProcessor, "_get_last_downloaded_action_id")
    @patch.object(SessionActionProcessor, "_get_session_actions_from_deadline")
    @patch.object(SessionActionProcessor, "_get_action_id_number_from_session_action_id")
    def test_get_list_of_ongoing_session_action_ids_for_jobs(
        self,
        mock_get_action_id_number,
        mock_get_session_actions,
        mock_get_last_downloaded_action_id,
        mock_get_sessions_from_download_progress,
        mock_get_updated_sessions,
        mock_boto3_session,
        mock_print_function,
        sample_download_progress,
    ):
        """
        Test get_list_of_ongoing_session_action_ids_for_jobs method.
        """
        processor = SessionActionProcessor(
            boto3_session=mock_boto3_session,
            download_progress=sample_download_progress,
            print_function_callback=mock_print_function,
        )

        # Set up mocks
        mock_get_updated_sessions.return_value = ["session-123"]
        mock_get_sessions_from_download_progress.return_value = []
        mock_get_last_downloaded_action_id.return_value = 2
        mock_get_action_id_number.side_effect = (
            lambda x: int(x.split("-")[-1]) if x and len(x.split("-")) >= 3 else -1
        )

        mock_get_session_actions.return_value = [
            {
                "sessionActionId": "session-123-3",
                "status": "SUCCEEDED",
                "definition": {"taskRun": {"taskId": "task-123", "stepId": "step-123"}},
            },
            {
                "sessionActionId": "session-123-4",
                "status": "FAILED",
                "definition": {"taskRun": {"taskId": "task-456", "stepId": "step-456"}},
            },
            {
                "sessionActionId": "session-123-5",
                "status": "SUCCEEDED",
                "definition": {"taskRun": {"taskId": "task-789", "stepId": "step-789"}},
            },
        ]

        # Set up the session_to_job_map
        processor.session_to_job_map = {"session-123": "job-123"}

        job_ids = {"job-123", "job-456"}
        farm_id = "farm-123"
        queue_id = "queue-123"
        last_lookback_time = "2025-06-09T08:00:00+00:00"

        result = processor.get_list_of_ongoing_session_action_ids_for_jobs(
            job_ids, farm_id, queue_id, last_lookback_time
        )

        # Verify that only SUCCEEDED actions with taskRun are returned
        assert result == {"job-123": ["session-123-3", "session-123-5"]}

        # Verify that methods are called with correct parameters
        mock_get_updated_sessions.assert_called_once_with(
            job_ids, farm_id, queue_id, last_lookback_time
        )
        mock_get_sessions_from_download_progress.assert_called_once_with(job_ids)
        mock_get_session_actions.assert_called_once_with(
            "session-123", farm_id, queue_id, "job-123"
        )

        assert processor.session_to_last_finished_action_id["session-123"] == "session-123-5"

    @patch.object(SessionActionProcessor, "_get_updated_sessions_since_lookback_from_deadline")
    @patch.object(SessionActionProcessor, "_get_sessions_from_download_progress")
    def test_get_list_of_ongoing_session_action_ids_for_jobs_no_job_id(
        self,
        mock_get_sessions_from_download_progress,
        mock_get_updated_sessions,
        mock_boto3_session,
        mock_print_function,
        sample_download_progress,
    ):
        """
        Test get_list_of_ongoing_session_action_ids_for_jobs method with no job ID for a session.
        """
        processor = SessionActionProcessor(
            boto3_session=mock_boto3_session,
            download_progress=sample_download_progress,
            print_function_callback=mock_print_function,
        )

        # Set up mocks
        mock_get_updated_sessions.return_value = ["session-123", "session-unknown"]
        mock_get_sessions_from_download_progress.return_value = []

        # Set up the session_to_job_map with only one session
        processor.session_to_job_map = {"session-123": "job-123"}

        job_ids = {"job-123"}
        farm_id = "farm-123"
        queue_id = "queue-123"
        last_lookback_time = "2025-06-09T08:00:00+00:00"

        # This should not raise an exception
        result = processor.get_list_of_ongoing_session_action_ids_for_jobs(
            job_ids, farm_id, queue_id, last_lookback_time
        )

        # Verify the result
        assert isinstance(result, dict)

    @patch.object(SessionActionProcessor, "_get_updated_sessions_since_lookback_from_deadline")
    @patch.object(SessionActionProcessor, "_get_sessions_from_download_progress")
    def test_get_list_of_ongoing_session_action_ids_for_jobs_job_id_not_in_target(
        self,
        mock_get_sessions_from_download_progress,
        mock_get_updated_sessions,
        mock_boto3_session,
        mock_print_function,
        sample_download_progress,
    ):
        """
        Test get_list_of_ongoing_session_action_ids_for_jobs method with job ID not in target set.
        """
        processor = SessionActionProcessor(
            boto3_session=mock_boto3_session,
            download_progress=sample_download_progress,
            print_function_callback=mock_print_function,
        )

        # Set up mocks
        mock_get_updated_sessions.return_value = ["session-123"]
        mock_get_sessions_from_download_progress.return_value = []

        # Set up the session_to_job_map
        processor.session_to_job_map = {"session-123": "job-123"}

        # Use a different job ID in the target set
        job_ids = {"job-456"}
        farm_id = "farm-123"
        queue_id = "queue-123"
        last_lookback_time = "2025-06-09T08:00:00+00:00"

        result = processor.get_list_of_ongoing_session_action_ids_for_jobs(
            job_ids, farm_id, queue_id, last_lookback_time
        )

        # Verify that no session actions are returned
        assert result == {}

    @patch.object(SessionActionProcessor, "_get_updated_sessions_since_lookback_from_deadline")
    @patch.object(SessionActionProcessor, "_get_sessions_from_download_progress")
    @patch.object(SessionActionProcessor, "_get_last_downloaded_action_id")
    @patch.object(SessionActionProcessor, "_get_session_actions_from_deadline")
    @patch.object(SessionActionProcessor, "_get_action_id_number_from_session_action_id")
    def test_get_list_of_ongoing_session_action_ids_for_jobs_no_new_actions(
        self,
        mock_get_action_id_number,
        mock_get_session_actions,
        mock_get_last_downloaded_action_id,
        mock_get_sessions_from_download_progress,
        mock_get_updated_sessions,
        mock_boto3_session,
        mock_print_function,
        sample_download_progress,
    ):
        """
        Test get_list_of_ongoing_session_action_ids_for_jobs method with no new actions.
        """
        processor = SessionActionProcessor(
            boto3_session=mock_boto3_session,
            download_progress=sample_download_progress,
            print_function_callback=mock_print_function,
        )

        # Set up mocks
        mock_get_updated_sessions.return_value = ["session-123"]
        mock_get_sessions_from_download_progress.return_value = []
        mock_get_last_downloaded_action_id.return_value = 10
        mock_get_action_id_number.side_effect = (
            lambda x: int(x.split("-")[-1]) if x and len(x.split("-")) >= 3 else -1
        )

        mock_get_session_actions.return_value = [
            {
                "sessionActionId": "session-123-3",
                "status": "SUCCEEDED",
                "definition": {"taskRun": {"taskId": "task-123", "stepId": "step-123"}},
            }
        ]

        # Set up the session_to_job_map
        processor.session_to_job_map = {"session-123": "job-123"}

        job_ids = {"job-123"}
        farm_id = "farm-123"
        queue_id = "queue-123"
        last_lookback_time = "2025-06-09T08:00:00+00:00"

        result = processor.get_list_of_ongoing_session_action_ids_for_jobs(
            job_ids, farm_id, queue_id, last_lookback_time
        )

        # Verify that no session actions are returned
        assert result == {}
