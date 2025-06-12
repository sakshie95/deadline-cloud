# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.


import os
import pytest
from unittest.mock import patch, MagicMock

import boto3
from deadline.client.api._queue_apis import (
    _incremental_output_download,
    _validate_file_inputs_for_incremental_output_download,
)
from deadline.client.cli._groups.click_logger import ClickLogger
from deadline.job_attachments.incremental_downloads.incremental_download_state import (
    IncrementalDownloadState,
)
from freezegun import freeze_time
from deadline.job_attachments.incremental_downloads.exceptions import PidLockAlreadyHeld


@patch("deadline.client.api._queue_apis._pid_utils.release_pid_lock")
@patch("deadline.client.api._queue_apis._pid_utils.try_acquire_pid_lock")
@patch("deadline.client.api._queue_apis.load_progress_from_state_file")
@patch("deadline.client.api._queue_apis.save_progress_to_state_file")
@patch("deadline.client.api._queue_apis.aggregate_manifest_and_download_outputs")
@patch("deadline.client.api._queue_apis.get_list_of_ongoing_jobs_on_queue")
@patch("deadline.client.api._queue_apis.update_download_state_using_ongoing_sessions")
@patch("deadline.client.api._queue_apis.SessionActionProcessor")
@freeze_time("2025-05-26 12:00:00")
def test_incremental_output_download_success_load_from_progress(
    mock_session_action_processor_class,
    mock_update_download_state,
    mock_get_ongoing_jobs,
    mock_aggregate_manifest,
    mock_save_progress,
    mock_load_progress,
    mock_acquire_pid_lock,
    mock_release_pid_lock,
    tmp_path,
):
    """Test successful execution of _incremental_output_download with loading progress from state file"""
    # Arrange
    farm_id = "farm-0123456789abcdef"
    queue_id = "queue-0123456789abcdef"
    boto3_session = MagicMock(spec=boto3.Session)
    # Mock boto3 client and responses
    mock_client = MagicMock()
    boto3_session.client.return_value = mock_client
    mock_client.get_queue.return_value = {
        "jobAttachmentSettings": {"bucketName": "test-bucket", "keyPrefix": "test-prefix"},
        "displayName": "Test Queue",
    }

    saved_progress_checkpoint_location = str(tmp_path / "checkpoint")
    os.makedirs(saved_progress_checkpoint_location, exist_ok=True)

    pid_file_full_path = os.path.join(
        saved_progress_checkpoint_location, "queue-0123456789abcdef_incremental_output_download.pid"
    )
    saved_progress_checkpoint_full_path: str = os.path.join(
        saved_progress_checkpoint_location, f"{queue_id}_download_progress.json"
    )
    logger: ClickLogger = ClickLogger(is_json=False)

    # Test download progress
    download_progress_json = {
        "lastLookbackTime": "2025-04-04T05:30:00",
        "jobs": [
            {
                "jobId": "job-1234353453443",
                "sessions": [
                    {
                        "sessionId": "session-1324324354354",
                        "sessionLifecycleStatus": "SUCCESSFUL",
                        "lastDownloadedSessActionId": 3,
                    },
                    {
                        "sessionId": "session-3423435435454",
                        "sessionLifecycleStatus": "RUNNING",
                        "lastDownloadedSessActionId": 6,
                    },
                ],
            },
            {
                "jobId": "Job-3234324354345",
                "sessions": [
                    {
                        "sessionId": "session-4235435434345",
                        "sessionLifecycleStatus": "FAILED",
                        "lastDownloadedSessActionId": 3,
                    }
                ],
            },
        ],
    }

    expected_current_download_progress: IncrementalDownloadState = (
        IncrementalDownloadState.from_dict(download_progress_json)
    )
    mock_load_progress.return_value = expected_current_download_progress
    expected_updated_download_progress = expected_current_download_progress
    expected_updated_download_progress.last_lookback_time = "2025-05-26T12:00:00"

    # Mock the ongoing jobs
    mock_get_ongoing_jobs.return_value = {"job-1234353453443", "Job-3234324354345"}

    # Create a mock for the SessionActionProcessor instance with all required methods
    mock_session_action_processor = MagicMock()
    mock_session_action_processor_class.return_value = mock_session_action_processor

    # Configure get_list_of_ongoing_session_action_ids_for_jobs
    mock_session_action_processor.get_list_of_ongoing_session_action_ids_for_jobs.return_value = [
        MagicMock(session_action_id="session-action-1"),
        MagicMock(session_action_id="session-action-2"),
        MagicMock(session_action_id="session-action-3"),
    ]

    # Configure mock_ongoing_sessions
    mock_ongoing_sessions = [
        MagicMock(job_id="job-1234353453443", session_id="session-1324324354354"),
        MagicMock(job_id="job-1234353453443", session_id="session-3423435435454"),
        MagicMock(job_id="Job-3234324354345", session_id="session-4235435434345"),
    ]

    # Create a separate mock for the specific method we're having trouble with
    get_updated_list_mock = MagicMock(return_value=mock_ongoing_sessions)
    mock_session_action_processor.get_updated_list_of_ongoing_sessions_pending_download = (
        get_updated_list_mock
    )

    # Mock the aggregate manifest download
    mock_aggregate_manifest.return_value = [
        "session-action-1",
        "session-action-2",
        "session-action-3",
    ]

    # Mock the update_download_state_using_ongoing_sessions function
    mock_update_download_state.return_value = expected_updated_download_progress

    # Act
    _incremental_output_download(
        farm_id=farm_id,
        queue_id=queue_id,
        boto3_session=boto3_session,
        saved_progress_checkpoint_location=saved_progress_checkpoint_location,
        print_function_callback=logger.echo,
    )

    # Assert the calls were made in expected order
    mock_acquire_pid_lock.assert_called_once_with(pid_file_full_path, logger.echo)
    mock_load_progress.assert_called_once_with(saved_progress_checkpoint_full_path, logger.echo)

    # Verify the new method calls
    get_updated_list_mock.assert_called_once_with(mock_aggregate_manifest.return_value)

    mock_update_download_state.assert_called_once_with(
        expected_current_download_progress, mock_ongoing_sessions, "2025-05-26T12:00:00"
    )

    mock_save_progress.assert_called_once_with(
        saved_progress_checkpoint_location,
        saved_progress_checkpoint_full_path,
        expected_updated_download_progress,
        logger.echo,
    )

    mock_release_pid_lock.assert_called_once_with(pid_file_full_path, logger.echo)


@pytest.mark.parametrize("mock_bootstrap_lookback_in_minutes", [60, None])
@patch("deadline.client.api._queue_apis._pid_utils.release_pid_lock")
@patch("deadline.client.api._queue_apis._pid_utils.try_acquire_pid_lock")
@patch("deadline.client.api._queue_apis.bootstrap_fresh_state")
@patch("deadline.client.api._queue_apis.save_progress_to_state_file")
@patch("deadline.client.api._queue_apis.aggregate_manifest_and_download_outputs")
@patch("deadline.client.api._queue_apis.get_list_of_ongoing_jobs_on_queue")
@patch("deadline.client.api._queue_apis.update_download_state_using_ongoing_sessions")
@patch("deadline.client.api._queue_apis.SessionActionProcessor")
@freeze_time("2025-05-26 12:00:00")
def test_incremental_output_download_success_with_force_bootstrap(
    mock_session_action_processor_class,
    mock_update_download_state,
    mock_get_ongoing_jobs,
    mock_aggregate_manifest,
    mock_save_progress,
    mock_bootstrap_fresh_state,
    mock_acquire_pid_lock,
    mock_release_pid_lock,
    tmp_path,
    mock_bootstrap_lookback_in_minutes,
):
    """Test successful execution of _incremental_output_download with bootstrapping"""
    # Arrange
    farm_id = "farm-0123456789abcdef"
    queue_id = "queue-0123456789abcdef"
    boto3_session = MagicMock(spec=boto3.Session)
    # Mock boto3 client and responses
    mock_client = MagicMock()
    boto3_session.client.return_value = mock_client
    mock_client.get_queue.return_value = {
        "jobAttachmentSettings": {"bucketName": "test-bucket", "keyPrefix": "test-prefix"},
        "displayName": "Test Queue",
    }

    saved_progress_checkpoint_location = str(tmp_path / "checkpoint")
    os.makedirs(saved_progress_checkpoint_location, exist_ok=True)

    pid_file_full_path = os.path.join(
        saved_progress_checkpoint_location, "queue-0123456789abcdef_incremental_output_download.pid"
    )
    saved_progress_checkpoint_full_path: str = os.path.join(
        saved_progress_checkpoint_location, f"{queue_id}_download_progress.json"
    )
    logger: ClickLogger = ClickLogger(is_json=False)

    # Set all assumptions
    expected_current_download_progress: IncrementalDownloadState = IncrementalDownloadState()
    expected_current_download_progress.last_lookback_time = "2025-05-26T11:00:00"
    mock_bootstrap_fresh_state.return_value = expected_current_download_progress
    expected_updated_download_progress = expected_current_download_progress
    expected_updated_download_progress.last_lookback_time = "2025-05-26T12:00:00"

    # Mock the ongoing jobs
    mock_get_ongoing_jobs.return_value = {"job-1234353453443", "Job-3234324354345"}

    # Create a mock for the SessionActionProcessor instance with all required methods
    mock_session_action_processor = MagicMock()
    mock_session_action_processor_class.return_value = mock_session_action_processor

    # Configure get_list_of_ongoing_session_action_ids_for_jobs
    mock_session_action_processor.get_list_of_ongoing_session_action_ids_for_jobs.return_value = [
        MagicMock(session_action_id="session-action-1"),
        MagicMock(session_action_id="session-action-2"),
        MagicMock(session_action_id="session-action-3"),
    ]

    # Configure mock_ongoing_sessions
    mock_ongoing_sessions = [
        MagicMock(job_id="job-1234353453443", session_id="session-1324324354354"),
        MagicMock(job_id="job-1234353453443", session_id="session-3423435435454"),
        MagicMock(job_id="Job-3234324354345", session_id="session-4235435434345"),
    ]

    # Create a separate mock for the specific method we're having trouble with
    get_updated_list_mock = MagicMock(return_value=mock_ongoing_sessions)
    mock_session_action_processor.get_updated_list_of_ongoing_sessions_pending_download = (
        get_updated_list_mock
    )

    # Mock the aggregate manifest download
    mock_aggregate_manifest.return_value = [
        "session-action-1",
        "session-action-2",
        "session-action-3",
    ]

    # Mock the update_download_state_using_ongoing_sessions function
    mock_update_download_state.return_value = expected_updated_download_progress

    # Act
    _incremental_output_download(
        farm_id=farm_id,
        queue_id=queue_id,
        boto3_session=boto3_session,
        saved_progress_checkpoint_location=saved_progress_checkpoint_location,
        bootstrap_lookback_in_minutes=mock_bootstrap_lookback_in_minutes,
        print_function_callback=logger.echo,
        force_bootstrap=True,
    )

    # Assert the calls were made in expected order
    mock_acquire_pid_lock.assert_called_once_with(pid_file_full_path, logger.echo)
    mock_bootstrap_fresh_state.assert_called_once_with(
        mock_bootstrap_lookback_in_minutes, logger.echo
    )

    # Verify the new method calls
    get_updated_list_mock.assert_called_once_with(mock_aggregate_manifest.return_value)

    mock_update_download_state.assert_called_once_with(
        expected_current_download_progress, mock_ongoing_sessions, "2025-05-26T12:00:00"
    )

    mock_save_progress.assert_called_once_with(
        saved_progress_checkpoint_location,
        saved_progress_checkpoint_full_path,
        expected_updated_download_progress,
        logger.echo,
    )

    mock_release_pid_lock.assert_called_once_with(pid_file_full_path, logger.echo)


@patch("deadline.client.api._queue_apis._pid_utils.release_pid_lock")
@patch("deadline.client.api._queue_apis._pid_utils.try_acquire_pid_lock")
def test_incremental_output_download_pid_lock_already_held_error(
    mock_pid_lock, mock_release_lock, tmp_path
):
    """Test _incremental_output_download when PidLockAlreadyHeld is raised"""
    # Arrange
    farm_id = "farm-0123456789abcdef"
    queue_id = "queue-0123456789abcdef"
    boto3_session = MagicMock(spec=boto3.Session)
    # Mock boto3 client and responses
    mock_client = MagicMock()
    boto3_session.client.return_value = mock_client
    mock_client.get_queue.return_value = {
        "jobAttachmentSettings": {"bucketName": "test-bucket", "keyPrefix": "test-prefix"},
        "displayName": "Test Queue",
    }

    saved_progress_checkpoint_location = str(tmp_path / "checkpoint")
    os.makedirs(saved_progress_checkpoint_location, exist_ok=True)

    pid_file_full_path = os.path.join(
        saved_progress_checkpoint_location, "queue-0123456789abcdef_incremental_output_download.pid"
    )
    logger: ClickLogger = ClickLogger(is_json=False)

    # Mock the pid lock to raise PidLockAlreadyHeld
    mock_pid_lock.side_effect = PidLockAlreadyHeld("Lock already held")

    # Act
    _incremental_output_download(
        farm_id=farm_id,
        queue_id=queue_id,
        boto3_session=boto3_session,
        saved_progress_checkpoint_location=saved_progress_checkpoint_location,
        print_function_callback=logger.echo,
    )

    # Assert
    mock_pid_lock.assert_called_once_with(pid_file_full_path, logger.echo)
    mock_release_lock.assert_called_once_with(pid_file_full_path, logger.echo)


def test_validate_file_inputs_for_incremental_output_download_valid_dir(tmp_path):
    """Test _validate_file_inputs_for_incremental_output_download with valid directory"""
    # Arrange
    saved_progress_checkpoint_location = str(tmp_path / "checkpoint")
    os.makedirs(saved_progress_checkpoint_location, exist_ok=True)

    # Act
    result = _validate_file_inputs_for_incremental_output_download(
        saved_progress_checkpoint_location
    )

    # Assert
    assert result is True


def test_validate_file_inputs_for_incremental_output_download_invalid_dir():
    """Test _validate_file_inputs_for_incremental_output_download with invalid directory"""
    # Arrange
    saved_progress_checkpoint_location = "/non/existent/directory"

    # Act & Assert
    with pytest.raises(RuntimeError) as excinfo:
        _validate_file_inputs_for_incremental_output_download(saved_progress_checkpoint_location)

    assert "not a valid directory" in str(excinfo.value)
