# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import json
import os
import pytest
from unittest.mock import patch, MagicMock

import boto3
from deadline.client.api._queue_apis import (
    _incremental_output_download,
    _validate_file_inputs_for_incremental_output_download,
)
from deadline.client.cli._groups.click_logger import ClickLogger
from freezegun import freeze_time


# Fixtures for shared resources
@pytest.fixture(scope="module")
def checkpoint_dir(tmp_path_factory):
    """Create a checkpoint directory for all tests to use."""
    checkpoint_dir = tmp_path_factory.mktemp("checkpoint")
    yield str(checkpoint_dir)
    # No cleanup needed here as tmp_path_factory handles it automatically


@pytest.fixture(scope="module")
def queue_id():
    """Return a consistent queue ID for all tests."""
    return "queue-0123456789abcdef"


@pytest.fixture(scope="module")
def farm_id():
    """Return a consistent farm ID for all tests."""
    return "farm-0123456789abcdef"


@pytest.fixture(scope="module")
def boto3_session():
    """Create a mock boto3 session for all tests to use."""
    return MagicMock(spec=boto3.Session)


@pytest.fixture
def progress_file(checkpoint_dir, queue_id):
    """Create a progress file path for tests that need it.

    This has function scope so each test gets a fresh file.
    """
    progress_file_path = os.path.join(checkpoint_dir, f"{queue_id}_download_progress.json")
    # File will be created by the test that needs it
    yield progress_file_path
    # Clean up after each test
    if os.path.exists(progress_file_path):
        os.remove(progress_file_path)


@pytest.fixture
def pid_file(checkpoint_dir, queue_id):
    """Create a PID file path for tests that need it."""
    pid_file_path = os.path.join(checkpoint_dir, f"{queue_id}_incremental_output_download.pid")
    yield pid_file_path
    # Clean up
    if os.path.exists(pid_file_path):
        os.remove(pid_file_path)


@pytest.fixture
def rules_file(tmp_path_factory):
    """Create a rules file for tests that need it."""
    # Create in a separate directory to avoid conflicts
    rules_dir = tmp_path_factory.mktemp("rules")
    rules_file_path = os.path.join(str(rules_dir), "rules.json")
    yield rules_file_path
    # Clean up
    if os.path.exists(rules_file_path):
        os.remove(rules_file_path)


@freeze_time("2025-05-26 12:00:00")
def test_incremental_output_download_success_load_from_progress(
    checkpoint_dir, queue_id, farm_id, boto3_session, progress_file
):
    """Test successful execution of _incremental_output_download with loading progress from state file"""
    # Create a real progress file with test data
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

    # Write the initial progress file
    with open(progress_file, "w") as f:
        json.dump(download_progress_json, f, indent=2)

    logger = ClickLogger(is_json=False)

    # Act
    _incremental_output_download(
        farm_id=farm_id,
        queue_id=queue_id,
        boto3_session=boto3_session,
        saved_progress_checkpoint_location=checkpoint_dir,
        print_function_callback=logger.echo,
    )

    # Assert
    # Check that the progress file was updated
    with open(progress_file, "r") as f:
        updated_progress = json.load(f)

    # Verify the lastLookbackTime was updated to the frozen time
    assert updated_progress["lastLookbackTime"] == "2025-05-26T12:00:00Z"

    # Verify the job data was preserved
    assert len(updated_progress["jobs"]) == 2
    assert updated_progress["jobs"][0]["jobId"] == "job-1234353453443"


@freeze_time("2025-05-26 12:00:00")
@pytest.mark.parametrize("mock_bootstrap_lookback_in_minutes", [60, None])
def test_incremental_output_download_success_with_force_bootstrap(
    checkpoint_dir,
    queue_id,
    farm_id,
    boto3_session,
    progress_file,
    mock_bootstrap_lookback_in_minutes,
):
    """Test successful execution of _incremental_output_download with bootstrapping"""
    # Create a file that should be ignored due to force_bootstrap=True
    with open(progress_file, "w") as f:
        json.dump({"lastLookbackTime": "2025-01-01T00:00:00", "jobs": []}, f)

    logger = ClickLogger(is_json=False)

    # Act
    _incremental_output_download(
        farm_id=farm_id,
        queue_id=queue_id,
        boto3_session=boto3_session,
        saved_progress_checkpoint_location=checkpoint_dir,
        bootstrap_lookback_in_minutes=mock_bootstrap_lookback_in_minutes,
        print_function_callback=logger.echo,
        force_bootstrap=True,
    )

    # Assert
    # Check that the progress file was updated
    with open(progress_file, "r") as f:
        updated_progress = json.load(f)

    # Verify the lastLookbackTime was updated to the frozen time
    assert updated_progress["lastLookbackTime"] == "2025-05-26T12:00:00Z"

    # Verify the jobs list is empty (as it would be with a fresh bootstrap)
    assert updated_progress["jobs"] == []


@patch("psutil.pid_exists")
def test_incremental_output_download_pid_lock_already_held_error(
    mock_pid_exists, checkpoint_dir, queue_id, farm_id, boto3_session, pid_file
):
    """Test _incremental_output_download when PidLockAlreadyHeld is raised"""
    # Write a fake PID to the file
    with open(pid_file, "w") as f:
        # Use a fake PID
        f.write("12345")

    # Make psutil.pid_exists return True to simulate the process is running
    mock_pid_exists.return_value = True

    logger = MagicMock(spec=ClickLogger)

    # Act
    _incremental_output_download(
        farm_id=farm_id,
        queue_id=queue_id,
        boto3_session=boto3_session,
        saved_progress_checkpoint_location=checkpoint_dir,
        print_function_callback=logger.echo,
    )

    # Assert
    # Verify the PID file still exists since we're simulating another process holding the lock
    assert os.path.exists(pid_file)

    logger.echo.assert_any_call(
        f"Another download is in progress at {checkpoint_dir}, wait for previous download to finish"
    )


@patch("os.access")
def test_incremental_output_download_generic_exception(
    mock_access, checkpoint_dir, queue_id, farm_id, boto3_session
):
    """Test _incremental_output_download when a generic Exception is raised"""

    # Mock os.access to simulate a permission error
    def access_side_effect(path, mode):
        # Only deny write access to the checkpoint directory
        if path == checkpoint_dir and mode == os.W_OK:
            return False
        return True

    mock_access.side_effect = access_side_effect

    logger = MagicMock(spec=ClickLogger)

    # Act
    _incremental_output_download(
        farm_id=farm_id,
        queue_id=queue_id,
        boto3_session=boto3_session,
        saved_progress_checkpoint_location=checkpoint_dir,
        print_function_callback=logger.echo,
    )

    # Assert
    # We only need to verify that the function completes without error


def test_validate_file_inputs_success(checkpoint_dir):
    """Test successful validation of file inputs"""
    # Act
    result = _validate_file_inputs_for_incremental_output_download(checkpoint_dir)

    # Assert
    assert result is True


def test_validate_file_inputs_invalid_directory(tmp_path):
    """Test validation when directory is invalid"""
    # Arrange
    nonexistent_dir = str(tmp_path / "nonexistent_directory")

    # Act & Assert
    with pytest.raises(RuntimeError) as excinfo:
        _validate_file_inputs_for_incremental_output_download(nonexistent_dir)

    assert "is not a valid directory" in str(excinfo.value)


@patch("os.access")
def test_validate_file_inputs_not_writable(mock_access, tmp_path):
    """Test validation when directory is not writable"""
    # Arrange
    readonly_dir = str(tmp_path / "readonly_dir")

    # Create the directory
    os.makedirs(readonly_dir, exist_ok=True)

    # Mock os.access to simulate a read-only directory
    def access_side_effect(path, mode):
        if path == readonly_dir and mode == os.W_OK:
            return False
        return True

    mock_access.side_effect = access_side_effect

    # Act & Assert
    with pytest.raises(RuntimeError) as excinfo:
        _validate_file_inputs_for_incremental_output_download(readonly_dir)

    assert "is not writable" in str(excinfo.value)


def test_validate_file_inputs_with_mapping_rules_success(checkpoint_dir, rules_file):
    """Test successful validation with path mapping rules"""
    # Create the rules file with valid content
    with open(rules_file, "w") as f:
        f.write('{"rules": []}')

    # Act
    result = _validate_file_inputs_for_incremental_output_download(checkpoint_dir, rules_file)

    # Assert
    assert result is True


def test_validate_file_inputs_mapping_rules_not_exist(checkpoint_dir, tmp_path):
    """Test validation when mapping rules file doesn't exist"""
    # Arrange
    nonexistent_rules = str(tmp_path / "nonexistent_rules.json")

    # Act & Assert
    with pytest.raises(RuntimeError) as excinfo:
        _validate_file_inputs_for_incremental_output_download(checkpoint_dir, nonexistent_rules)

    assert "does not exist" in str(excinfo.value)


@patch("os.access")
def test_validate_file_inputs_mapping_rules_not_readable(mock_access, checkpoint_dir, rules_file):
    """Test validation when mapping rules file is not readable"""
    # Create the rules file
    with open(rules_file, "w") as f:
        f.write('{"rules": []}')

    # Mock os.access to simulate a non-readable rules file
    def access_side_effect(path, mode):
        if path == rules_file and mode == os.R_OK:
            return False
        return True

    mock_access.side_effect = access_side_effect

    # Act & Assert
    with pytest.raises(RuntimeError) as excinfo:
        _validate_file_inputs_for_incremental_output_download(checkpoint_dir, rules_file)

    assert "is not readable" in str(excinfo.value)
