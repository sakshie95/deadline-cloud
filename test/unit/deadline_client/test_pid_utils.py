# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import os
import pytest
import psutil
from unittest.mock import patch, mock_open, MagicMock

from deadline.client._pid_utils import (
    check_and_obtain_pid_lock_if_available,
    _obtain_pid_lock_atomically,
)


class TestPidUtils:
    @pytest.fixture
    def mock_logger(self):
        """
        Fixture to create a mock logger object.
        """
        logger = MagicMock()
        logger.echo = MagicMock()
        return logger

    @pytest.fixture
    def test_paths(self):
        """
        Fixture to provide test file paths.
        """
        location = "/path/to/download/location"
        return {
            "location": location,
            "pid_file": os.path.join(location, "incremental_output_download.pid"),
        }

    def test_check_pid_lock_when_file_does_not_exist(self, mock_logger, test_paths):
        """
        Tests the scenario when no PID file exists.
        This is the case when the cli is being bootstrapped.
        """
        with patch("os.path.exists") as mock_exists, patch(
            "deadline.client._pid_utils._obtain_pid_lock_atomically"
        ) as mock_obtain_lock, patch("os.getpid") as mock_get_pid:
            mock_exists.return_value = False
            pid: int = 1234
            mock_get_pid.return_value = pid

            check_and_obtain_pid_lock_if_available(test_paths["location"], str(pid), mock_logger)

            expected_pid_file = os.path.join(
                test_paths["location"], "incremental_output_download.pid"
            )
            mock_obtain_lock.assert_called_once_with(expected_pid_file, mock_logger, 1234)
            assert mock_logger.echo.called

    def test_check_pid_lock_when_process_not_running(self, mock_logger, test_paths):
        """
        Tests when PID file exists but process is not running.
        This is the case when a run was terminated mid-way causing a stale pid file to exist.
        """
        # Create a mock file that will be returned by the context manager
        mock_file = MagicMock()
        mock_file.read.return_value = "1234"

        # Create a context manager mock
        mock_cm = MagicMock()
        mock_cm.__enter__.return_value = mock_file

        with patch("os.path.exists") as mock_exists, patch("psutil.Process") as mock_process, patch(
            "builtins.open", return_value=mock_cm
        ), patch(
            "deadline.client._pid_utils._obtain_pid_lock_atomically"
        ) as mock_obtain_lock, patch("os.getpid") as mock_get_pid:
            mock_exists.return_value = True
            pid: int = 5678
            mock_get_pid.return_value = pid
            mock_process.side_effect = psutil.NoSuchProcess(1234)

            check_and_obtain_pid_lock_if_available(test_paths["location"], str(pid), mock_logger)

            expected_pid_file = os.path.join(
                test_paths["location"], "incremental_output_download.pid"
            )

            # Verify the file operations happened in the correct order
            mock_file.read.assert_called_once()
            mock_file.close.assert_called_once()
            mock_obtain_lock.assert_called_once_with(expected_pid_file, mock_logger, 5678)

    def test_check_pid_lock_when_process_running(self, mock_logger, test_paths):
        """
        Tests when PID file exists and process is running.
        This is the case when there is a run already ongoing for the CLI so the new one needs to be terminated.
        """
        with patch("os.path.exists") as mock_exists, patch("psutil.Process") as mock_process, patch(
            "builtins.open", mock_open(read_data="1234")
        ):
            mock_exists.return_value = True
            mock_process.return_value = MagicMock()  # Process exists

            with pytest.raises(RuntimeError) as exc_info:
                check_and_obtain_pid_lock_if_available(test_paths["location"], "1234", mock_logger)

            assert "Another download is in progress" in str(exc_info.value)
            assert mock_logger.echo.called

    def test_obtain_pid_lock_atomically(self, mock_logger, test_paths):
        """
        Tests the atomic file writing operation for the PID lock.
        """
        test_pid = 5678
        pid_file = os.path.join(test_paths["location"], "incremental_output_download.pid")
        tmp_file = f"{pid_file}{test_pid}~tmp"

        # Create the mock_open correctly
        mocked_open = mock_open()

        with patch("builtins.open", mocked_open) as mock_file_open, patch(
            "os.getpid"
        ) as mock_getpid, patch("os.replace") as mock_replace, patch("os.fsync") as mock_fsync:
            mock_getpid.return_value = test_pid

            _obtain_pid_lock_atomically(pid_file, mock_logger, test_pid)

            # Verify file operations
            mock_file_open.assert_called_once_with(tmp_file, "w+")
            handle = mock_file_open()
            handle.write.assert_called_once_with(str(test_pid))
            handle.flush.assert_called_once()
            mock_fsync.assert_called_once_with(handle.fileno())
            mock_replace.assert_called_once_with(tmp_file, pid_file)
            mock_logger.echo.assert_called_with(
                f"Creating new pid file at {pid_file} with pid {test_pid}"
            )

    @pytest.mark.parametrize(
        ("error", "expected_message"),
        [
            (PermissionError("Access denied"), "Access denied"),
            (OSError("Disk full"), "Disk full"),
            (Exception("Unexpected error"), "Unexpected error"),
        ],
    )
    def test_check_pid_lock_with_errors(self, mock_logger, test_paths, error, expected_message):
        """
        Tests various error conditions when checking PID lock.
        """
        with patch("os.path.exists") as mock_exists, patch("builtins.open", side_effect=error):
            mock_exists.return_value = True

            with pytest.raises(type(error)) as exc_info:
                check_and_obtain_pid_lock_if_available(test_paths["location"], "1234", mock_logger)

            assert str(exc_info.value) == expected_message

    def test_release_pid_lock_when_file_does_not_exist(self, mock_logger, test_paths):
        """
        Tests releasing a PID lock when the file doesn't exist.
        """
        with patch("os.path.exists") as mock_exists:
            mock_exists.return_value = False

            from deadline.client._pid_utils import release_pid_lock

            result = release_pid_lock(test_paths["location"], "1234", mock_logger)

            assert result is True
            assert mock_logger.echo.called
            assert "Pid lock file does not exist" in mock_logger.echo.call_args_list[1][0][0]

    def test_release_pid_lock_when_pid_matches(self, mock_logger, test_paths):
        """
        Tests releasing a PID lock when the PID matches the current process.
        """
        with patch("os.path.exists") as mock_exists, patch(
            "builtins.open", mock_open(read_data="1234")
        ), patch("os.remove") as mock_remove:
            mock_exists.return_value = True

            from deadline.client._pid_utils import release_pid_lock

            result = release_pid_lock(test_paths["location"], "1234", mock_logger)

            assert result is True
            assert mock_logger.echo.called
            assert "Deleting pid file" in mock_logger.echo.call_args_list[1][0][0]
            mock_remove.assert_called_once()

    def test_release_pid_lock_when_pid_does_not_match(self, mock_logger, test_paths):
        """
        Tests releasing a PID lock when the PID doesn't match the current process.
        """
        with patch("os.path.exists") as mock_exists, patch(
            "builtins.open", mock_open(read_data="5678")
        ):
            mock_exists.return_value = True

            from deadline.client._pid_utils import release_pid_lock

            result = release_pid_lock(test_paths["location"], "1234", mock_logger)

            assert result is False
            assert mock_logger.echo.called
            assert "Skipping pid file deletion" in mock_logger.echo.call_args_list[1][0][0]

    def test_check_pid_lock_concurrent_access(self, mock_logger, test_paths):
        """
        Tests the scenario when two threads with different PIDs try to obtain a lock.
        The first thread should succeed, and the second thread should fail.
        """
        # Setup
        pid1 = "1234"
        pid2 = "5678"
        pid_file_path = os.path.join(test_paths["location"], "incremental_output_download.pid")

        # Mock file operations and process checks
        with patch("os.path.exists") as mock_exists, patch(
            "builtins.open", mock_open(read_data=pid1)
        ) as mock_file, patch("psutil.Process") as mock_process, patch(
            "deadline.client._pid_utils._obtain_pid_lock_atomically"
        ) as mock_obtain_lock:
            # First call - file doesn't exist, lock is obtained
            mock_exists.side_effect = [False]  # First PID file doesn't exist
            mock_obtain_lock.return_value = True

            mock_file.return_value = MagicMock()

            # First thread obtains lock successfully
            result1 = check_and_obtain_pid_lock_if_available(
                test_paths["location"], pid1, mock_logger
            )
            assert result1 is True
            mock_obtain_lock.assert_called_once_with(pid_file_path, mock_logger, int(pid1))

            # Reset mocks for second call
            mock_exists.reset_mock()
            mock_obtain_lock.reset_mock()

            # Second call - file exists, process is running
            mock_exists.side_effect = [True]  # Second PID file exists
            mock_process.return_value = MagicMock()  # Process exists/is running

            # Second thread fails to obtain lock
            with pytest.raises(RuntimeError) as exc_info:
                check_and_obtain_pid_lock_if_available(test_paths["location"], pid2, mock_logger)

            assert "Another download is in progress" in str(exc_info.value)
            mock_obtain_lock.assert_not_called()  # Lock should not be attempted

            # Verify logger was called appropriately
            assert mock_logger.echo.called
