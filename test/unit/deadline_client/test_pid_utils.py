# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import os
import sys
import pytest
import psutil
from unittest.mock import patch, mock_open, MagicMock

from deadline.client._pid_utils import (
    check_and_obtain_pid_lock_if_available,
    _lock_pid_file_and_release_dangling_pid_lock,
)

# Constants for file locking to avoid platform-specific module references
# These match the values in the respective modules
LOCK_EX = 2  # fcntl.LOCK_EX - Exclusive lock
LOCK_UN = 8  # fcntl.LOCK_UN - Unlock
LK_RLCK = 0  # msvcrt.LK_RLCK - Lock
LK_UNLCK = 3  # msvcrt.LK_UNLCK - Unlock


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
            "pid_file": os.path.join(location, "queue-12345_incremental_output_download.pid"),
        }

    @pytest.fixture
    def setup_mock_file(self, test_paths, pid="1234"):
        """
        Fixture to create and configure a mock file object for testing.
        """
        mock_file = MagicMock()
        mock_file.read.return_value = pid
        mock_file.fileno.return_value = 5
        return mock_file

    def verify_common_assertions(
        self, mock_open_file, mock_file, pid_file, mock_getsize, mock_realpath
    ):
        """
        Helper method to verify common assertions across tests.
        """
        mock_open_file.assert_called_once_with(pid_file, "r+")
        mock_file.read.assert_called_once()
        mock_getsize.assert_called_once_with(pid_file)
        mock_realpath.assert_called_once_with(pid_file)

    def test_check_pid_lock_when_file_does_not_exist(self, mock_logger, test_paths):
        """
        Tests the scenario when no PID file exists.
        This is the case when the cli is being bootstrapped.
        """
        with patch("os.path.exists") as mock_exists, patch("os.getpid") as mock_get_pid, patch(
            "builtins.open", mock_open()
        ) as mock_file, patch("os.fsync") as mock_fsync, patch(
            "os.link"
            if sys.platform.startswith("linux") or sys.platform.startswith("darwin")
            else "os.rename"
        ) as mock_atomic_op, patch("os.remove") as mock_remove:
            mock_exists.return_value = False
            pid: int = 1234
            mock_get_pid.return_value = pid

            expected_pid_file = os.path.join(
                test_paths["location"], "queue-12345_incremental_output_download.pid"
            )

            result: bool = check_and_obtain_pid_lock_if_available(
                expected_pid_file, mock_logger.echo
            )

            # Verify file operations for atomic write
            mock_file.assert_called_once_with(f"{expected_pid_file}{pid}~tmp", "w+")
            handle = mock_file()
            handle.write.assert_called_once_with(str(pid))
            handle.flush.assert_called_once()
            mock_fsync.assert_called_once()
            if sys.platform.startswith("linux") or sys.platform.startswith("darwin"):
                mock_remove.assert_called_once()
            mock_atomic_op.assert_called_once()
            assert result is True

    def test_check_pid_lock_when_process_not_running(self, mock_logger, test_paths):
        """
        Tests when PID file exists but process is not running.
        This is the case when a run was terminated mid-way causing a stale pid file to exist.
        """
        # Create a mock file that will be returned by the context manager
        mock_file = MagicMock()
        mock_file.read.return_value = "1234"
        mock_file.name = os.path.join(
            test_paths["location"], "queue-12345_incremental_output_download.pid"
        )

        # Create a separate mock for the context manager
        mock_cm = MagicMock()
        mock_cm.__enter__.return_value = mock_file

        with patch("os.path.exists") as mock_exists, patch("psutil.Process") as mock_process, patch(
            "builtins.open", return_value=mock_cm
        ), patch(
            "deadline.client._pid_utils._lock_pid_file_and_release_dangling_pid_lock"
        ) as mock_lock_and_release, patch("os.getpid") as mock_get_pid, patch(
            "os.fsync"
        ) as mock_fsync, patch(
            "os.link"
            if sys.platform.startswith("linux") or sys.platform.startswith("darwin")
            else "os.rename"
        ) as mock_atomic_op, patch("os.remove") as mock_remove:
            mock_exists.return_value = True
            pid: int = 5678
            mock_get_pid.return_value = pid
            mock_process.side_effect = psutil.NoSuchProcess(1234)

            expected_pid_file = os.path.join(
                test_paths["location"], "queue-12345_incremental_output_download.pid"
            )

            check_and_obtain_pid_lock_if_available(expected_pid_file, mock_logger.echo)

            # Verify the file operations happened in the correct order
            mock_file.read.assert_called_once()
            mock_file.close.assert_called_once()
            mock_fsync.assert_called_once()
            mock_atomic_op.assert_called_once()
            mock_lock_and_release.assert_called_once_with("1234", expected_pid_file)

            if sys.platform.startswith("linux") or sys.platform.startswith("darwin"):
                mock_remove.assert_called_once()

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

            expected_pid_file = os.path.join(
                test_paths["location"], "queue-12345_incremental_output_download.pid"
            )

            with pytest.raises(RuntimeError) as exc_info:
                check_and_obtain_pid_lock_if_available(expected_pid_file, mock_logger)

            assert "Unable to acquire pid lock" in str(exc_info.value)

    @pytest.mark.skipif(
        not sys.platform.startswith("linux") and not sys.platform.startswith("darwin"),
        reason="Test only applicable on Linux/macOS",
    )
    def test_lock_pid_file_and_release_dangling_pid_lock_linux_mac(
        self, mock_logger, test_paths, setup_mock_file
    ):
        """
        Tests the locking and releasing of a dangling PID lock on Linux/macOS.
        """
        pid = "1234"
        pid_file = os.path.join(
            test_paths["location"], "queue-12345_incremental_output_download.pid"
        )

        # Use the fixture for mock file
        mock_file = setup_mock_file
        mock_file.read.return_value = pid

        with patch("sys.platform", "linux"), patch(
            "builtins.open", return_value=mock_file
        ) as mock_open_file, patch("fcntl.flock") as mock_flock, patch(
            "os.path.getsize", return_value=10
        ) as mock_getsize, patch("os.path.realpath", return_value=pid_file) as mock_realpath, patch(
            "os.remove"
        ) as mock_remove:
            result = _lock_pid_file_and_release_dangling_pid_lock(pid, pid_file)

            # Use helper method for common assertions
            self.verify_common_assertions(
                mock_open_file, mock_file, pid_file, mock_getsize, mock_realpath
            )

            # Additional specific assertions
            mock_flock.assert_any_call(mock_file.fileno(), LOCK_EX)  # Exclusive lock
            mock_flock.assert_any_call(mock_file.fileno(), LOCK_UN)  # Unlock
            mock_remove.assert_called_once_with(pid_file)
            assert result is True

            # Verify operations
            mock_open_file.assert_called_once_with(pid_file, "r+")
            mock_flock.assert_any_call(mock_file.fileno(), LOCK_EX)  # Exclusive lock
            mock_flock.assert_any_call(mock_file.fileno(), LOCK_UN)  # Unlock
            mock_file.read.assert_called_once()
            mock_remove.assert_called_once_with(pid_file)
            mock_getsize.assert_called_once_with(pid_file)
            mock_realpath.assert_called_once_with(pid_file)
            assert result is True

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
                check_and_obtain_pid_lock_if_available(test_paths["location"], mock_logger)

            assert str(exc_info.value) == expected_message

    def test_release_pid_lock_when_file_does_not_exist(self, mock_logger, test_paths):
        """
        Tests releasing a PID lock when the file doesn't exist.
        """
        with patch("os.path.exists") as mock_exists:
            mock_exists.return_value = False

            from deadline.client._pid_utils import release_pid_lock

            result = release_pid_lock(test_paths["location"], mock_logger)

            assert result is True

    def test_release_pid_lock_when_pid_matches(self, mock_logger, test_paths):
        """
        Tests releasing a PID lock when the PID matches the current process.
        """
        pid_file_path = os.path.join(
            test_paths["location"], "queue-12345_incremental_output_download.pid"
        )

        with patch("os.path.exists") as mock_exists, patch(
            "builtins.open", mock_open(read_data="1234")
        ), patch("os.remove") as mock_remove, patch("os.getpid") as mock_get_pid:
            mock_exists.return_value = True
            mock_get_pid.return_value = 1234

            from deadline.client._pid_utils import release_pid_lock

            result = release_pid_lock(pid_file_path, mock_logger)

            assert result is True
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

            result = release_pid_lock(test_paths["location"], mock_logger)

            assert result is False

    def test_check_pid_lock_concurrent_access(self, mock_logger, test_paths):
        """
        Tests the scenario when two processes with different PIDs try to obtain a lock.
        The first process should succeed, and the second process should fail.
        """
        # Setup
        pid1 = 1234
        pid_file_path = os.path.join(
            test_paths["location"], "queue-12345_incremental_output_download.pid"
        )

        # Mock file operations and process checks
        with patch("os.path.exists") as mock_exists, patch(
            "builtins.open", mock_open(read_data=str(pid1))
        ) as mock_file, patch("psutil.Process") as mock_process, patch(
            "os.getpid"
        ) as mock_get_pid, patch("os.fsync") as mock_fsync, patch(
            "os.link"
            if sys.platform.startswith("linux") or sys.platform.startswith("darwin")
            else "os.rename"
        ) as mock_atomic_op, patch("os.remove") as mock_remove:
            # First call - file doesn't exist, lock is obtained
            mock_exists.side_effect = [False]  # First PID file doesn't exist
            mock_get_pid.return_value = pid1

            # First process obtains lock successfully
            result1 = check_and_obtain_pid_lock_if_available(pid_file_path, mock_logger.echo)
            assert result1 is True

            # Verify file operations for atomic write
            mock_file.assert_called_once_with(f"{pid_file_path}{pid1}~tmp", "w+")
            handle = mock_file()
            handle.write.assert_called_once_with(str(pid1))
            handle.flush.assert_called_once()
            mock_fsync.assert_called_once()
            if sys.platform.startswith("linux") or sys.platform.startswith("darwin"):
                mock_remove.assert_called_once()
            mock_atomic_op.assert_called_once()

            # Reset mocks for second call
            mock_exists.reset_mock()
            mock_file.reset_mock()
            mock_fsync.reset_mock()
            mock_remove.reset_mock()
            mock_atomic_op.reset_mock()

            # Second call - file exists, process is running
            mock_exists.side_effect = [True]  # Second PID file exists
            mock_process.return_value = MagicMock()  # Process exists/is running

            # Second process fails to obtain lock
            with pytest.raises(RuntimeError) as exc_info:
                check_and_obtain_pid_lock_if_available(pid_file_path, mock_logger.echo)

            assert "Unable to acquire pid lock" in str(exc_info.value)
            mock_atomic_op.assert_not_called()  # Atomic operation should not be attempted

    @pytest.mark.skipif(
        not sys.platform.startswith("win"), reason="Test only applicable on Windows"
    )
    def test_lock_pid_file_and_release_dangling_pid_lock_different_pid_windows(
        self, mock_logger, test_paths, setup_mock_file
    ):
        """
        Tests the locking and releasing of a dangling PID lock when the pid in the file has changed on Windows.
        """
        original_pid = "1234"
        changed_pid = "5678"
        pid_file = os.path.join(
            test_paths["location"], "queue-12345_incremental_output_download.pid"
        )

        # Use the fixture for mock file
        mock_file = setup_mock_file
        mock_file.read.return_value = changed_pid  # Different PID than what was passed

        with patch("sys.platform", "win32"), patch(
            "builtins.open", return_value=mock_file
        ) as mock_open_file, patch("msvcrt.locking") as mock_locking, patch(
            "os.path.getsize", return_value=10
        ) as mock_getsize, patch("os.path.realpath", return_value=pid_file) as mock_realpath, patch(
            "os.remove"
        ) as mock_remove:
            # Should raise RuntimeError because PIDs don't match
            with pytest.raises(RuntimeError) as exc_info:
                _lock_pid_file_and_release_dangling_pid_lock(original_pid, pid_file)

            assert "Unable to acquire pid lock" in str(exc_info.value)

            # Use helper method for common assertions
            self.verify_common_assertions(
                mock_open_file, mock_file, pid_file, mock_getsize, mock_realpath
            )

            # Additional specific assertions
            mock_locking.assert_any_call(5, LK_RLCK, 10)  # Lock
            mock_locking.assert_any_call(5, LK_UNLCK, 10)  # Unlock
            # Should not remove the file since PIDs don't match
            mock_remove.assert_not_called()

    def test_check_pid_lock_file_exists_error(self, mock_logger, test_paths):
        """
        Tests the scenario when trying to create a pid file but it already exists (race condition).
        """
        pid = 1234
        pid_file_path = os.path.join(
            test_paths["location"], "queue-12345_incremental_output_download.pid"
        )

        with patch("os.path.exists") as mock_exists, patch(
            "builtins.open", mock_open()
        ) as mock_file, patch("os.getpid") as mock_get_pid, patch("os.fsync"), patch(
            "os.link"
            if sys.platform.startswith("linux") or sys.platform.startswith("darwin")
            else "os.rename",
            side_effect=FileExistsError("File exists"),
        ):
            mock_exists.return_value = False
            mock_get_pid.return_value = pid

            with pytest.raises(FileExistsError):
                check_and_obtain_pid_lock_if_available(pid_file_path, mock_logger.echo)

            # Verify file operations were attempted
            mock_file.assert_called_once_with(f"{pid_file_path}{pid}~tmp", "w+")

    def test_concurrent_pid_lock_acquisition(self, mock_logger, test_paths):
        """
        Tests the scenario where two processes try to obtain a lock on the PID file simultaneously.
        One should succeed and the other should fail based on which one calls os.rename or os.link first.
        """
        pid1 = 1234
        pid2 = 5678
        pid_file_path = os.path.join(
            test_paths["location"], "queue-12345_incremental_output_download.pid"
        )

        # Mock for process 1
        mock_file1 = mock_open()
        # Mock for process 2
        mock_file2 = mock_open()

        # Setup the atomic operation mock to succeed for process 1 and fail for process 2
        atomic_op_name = (
            "os.link"
            if sys.platform.startswith("linux") or sys.platform.startswith("darwin")
            else "os.rename"
        )

        with patch("os.path.exists") as mock_exists, patch("os.getpid") as mock_get_pid, patch(
            "os.fsync"
        ) as mock_fsync, patch("os.remove") as mock_remove:
            # Both processes see that the file doesn't exist
            mock_exists.return_value = False

            # Process 1 execution
            with patch("builtins.open", mock_file1) as mock_open1, patch(
                atomic_op_name
            ) as mock_atomic_op1:
                mock_get_pid.return_value = pid1

                # Process 1 succeeds in creating the lock
                result1 = check_and_obtain_pid_lock_if_available(pid_file_path, mock_logger.echo)

                # Verify process 1 operation
                assert result1 is True
                mock_fsync.assert_called_once()
                if sys.platform.startswith("linux") or sys.platform.startswith("darwin"):
                    mock_remove.assert_called_once()
                mock_open1.assert_called_once_with(f"{pid_file_path}{pid1}~tmp", "w+")
                handle1 = mock_open1()
                handle1.write.assert_called_once_with(str(pid1))
                handle1.flush.assert_called_once()
                mock_atomic_op1.assert_called_once()

            # Process 2 execution - the file now exists due to process 1
            # But we'll simulate the race condition where process 2 also checks before process 1 creates the file
            with patch("builtins.open", mock_file2) as mock_open2, patch(
                atomic_op_name, side_effect=FileExistsError("File exists")
            ) as mock_atomic_op2:
                mock_get_pid.return_value = pid2

                # Process 2 should fail with FileExistsError when trying to create the lock
                with pytest.raises(FileExistsError):
                    check_and_obtain_pid_lock_if_available(pid_file_path, mock_logger.echo)

                # Verify process 2 operations
                mock_open2.assert_called_once_with(f"{pid_file_path}{pid2}~tmp", "w+")
                handle2 = mock_open2()
                handle2.write.assert_called_once_with(str(pid2))
                handle2.flush.assert_called_once()
                mock_atomic_op2.assert_called_once()

    @pytest.mark.skipif(
        not sys.platform.startswith("win"), reason="Test only applicable on Windows"
    )
    def test_lock_pid_file_and_release_dangling_pid_lock_windows(
        self, mock_logger, test_paths, setup_mock_file
    ):
        """
        Tests the locking and releasing of a dangling PID lock on Windows.
        """
        pid = "1234"
        pid_file = os.path.join(
            test_paths["location"], "queue-12345_incremental_output_download.pid"
        )

        # Use the fixture for mock file
        mock_file = setup_mock_file
        mock_file.read.return_value = pid

        with patch("sys.platform", "win32"), patch(
            "builtins.open", return_value=mock_file
        ) as mock_open_file, patch("msvcrt.locking") as mock_locking, patch(
            "os.path.getsize", return_value=10
        ) as mock_getsize, patch("os.path.realpath", return_value=pid_file) as mock_realpath, patch(
            "os.remove"
        ) as mock_remove:
            result = _lock_pid_file_and_release_dangling_pid_lock(pid, pid_file)

            # Use helper method for common assertions
            self.verify_common_assertions(
                mock_open_file, mock_file, pid_file, mock_getsize, mock_realpath
            )

            # Additional specific assertions
            mock_locking.assert_any_call(5, LK_RLCK, 10)  # Lock
            mock_locking.assert_any_call(5, LK_UNLCK, 10)  # Unlock
            mock_remove.assert_called_once_with(pid_file)
            assert result is True

    @pytest.mark.skipif(
        not sys.platform.startswith("linux") and not sys.platform.startswith("darwin"),
        reason="Test only applicable on Linux/macOS",
    )
    def test_lock_pid_file_and_release_dangling_pid_lock_different_pid_linux_mac(
        self, mock_logger, test_paths
    ):
        """
        Tests the locking and releasing of a dangling PID lock when the pid in the file has changed on Linux/macOS.
        """
        original_pid = "1234"
        changed_pid = "5678"
        pid_file = os.path.join(
            test_paths["location"], "queue-12345_incremental_output_download.pid"
        )

        # Create mock file object
        mock_file = MagicMock()
        mock_file.read.return_value = changed_pid  # Different PID than what was passed
        mock_file.fileno.return_value = 5

        with patch("sys.platform", "linux"), patch(
            "builtins.open", return_value=mock_file
        ) as mock_open_file, patch("fcntl.flock") as mock_flock, patch(
            "os.path.getsize", return_value=10
        ) as mock_getsize, patch("os.path.realpath", return_value=pid_file) as mock_realpath, patch(
            "os.remove"
        ) as mock_remove:
            # Should raise RuntimeError because PIDs don't match
            with pytest.raises(RuntimeError) as exc_info:
                _lock_pid_file_and_release_dangling_pid_lock(original_pid, pid_file)

            assert "Unable to acquire pid lock" in str(exc_info.value)

            # Verify operations
            mock_open_file.assert_called_once_with(pid_file, "r+")
            mock_flock.assert_any_call(mock_file.fileno(), LOCK_EX)  # Exclusive lock
            mock_flock.assert_any_call(mock_file.fileno(), LOCK_UN)  # Unlock
            mock_file.read.assert_called_once()
            # Should not remove the file since PIDs don't match
            mock_remove.assert_not_called()
            mock_getsize.assert_called_once_with(pid_file)
            mock_realpath.assert_called_once_with(pid_file)

    def test_lock_pid_file_and_release_dangling_pid_lock_exception(self, mock_logger, test_paths):
        """
        Tests the exception handling in _lock_pid_file_and_release_dangling_pid_lock.
        """
        pid = "1234"
        pid_file = os.path.join(
            test_paths["location"], "queue-12345_incremental_output_download.pid"
        )

        # Create mock file object that raises an exception when read
        mock_file = MagicMock()
        mock_file.read.side_effect = Exception("Failed to read file")
        mock_file.fileno.return_value = 5

        with patch("builtins.open", return_value=mock_file) as mock_open_file, patch(
            "os.path.getsize", return_value=10
        ) as mock_getsize, patch("os.path.realpath", return_value=pid_file) as mock_realpath, patch(
            "os.remove"
        ) as mock_remove:
            # Should raise Exception because of the read error
            with pytest.raises(Exception):
                _lock_pid_file_and_release_dangling_pid_lock(pid, pid_file)

            # Verify operations
            mock_open_file.assert_called_once_with(pid_file, "r+")
            mock_file.read.assert_called_once()
            # Should not remove the file due to exception
            mock_remove.assert_not_called()
            mock_getsize.assert_called_once_with(pid_file)
            mock_realpath.assert_called_once_with(pid_file)

    @pytest.mark.skipif(
        not sys.platform.startswith("linux") and not sys.platform.startswith("darwin"),
        reason="Test only applicable on Linux/macOS",
    )
    def test_lock_pid_file_and_release_dangling_pid_lock_with_finally_block(
        self, mock_logger, test_paths
    ):
        """
        Tests that the finally block in _lock_pid_file_and_release_dangling_pid_lock is executed.
        """
        pid = "1234"
        pid_file = os.path.join(
            test_paths["location"], "queue-12345_incremental_output_download.pid"
        )

        # Create mock file object
        mock_file = MagicMock()
        mock_file.read.return_value = pid
        mock_file.fileno.return_value = 5

        with patch("sys.platform", "linux"), patch(
            "builtins.open", return_value=mock_file
        ) as mock_open_file, patch("fcntl.flock") as mock_flock, patch(
            "os.path.getsize", return_value=10
        ) as mock_getsize, patch("os.path.realpath", return_value=pid_file) as mock_realpath, patch(
            "os.remove", side_effect=Exception("Failed to remove file")
        ) as mock_remove:
            # Should raise Exception because of the remove error, but finally block should still execute
            with pytest.raises(Exception):
                _lock_pid_file_and_release_dangling_pid_lock(pid, pid_file)

            # Verify operations
            mock_open_file.assert_called_once_with(pid_file, "r+")
            mock_flock.assert_any_call(mock_file.fileno(), LOCK_EX)  # Exclusive lock
            mock_flock.assert_any_call(mock_file.fileno(), LOCK_UN)  # Unlock
            mock_file.read.assert_called_once()
            mock_remove.assert_called_once_with(pid_file)
            mock_getsize.assert_called_once_with(pid_file)
            mock_realpath.assert_called_once_with(pid_file)

    @pytest.mark.skipif(
        not sys.platform.startswith("linux") and not sys.platform.startswith("darwin"),
        reason="Test only applicable on Linux/macOS",
    )
    def test_lock_pid_file_and_release_dangling_pid_lock_with_str_comparison(
        self, mock_logger, test_paths
    ):
        """
        Tests that the str comparison in _lock_pid_file_and_release_dangling_pid_lock works correctly.
        """
        pid = "1234"
        pid_file = os.path.join(
            test_paths["location"], "queue-12345_incremental_output_download.pid"
        )

        # Create mock file object
        mock_file = MagicMock()
        mock_file.read.return_value = "1234"  # String
        mock_file.fileno.return_value = 5

        with patch("sys.platform", "linux"), patch(
            "builtins.open", return_value=mock_file
        ) as mock_open_file, patch("fcntl.flock") as mock_flock, patch(
            "os.path.getsize", return_value=10
        ) as mock_getsize, patch("os.path.realpath", return_value=pid_file) as mock_realpath, patch(
            "os.remove"
        ) as mock_remove:
            result = _lock_pid_file_and_release_dangling_pid_lock(pid, pid_file)

            # Verify operations
            mock_open_file.assert_called_once_with(pid_file, "r+")
            mock_flock.assert_any_call(mock_file.fileno(), LOCK_EX)  # Exclusive lock
            mock_flock.assert_any_call(mock_file.fileno(), LOCK_UN)  # Unlock
            mock_file.read.assert_called_once()
            mock_remove.assert_called_once_with(pid_file)
            mock_getsize.assert_called_once_with(pid_file)
            mock_realpath.assert_called_once_with(pid_file)
            assert result is True
