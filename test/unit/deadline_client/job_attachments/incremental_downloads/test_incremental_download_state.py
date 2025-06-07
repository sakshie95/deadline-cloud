# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import os
import json
import pytest
from unittest.mock import patch, MagicMock, mock_open

from deadline.job_attachments.incremental_downloads.incremental_download_state import (
    IncrementalDownloadState,
    bootstrap_fresh_state,
    load_progress_from_state_file,
    save_progress_to_state_file,
)
from freezegun import freeze_time


class TestIncrementalDownloadState:
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
            "progress_file": os.path.join(location, "download_progress.json"),
        }

    @pytest.fixture
    def mock_state(self):
        """
        Fixture to create a mock IncrementalDownloadState.
        """
        model = IncrementalDownloadState()
        model.last_lookback_time = "2023-01-01T00:00:00Z"
        model.jobs = [
            {
                "jobId": "job-123",
                "sessions": [
                    {
                        "sessionId": "session-123",
                        "sessionLifecycleStatus": "RUNNING",
                        "lastDownloadedSessActionId": 5,
                    }
                ],
            }
        ]
        return model

    @freeze_time("2025-05-26 12:00:00")
    def test_bootstrap_fresh_state(self, mock_logger):
        """
        Test bootstrap_fresh_state with lookback minutes.
        """
        # Setup
        bootstrap_lookback_in_minutes = 60

        # Execute
        result = bootstrap_fresh_state(
            bootstrap_lookback_in_minutes,
            mock_logger.echo,
        )

        # Assert
        assert result.last_lookback_time.isoformat() == "2025-05-26T11:00:00"
        assert result.jobs == []

    @freeze_time("2025-05-26 12:00:00")
    def test_bootstrap_fresh_state_no_lookback(self, mock_logger):
        """
        Test bootstrap_fresh_state without lookback minutes.
        """
        # Execute
        result = bootstrap_fresh_state(
            None,
            mock_logger.echo,
        )

        # Assert
        assert result.last_lookback_time.isoformat() == "2025-05-26T12:00:00"
        assert result.jobs == []

    def test_load_progress_from_state_file(self, mock_logger, mock_state, test_paths):
        """
        Test load_progress_from_state_file successfully loads the state file.
        """
        state_dict = mock_state.to_dict()

        with patch("builtins.open", mock_open(read_data=json.dumps(state_dict))), patch.object(
            IncrementalDownloadState, "from_dict", return_value=mock_state
        ) as mock_from_dict:
            # Execute
            result = load_progress_from_state_file(
                test_paths["progress_file"],
                mock_logger.echo,
            )

            # Assert
            mock_from_dict.assert_called_once()
            assert result == mock_state

    def test_load_progress_from_state_file_exception(self, mock_logger, test_paths):
        """
        Test load_progress_from_state_file when an exception occurs.
        """
        # Setup
        with patch("builtins.open", side_effect=Exception("File read error")):
            # Execute and Assert
            with pytest.raises(Exception) as excinfo:
                load_progress_from_state_file(
                    test_paths["progress_file"],
                    mock_logger.echo,
                )
            assert str(excinfo.value) == "File read error"

    @patch("os.makedirs")
    @patch("builtins.open", new_callable=mock_open)
    @patch("json.dump")
    @patch("os.fsync")
    @patch("os.replace")
    def test_save_progress_to_state_file(
        self,
        mock_replace,
        mock_fsync,
        mock_json_dump,
        mock_file,
        mock_makedirs,
        mock_logger,
        test_paths,
        mock_state,
    ):
        """
        Test save_progress_to_state_file successfully saves the state file.
        """
        # Execute
        save_progress_to_state_file(
            test_paths["location"],
            test_paths["progress_file"],
            mock_state,
            mock_logger.echo,
        )

        # Assert
        mock_makedirs.assert_called_once_with(
            os.path.dirname(test_paths["location"]), exist_ok=True
        )
        mock_file.assert_called_once()
        mock_json_dump.assert_called_once()
        mock_fsync.assert_called_once()
        mock_replace.assert_called_once()

    @patch("os.makedirs", side_effect=Exception("Directory creation error"))
    def test_save_progress_to_state_file_exception(
        self, mock_makedirs, mock_logger, test_paths, mock_state
    ):
        """
        Test save_progress_to_state_file when an exception occurs.
        """
        # Execute
        with pytest.raises(Exception) as excinfo:
            save_progress_to_state_file(
                test_paths["location"],
                test_paths["progress_file"],
                mock_state,
                mock_logger.echo,
            )

        # Assert
        assert str(excinfo.value) == "Directory creation error"

    def test_incremental_download_state_init(self):
        """
        Test IncrementalDownloadState initialization.
        """
        # Test with default values
        state = IncrementalDownloadState()
        assert state.last_lookback_time is None
        assert state.jobs == []

        # Test with provided values
        last_lookback_time = "2023-01-01T00:00:00Z"
        jobs = [{"jobId": "job-123", "sessions": []}]
        state = IncrementalDownloadState(last_lookback_time=last_lookback_time, jobs=jobs)
        assert state.last_lookback_time == last_lookback_time
        assert state.jobs == jobs

    def test_incremental_download_state_from_dict(self):
        """
        Test IncrementalDownloadState.from_dict method.
        """
        # Test with empty dict
        state = IncrementalDownloadState.from_dict({})
        assert state.last_lookback_time is None
        assert state.jobs == []

        # Test with None
        state = IncrementalDownloadState.from_dict(None)
        assert state.last_lookback_time is None
        assert state.jobs == []

        # Test with valid data
        data = {
            "lastLookbackTime": "2023-01-01T00:00:00Z",
            "jobs": [
                {
                    "jobId": "job-123",
                    "sessions": [
                        {
                            "sessionId": "session-123",
                            "sessionLifecycleStatus": "RUNNING",
                            "lastDownloadedSessActionId": 5,
                        }
                    ],
                }
            ],
        }
        state = IncrementalDownloadState.from_dict(data)
        assert state.last_lookback_time == "2023-01-01T00:00:00Z"
        assert len(state.jobs) == 1
        assert state.jobs[0]["jobId"] == "job-123"

    def test_incremental_download_state_to_dict(self):
        """
        Test IncrementalDownloadState.to_dict method.
        """
        # Create a state
        last_lookback_time = "2023-01-01T00:00:00Z"
        jobs = [
            {
                "jobId": "job-123",
                "sessions": [
                    {
                        "sessionId": "session-123",
                        "sessionLifecycleStatus": "RUNNING",
                        "lastDownloadedSessActionId": 5,
                    }
                ],
            }
        ]
        state = IncrementalDownloadState(last_lookback_time=last_lookback_time, jobs=jobs)

        # Convert to dict
        result = state.to_dict()

        # Assert
        assert result == {"lastLookbackTime": last_lookback_time, "jobs": jobs}
