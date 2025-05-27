# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import os
import json
import pytest
from unittest.mock import patch, MagicMock, mock_open

from deadline.job_attachments.incremental_downloads.orchestrator import (
    IncrementalDownloadsOrchestrator,
)
from deadline.job_attachments.incremental_downloads.models import StateFileModel
from freezegun import freeze_time
from freezegun.api import FakeDatetime


class TestIncrementalDownloadsOrchestrator:
    @pytest.fixture
    def mock_logger(self):
        """
        Fixture to create a mock logger object.
        """
        logger = MagicMock()
        logger.echo = MagicMock()
        return logger

    @pytest.fixture
    def mock_boto3_session(self):
        """
        Fixture to create a mock boto3 session.
        """
        return MagicMock()

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
    def mock_state_file_model(self):
        """
        Fixture to create a mock StateFileModel.
        """
        model = StateFileModel()
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
    @patch("os.path.exists")
    @patch.object(IncrementalDownloadsOrchestrator, "_save_download_progress_to_state_file")
    def test_orchestrate_download_outputs_workflow_bootstrap(
        self,
        mock_save,
        mock_exists,
        mock_logger,
        mock_boto3_session,
        test_paths,
    ):
        """
        Test orchestrate_download_outputs_workflow when bootstrapping is required.
        """
        # Setup
        mock_exists.return_value = False
        bootstrap_lookback_in_minutes: int = 60

        # Execute
        result = IncrementalDownloadsOrchestrator.orchestrate_download_outputs_workflow(
            mock_boto3_session,
            "farm-123",
            mock_logger,
            "path_mapping_rules",
            "queue-123",
            test_paths["location"],
            bootstrap_lookback_in_minutes,
            False,
        )

        # Assert
        assert result is True
        expected_download_progress: StateFileModel = StateFileModel()
        # Frozen current time - 60 minutes of bootstrap_lookback_in_minutes
        expected_download_progress.last_lookback_time = FakeDatetime(2025, 5, 26, 11, 0)

        mock_save.assert_called_once()

        # Validate that the last lookback time got updated to the current time - bootstrap_lookback_in_minutes
        assert (
            mock_save.call_args[0][2].last_lookback_time
            == expected_download_progress.last_lookback_time
        )

    def test_orchestrate_download_outputs_workflow_load_existing(
        self, mock_logger, mock_boto3_session, test_paths, mock_state_file_model
    ):
        """
        Test orchestrate_download_outputs_workflow when loading existing progress file.
        """
        state_dict = mock_state_file_model.to_dict()

        with patch("os.path.exists") as mock_exists, patch(
            "builtins.open", mock_open(read_data=json.dumps(state_dict))
        ), patch.object(
            StateFileModel, "from_dict", return_value=mock_state_file_model
        ) as mock_from_dict, patch.object(
            IncrementalDownloadsOrchestrator, "_save_download_progress_to_state_file"
        ) as mock_save:
            # Setup
            mock_exists.return_value = True
            bootstrap_lookback_in_minutes: int = 60

            # Execute
            result = IncrementalDownloadsOrchestrator.orchestrate_download_outputs_workflow(
                mock_boto3_session,
                "farm-123",
                mock_logger,
                "path_mapping_rules",
                "queue-123",
                test_paths["location"],
                bootstrap_lookback_in_minutes,
                False,
            )

            # Assert
            assert result is True
            mock_from_dict.assert_called_once()
            mock_save.assert_called_once()
            # Validate that the last lookback time got updated to the lookback time from current progress state file
            assert mock_save.call_args[0][2].last_lookback_time == "2023-01-01T00:00:00Z"

    @freeze_time("2025-05-26 12:00:00")
    def test_orchestrate_download_outputs_workflow_force_bootstrap(
        self, mock_logger, mock_boto3_session, test_paths
    ):
        """
        Test orchestrate_download_outputs_workflow when force_bootstrap is True.
        """
        with patch("os.path.exists") as mock_exists, patch.object(
            IncrementalDownloadsOrchestrator, "_save_download_progress_to_state_file"
        ) as mock_save:
            # Setup
            mock_exists.return_value = True  # File exists but we're forcing bootstrap
            bootstrap_lookback_in_minutes: int = 60

            # Execute
            result = IncrementalDownloadsOrchestrator.orchestrate_download_outputs_workflow(
                mock_boto3_session,
                "farm-123",
                mock_logger,
                "path_mapping_rules",
                "queue-123",
                test_paths["location"],
                bootstrap_lookback_in_minutes,
                True,
            )

            # Assert
            assert result is True
            expected_download_progress: StateFileModel = StateFileModel()
            # Frozen current time - 60 minutes of bootstrap_lookback_in_minutes
            expected_download_progress.last_lookback_time = FakeDatetime(2025, 5, 26, 11, 0)

            mock_save.assert_called_once()

            # Validate that the last lookback time got updated to the current time - bootstrap_lookback_in_minutes
            assert (
                mock_save.call_args[0][2].last_lookback_time
                == expected_download_progress.last_lookback_time
            )

    @freeze_time("2025-05-26 12:00:00")
    def test_orchestrate_download_outputs_workflow_force_bootstrap_without_lookback(
        self, mock_logger, mock_boto3_session, test_paths
    ):
        """
        Test orchestrate_download_outputs_workflow when force_bootstrap is True.
        """
        with patch("os.path.exists") as mock_exists, patch.object(
            IncrementalDownloadsOrchestrator, "_save_download_progress_to_state_file"
        ) as mock_save:
            # Setup
            mock_exists.return_value = True  # File exists but we're forcing bootstrap

            # Execute
            result = IncrementalDownloadsOrchestrator.orchestrate_download_outputs_workflow(
                mock_boto3_session,
                "farm-123",
                mock_logger,
                "path_mapping_rules",
                "queue-123",
                test_paths["location"],
                None,
                True,
            )

            # Assert
            assert result is True
            expected_download_progress: StateFileModel = StateFileModel()
            # Frozen current time - 60 minutes of bootstrap_lookback_in_minutes
            expected_download_progress.last_lookback_time = FakeDatetime(2025, 5, 26, 12, 0)

            mock_save.assert_called_once()

            # Validate that last lookback time got updated to the current time - 0 since there's no bootstrap lookback
            assert (
                mock_save.call_args[0][2].last_lookback_time
                == expected_download_progress.last_lookback_time
            )

    @patch("os.path.exists")
    @patch("builtins.open", side_effect=Exception("File read error"))
    @patch.object(IncrementalDownloadsOrchestrator, "_save_download_progress_to_state_file")
    def test_orchestrate_download_outputs_workflow_load_error(
        self,
        mock_save,
        mock_open,
        mock_exists,
        mock_logger,
        mock_boto3_session,
        test_paths,
    ):
        """
        Test orchestrate_download_outputs_workflow when there's an error loading the progress file.
        """
        # Setup
        mock_exists.return_value = True

        result = IncrementalDownloadsOrchestrator.orchestrate_download_outputs_workflow(
            mock_boto3_session,
            "farm-123",
            mock_logger,
            "path_mapping_rules",
            "queue-123",
            test_paths["location"],
            60,
            False,
        )

        # Assert
        assert result is False
        mock_save.assert_not_called()

    @patch("os.makedirs")
    @patch("builtins.open", new_callable=mock_open)
    @patch("json.dump")
    @patch("os.fsync")
    @patch("os.replace")
    def test_save_download_progress_to_state_file(
        self,
        mock_replace,
        mock_fsync,
        mock_json_dump,
        mock_file,
        mock_makedirs,
        mock_logger,
        test_paths,
        mock_state_file_model,
    ):
        """
        Test _save_download_progress_to_state_file successfully saves the state file.
        """
        # Execute
        IncrementalDownloadsOrchestrator._save_download_progress_to_state_file(
            test_paths["location"],
            test_paths["progress_file"],
            mock_state_file_model,
            mock_logger,
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
    def test_save_download_progress_to_state_file_exception(
        self, mock_makedirs, mock_logger, test_paths, mock_state_file_model
    ):
        """
        Test _save_download_progress_to_state_file when an exception occurs.
        """
        # Execute
        IncrementalDownloadsOrchestrator._save_download_progress_to_state_file(
            test_paths["location"],
            test_paths["progress_file"],
            mock_state_file_model,
            mock_logger,
        )

        # Assert
        assert "Failed to save download progress" in mock_logger.echo.call_args_list[0][0][0]
