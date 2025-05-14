# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import os
import json
import pytest
from deadline.job_attachments.incremental_downloads._session_action_processor import (
    SessionActionProcessor,
)
from unittest.mock import patch, MagicMock, mock_open

from deadline.job_attachments.incremental_downloads.orchestrator import (
    IncrementalDownloadsOrchestrator,
)
from deadline.job_attachments.incremental_downloads.models import StateFileModel, HydrationState


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

    @pytest.fixture
    def mock_hydration_state(self):
        """
        Fixture to create a mock HydrationState.
        """
        return HydrationState(
            ongoing_jobs={"job-123"},
            session_action_index_map={"session-123": 5},
            session_to_job_map={"session-123": "job-123"},
            session_to_lifecycle_status_map={"session-123": "RUNNING"},
            auxiliary_session_action_status_mapping={},
            session_to_last_finished_action_id_map={"session-123": 5},
        )

    @patch("os.path.exists")
    @patch.object(IncrementalDownloadsOrchestrator, "_download_outputs_from_current_progress")
    @patch.object(IncrementalDownloadsOrchestrator, "_save_download_progress_to_state_file")
    @patch("deadline.client._pid_utils.release_pid_lock")
    def test_orchestrate_download_outputs_workflow_bootstrap(
        self,
        mock_release_lock,
        mock_save,
        mock_download,
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
        mock_download.return_value = StateFileModel()

        # Execute
        result = IncrementalDownloadsOrchestrator.orchestrate_download_outputs_workflow(
            mock_boto3_session,
            "farm-123",
            mock_logger,
            "path_mapping_rules",
            "queue-123",
            test_paths["location"],
            60,
            False,
            "process-123",
        )

        # Assert
        assert result is True
        mock_download.assert_called_once()
        mock_save.assert_called_once()
        mock_release_lock.assert_called_once_with(
            test_paths["location"], "process-123", mock_logger
        )
        assert "Bootstrapping command" in mock_logger.echo.call_args_list[0][0][0]

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
            IncrementalDownloadsOrchestrator, "_download_outputs_from_current_progress"
        ) as mock_download, patch.object(
            IncrementalDownloadsOrchestrator, "_save_download_progress_to_state_file"
        ) as mock_save, patch("deadline.client._pid_utils.release_pid_lock") as mock_release_lock:
            # Setup
            mock_exists.return_value = True
            mock_download.return_value = StateFileModel()

            # Execute
            result = IncrementalDownloadsOrchestrator.orchestrate_download_outputs_workflow(
                mock_boto3_session,
                "farm-123",
                mock_logger,
                "path_mapping_rules",
                "queue-123",
                test_paths["location"],
                60,
                False,
                "process-123",
            )

            # Assert
            assert result is True
            mock_from_dict.assert_called_once()
            mock_download.assert_called_once_with(
                "farm-123",
                "queue-123",
                mock_boto3_session,
                mock_state_file_model,
                "path_mapping_rules",
                mock_logger,
            )
            mock_save.assert_called_once()
            mock_release_lock.assert_called_once_with(
                test_paths["location"], "process-123", mock_logger
            )

    def test_orchestrate_download_outputs_workflow_force_bootstrap(
        self, mock_logger, mock_boto3_session, test_paths
    ):
        """
        Test orchestrate_download_outputs_workflow when force_bootstrap is True.
        """
        with patch("os.path.exists") as mock_exists, patch.object(
            IncrementalDownloadsOrchestrator, "_download_outputs_from_current_progress"
        ) as mock_download, patch.object(
            IncrementalDownloadsOrchestrator, "_save_download_progress_to_state_file"
        ) as mock_save, patch("deadline.client._pid_utils.release_pid_lock") as mock_release_lock:
            # Setup
            mock_exists.return_value = True  # File exists but we're forcing bootstrap
            mock_download.return_value = StateFileModel()

            # Execute
            result = IncrementalDownloadsOrchestrator.orchestrate_download_outputs_workflow(
                mock_boto3_session,
                "farm-123",
                mock_logger,
                "path_mapping_rules",
                "queue-123",
                test_paths["location"],
                60,
                True,
                "process-123",
            )

            # Assert
            assert result is True
            mock_download.assert_called_once()
            mock_save.assert_called_once()
            mock_release_lock.assert_called_once_with(
                test_paths["location"], "process-123", mock_logger
            )
            assert "Bootstrapping command" in mock_logger.echo.call_args_list[0][0][0]

    @patch("os.path.exists")
    @patch("builtins.open", side_effect=Exception("File read error"))
    @patch.object(IncrementalDownloadsOrchestrator, "_download_outputs_from_current_progress")
    @patch.object(IncrementalDownloadsOrchestrator, "_save_download_progress_to_state_file")
    @patch("deadline.client._pid_utils.release_pid_lock")
    def test_orchestrate_download_outputs_workflow_load_error(
        self,
        mock_release_lock,
        mock_save,
        mock_download,
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
        mock_download.return_value = StateFileModel()

        # In the actual code, there's a TODO to throw an exception
        # but it's not implemented yet, so the function continues execution
        result = IncrementalDownloadsOrchestrator.orchestrate_download_outputs_workflow(
            mock_boto3_session,
            "farm-123",
            mock_logger,
            "path_mapping_rules",
            "queue-123",
            test_paths["location"],
            60,
            False,
            "process-123",
        )

        # Assert
        assert result is True
        assert "Failed to load download progress" in mock_logger.echo.call_args_list[0][0][0]
        mock_download.assert_called_once()
        mock_save.assert_called_once()
        mock_release_lock.assert_called_once()

    @patch(
        "deadline.job_attachments.incremental_downloads.state_checkpoint_hydrator.StateCheckpointHydrator.update_download_progress_state"
    )
    @patch.object(SessionActionProcessor, "hydrate_and_process_session_actions")
    @patch(
        "deadline.job_attachments.incremental_downloads._session_processor.SessionProcessor.hydrate_and_process_sessions"
    )
    @patch(
        "deadline.job_attachments.incremental_downloads._job_processor.JobProcessor.hydrate_and_process_jobs"
    )
    @patch(
        "deadline.job_attachments.incremental_downloads.state_checkpoint_hydrator.StateCheckpointHydrator.initialize_in_memory_maps_from_current_progress"
    )
    def test_download_outputs_from_current_progress_empty_jobs(
        self,
        mock_initialize,
        mock_job_processor,
        mock_session_processor,
        mock_action_processor,
        mock_update_state,
        mock_logger,
        mock_boto3_session,
        mock_state_file_model,
    ):
        """
        Test _download_outputs_from_current_progress with empty jobs list.
        """
        # Setup
        mock_state_file_model.jobs = []

        # Create a mock HydrationState for empty jobs
        empty_hydration_state = HydrationState()
        mock_initialize.return_value = empty_hydration_state

        # Setup mocks
        mock_job_processor.return_value = {"job-123"}
        mock_session_processor.return_value = {"session-123"}
        mock_update_state.return_value = [
            {"jobId": "job-123", "sessions": [{"sessionId": "session-123"}]}
        ]

        # Execute
        result = IncrementalDownloadsOrchestrator._download_outputs_from_current_progress(
            "farm-123",
            "queue-123",
            mock_boto3_session,
            mock_state_file_model,
            "path_mapping_rules",
            mock_logger,
        )

        # Assert
        assert isinstance(result, StateFileModel)
        assert len(result.jobs) == 1
        assert result.jobs[0]["jobId"] == "job-123"
        mock_job_processor.assert_called_once_with(
            set(),
            "farm-123",
            "queue-123",
            mock_state_file_model.last_lookback_time,
            mock_logger,
        )
        # Verify the method was called
        mock_action_processor.assert_called_once()

    @patch(
        "deadline.job_attachments.incremental_downloads.state_checkpoint_hydrator.StateCheckpointHydrator.update_download_progress_state"
    )
    @patch.object(SessionActionProcessor, "hydrate_and_process_session_actions")
    @patch(
        "deadline.job_attachments.incremental_downloads._session_processor.SessionProcessor.hydrate_and_process_sessions"
    )
    @patch(
        "deadline.job_attachments.incremental_downloads._job_processor.JobProcessor.hydrate_and_process_jobs"
    )
    @patch(
        "deadline.job_attachments.incremental_downloads.state_checkpoint_hydrator.StateCheckpointHydrator.initialize_in_memory_maps_from_current_progress"
    )
    def test_download_outputs_from_current_progress_with_jobs(
        self,
        mock_initialize,
        mock_job_processor,
        mock_session_processor,
        mock_action_processor,
        mock_update_state,
        mock_logger,
        mock_boto3_session,
        mock_state_file_model,
        mock_hydration_state,
    ):
        """
        Test _download_outputs_from_current_progress with existing jobs.
        """
        # Setup
        mock_initialize.return_value = mock_hydration_state

        # Setup mocks
        mock_job_processor.return_value = {"job-123", "job-456"}
        mock_session_processor.return_value = {"session-123", "session-456"}
        mock_update_state.return_value = [
            {"jobId": "job-123", "sessions": [{"sessionId": "session-123"}]},
            {"jobId": "job-456", "sessions": [{"sessionId": "session-456"}]},
        ]

        # Execute
        result = IncrementalDownloadsOrchestrator._download_outputs_from_current_progress(
            "farm-123",
            "queue-123",
            mock_boto3_session,
            mock_state_file_model,
            "path_mapping_rules",
            mock_logger,
        )

        # Assert
        assert isinstance(result, StateFileModel)
        assert len(result.jobs) == 2
        assert result.jobs[0]["jobId"] == "job-123"
        assert result.jobs[1]["jobId"] == "job-456"
        mock_job_processor.assert_called_once_with(
            mock_hydration_state.ongoing_jobs,
            "farm-123",
            "queue-123",
            mock_state_file_model.last_lookback_time,
            mock_logger,
        )
        mock_session_processor.assert_called_once_with(
            {"job-123", "job-456"},
            "farm-123",
            "queue-123",
            mock_state_file_model.last_lookback_time,
            mock_logger,
        )
        # Verify the method was called
        mock_action_processor.assert_called_once()

    @patch(
        "deadline.job_attachments.incremental_downloads.state_checkpoint_hydrator.StateCheckpointHydrator.initialize_in_memory_maps_from_current_progress"
    )
    @patch(
        "deadline.job_attachments.incremental_downloads._job_processor.JobProcessor.hydrate_and_process_jobs",
        side_effect=Exception("API error"),
    )
    def test_download_outputs_from_current_progress_exception(
        self,
        mock_job_processor,
        mock_initialize,
        mock_logger,
        mock_boto3_session,
        mock_state_file_model,
    ):
        """
        Test _download_outputs_from_current_progress when an exception occurs.
        """
        # Setup
        mock_initialize.return_value = HydrationState()

        # Execute and Assert
        with pytest.raises(Exception) as exc_info:
            IncrementalDownloadsOrchestrator._download_outputs_from_current_progress(
                "farm-123",
                "queue-123",
                mock_boto3_session,
                mock_state_file_model,
                "path_mapping_rules",
                mock_logger,
            )

        assert "API error" in str(exc_info.value)
        assert "Error downloading outputs" in mock_logger.echo.call_args_list[0][0][0]

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
            "process-123",
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
        assert "Successfully saved download progress" in mock_logger.echo.call_args_list[0][0][0]

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
            "process-123",
            mock_logger,
        )

        # Assert
        assert "Failed to save download progress" in mock_logger.echo.call_args_list[0][0][0]
