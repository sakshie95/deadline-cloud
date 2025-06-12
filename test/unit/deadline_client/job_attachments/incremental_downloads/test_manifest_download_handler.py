# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from typing import List
from unittest.mock import MagicMock, patch
import pytest

from deadline.job_attachments.incremental_downloads.manifest_download_handler import (
    aggregate_manifest_and_download_outputs,
)
from deadline.job_attachments.incremental_downloads.session_action_processor import (
    SessionActionMapping,
)
from deadline.job_attachments.models import FileConflictResolution
from deadline.job_attachments.asset_manifests.base_manifest import BaseAssetManifest


class TestManifestDownloadHandler:
    @pytest.fixture
    def mock_boto3_session(self):
        mock_session = MagicMock()
        mock_deadline_client = MagicMock()
        mock_session.client.return_value = mock_deadline_client

        # Mock the result of get_queue
        mock_deadline_client.get_queue.return_value = {
            "displayName": "test-queue",
            "jobAttachmentSettings": {"s3BucketName": "test-bucket", "rootPrefix": "test-prefix"},
        }

        return mock_session

    @pytest.fixture
    def mock_queue_role_session(self):
        return MagicMock()

    @pytest.fixture
    def session_action_mappings(self):
        return [
            SessionActionMapping(
                job_id="job-123",
                step_id="step-123",
                task_id="task-123",
                session_action_id="session-action-123",
            ),
            SessionActionMapping(
                job_id="job-123",
                step_id="step-456",
                task_id="task-456",
                session_action_id="session-action-456",
            ),
            SessionActionMapping(
                job_id="job-789",
                step_id="step-789",
                task_id="task-789",
                session_action_id="session-action-789",
            ),
        ]

    @patch(
        "deadline.job_attachments.incremental_downloads.manifest_download_handler._get_queue_user_boto3_session"
    )
    @patch(
        "deadline.job_attachments.incremental_downloads.manifest_download_handler.get_output_manifests_by_asset_root"
    )
    @patch(
        "deadline.job_attachments.incremental_downloads.manifest_download_handler.merge_asset_manifests"
    )
    @patch(
        "deadline.job_attachments.incremental_downloads.manifest_download_handler._attachment_download_with_root_manifests"
    )
    def test_aggregate_manifest_and_download_outputs_success(
        self,
        mock_attachment_download: MagicMock,
        mock_merge_asset_manifests: MagicMock,
        mock_get_output_manifests: MagicMock,
        mock_get_queue_user_session: MagicMock,
        mock_boto3_session: MagicMock,
        mock_queue_role_session: MagicMock,
        session_action_mappings: List[SessionActionMapping],
    ):
        # Setup mocks
        mock_get_queue_user_session.return_value = mock_queue_role_session

        # Mock output manifests for each session action
        mock_output_manifests = {
            "/root1": [MagicMock(spec=BaseAssetManifest)],
            "/root2": [MagicMock(spec=BaseAssetManifest)],
        }

        # Configure mock_get_output_manifests to return the same mock_output_manifests for each call
        mock_get_output_manifests.return_value = mock_output_manifests

        # Mock merged manifests - create a list of mocks for each call
        mock_merged_manifest1 = MagicMock(spec=BaseAssetManifest)
        mock_merged_manifest2 = MagicMock(spec=BaseAssetManifest)

        # Set up mock_merge_asset_manifests to return different values for different calls
        # We need to return a value for each root in each session action
        mock_merge_asset_manifests.side_effect = [
            mock_merged_manifest1,  # First call for /root1
            mock_merged_manifest2,  # First call for /root2
            mock_merged_manifest1,  # Second call for /root1
            mock_merged_manifest2,  # Second call for /root2
            mock_merged_manifest1,  # Third call for /root1
            mock_merged_manifest2,  # Third call for /root2
        ]

        # Call the function under test
        result = aggregate_manifest_and_download_outputs(
            boto3_session=mock_boto3_session,
            session_action_mappings=session_action_mappings,
            farm_id="farm-123",
            queue_id="queue-123",
            file_conflict_resolution=FileConflictResolution.OVERWRITE,
            path_mapping_rules=None,
            print_function_callback=lambda msg: None,
        )

        # Verify the results
        assert result == ["session-action-123", "session-action-456", "session-action-789"]

        # Verify get_queue was called correctly
        mock_boto3_session.client.return_value.get_queue.assert_called_once_with(
            farmId="farm-123", queueId="queue-123"
        )

        # Verify get_queue_user_boto3_session was called correctly
        mock_get_queue_user_session.assert_called_once_with(
            deadline=mock_boto3_session.client.return_value,
            base_session=mock_boto3_session,
            farm_id="farm-123",
            queue_id="queue-123",
            queue_display_name="test-queue",
        )

        # Verify get_output_manifests_by_asset_root was called for each session action
        assert mock_get_output_manifests.call_count == 3

        # Verify merge_asset_manifests was called for each root in each session action
        assert mock_merge_asset_manifests.call_count == 6  # 3 session actions * 2 roots

        # Verify _attachment_download_with_root_manifests was called for each session action
        assert mock_attachment_download.call_count == 3

    @patch(
        "deadline.job_attachments.incremental_downloads.manifest_download_handler._get_queue_user_boto3_session"
    )
    @patch(
        "deadline.job_attachments.incremental_downloads.manifest_download_handler.get_output_manifests_by_asset_root"
    )
    @patch(
        "deadline.job_attachments.incremental_downloads.manifest_download_handler.merge_asset_manifests"
    )
    @patch(
        "deadline.job_attachments.incremental_downloads.manifest_download_handler._attachment_download_with_root_manifests"
    )
    def test_aggregate_manifest_no_output_paths(
        self,
        mock_attachment_download: MagicMock,
        mock_merge_asset_manifests: MagicMock,
        mock_get_output_manifests: MagicMock,
        mock_get_queue_user_session: MagicMock,
        mock_boto3_session: MagicMock,
        mock_queue_role_session: MagicMock,
        session_action_mappings: List[SessionActionMapping],
    ):
        # Setup mocks
        mock_get_queue_user_session.return_value = mock_queue_role_session

        # Mock empty output manifests
        mock_get_output_manifests.return_value = {}

        # Mock merged manifests to return None (no manifests to merge)
        mock_merge_asset_manifests.return_value = None

        # Call the function under test
        result = aggregate_manifest_and_download_outputs(
            boto3_session=mock_boto3_session,
            session_action_mappings=session_action_mappings,
            farm_id="farm-123",
            queue_id="queue-123",
            file_conflict_resolution=FileConflictResolution.OVERWRITE,
            path_mapping_rules=None,
            print_function_callback=lambda msg: None,
        )

        # Verify the results - no session actions should be downloaded
        assert result == []

        # Verify get_output_manifests_by_asset_root was called for each session action
        assert mock_get_output_manifests.call_count == 3

        # Verify _attachment_download_with_root_manifests was not called
        mock_attachment_download.assert_not_called()

    @patch(
        "deadline.job_attachments.incremental_downloads.manifest_download_handler._get_queue_user_boto3_session"
    )
    @patch(
        "deadline.job_attachments.incremental_downloads.manifest_download_handler.get_output_manifests_by_asset_root"
    )
    @patch(
        "deadline.job_attachments.incremental_downloads.manifest_download_handler.merge_asset_manifests"
    )
    @patch(
        "deadline.job_attachments.incremental_downloads.manifest_download_handler._attachment_download_with_root_manifests"
    )
    def test_aggregate_manifest_with_path_mapping(
        self,
        mock_attachment_download: MagicMock,
        mock_merge_asset_manifests: MagicMock,
        mock_get_output_manifests: MagicMock,
        mock_get_queue_user_session: MagicMock,
        mock_boto3_session: MagicMock,
        mock_queue_role_session: MagicMock,
        session_action_mappings: List[SessionActionMapping],
    ):
        # Setup mocks
        mock_get_queue_user_session.return_value = mock_queue_role_session

        # Mock output manifests
        mock_output_manifests = {"/root1": [MagicMock(spec=BaseAssetManifest)]}
        mock_get_output_manifests.return_value = mock_output_manifests

        # Mock merged manifests
        mock_merged_manifest = MagicMock(spec=BaseAssetManifest)
        mock_merge_asset_manifests.return_value = mock_merged_manifest

        # Path mapping rules
        path_mapping_rules = "/source:/destination"

        # Call the function under test
        result = aggregate_manifest_and_download_outputs(
            boto3_session=mock_boto3_session,
            session_action_mappings=session_action_mappings,
            farm_id="farm-123",
            queue_id="queue-123",
            file_conflict_resolution=FileConflictResolution.OVERWRITE,
            path_mapping_rules=path_mapping_rules,
            print_function_callback=lambda msg: None,
        )

        # Verify the results
        assert result == ["session-action-123", "session-action-456", "session-action-789"]

        # Verify _attachment_download_with_root_manifests was called with path_mapping_rules
        for call_args in mock_attachment_download.call_args_list:
            assert call_args[1]["path_mapping_rules"] == path_mapping_rules

    @patch(
        "deadline.job_attachments.incremental_downloads.manifest_download_handler._get_queue_user_boto3_session"
    )
    @patch(
        "deadline.job_attachments.incremental_downloads.manifest_download_handler.get_output_manifests_by_asset_root"
    )
    @patch(
        "deadline.job_attachments.incremental_downloads.manifest_download_handler.merge_asset_manifests"
    )
    @patch(
        "deadline.job_attachments.incremental_downloads.manifest_download_handler._attachment_download_with_root_manifests"
    )
    def test_aggregate_manifest_with_print_callback(
        self,
        mock_attachment_download: MagicMock,
        mock_merge_asset_manifests: MagicMock,
        mock_get_output_manifests: MagicMock,
        mock_get_queue_user_session: MagicMock,
        mock_boto3_session: MagicMock,
        mock_queue_role_session: MagicMock,
        session_action_mappings: List[SessionActionMapping],
    ):
        # Setup mocks
        mock_get_queue_user_session.return_value = mock_queue_role_session

        # Mock output manifests
        mock_output_manifests = {"/root1": [MagicMock(spec=BaseAssetManifest)]}
        mock_get_output_manifests.return_value = mock_output_manifests

        # Mock merged manifests
        mock_merged_manifest = MagicMock(spec=BaseAssetManifest)
        mock_merge_asset_manifests.return_value = mock_merged_manifest

        # Create a mock print callback
        mock_print_callback = MagicMock()

        # Call the function under test
        result = aggregate_manifest_and_download_outputs(
            boto3_session=mock_boto3_session,
            session_action_mappings=session_action_mappings,
            farm_id="farm-123",
            queue_id="queue-123",
            file_conflict_resolution=FileConflictResolution.OVERWRITE,
            path_mapping_rules=None,
            print_function_callback=mock_print_callback,
        )

        # Verify the results
        assert result == ["session-action-123", "session-action-456", "session-action-789"]

        # Verify print callback was called
        assert (
            mock_print_callback.call_count >= 6
        )  # At least once per session action for processing and output paths

        # Verify specific messages were logged
        mock_print_callback.assert_any_call("Processing job job-123 with 2 session actions")
        mock_print_callback.assert_any_call("Processing job job-789 with 1 session actions")

    @patch(
        "deadline.job_attachments.incremental_downloads.manifest_download_handler._get_queue_user_boto3_session"
    )
    @patch(
        "deadline.job_attachments.incremental_downloads.manifest_download_handler.get_output_manifests_by_asset_root"
    )
    @patch(
        "deadline.job_attachments.incremental_downloads.manifest_download_handler.merge_asset_manifests"
    )
    @patch(
        "deadline.job_attachments.incremental_downloads.manifest_download_handler._attachment_download_with_root_manifests"
    )
    def test_aggregate_manifest_with_different_conflict_resolutions(
        self,
        mock_attachment_download: MagicMock,
        mock_merge_asset_manifests: MagicMock,
        mock_get_output_manifests: MagicMock,
        mock_get_queue_user_session: MagicMock,
        mock_boto3_session: MagicMock,
        mock_queue_role_session: MagicMock,
        session_action_mappings: List[SessionActionMapping],
    ):
        # Setup mocks
        mock_get_queue_user_session.return_value = mock_queue_role_session

        # Mock output manifests
        mock_output_manifests = {"/root1": [MagicMock(spec=BaseAssetManifest)]}
        mock_get_output_manifests.return_value = mock_output_manifests

        # Mock merged manifests
        mock_merged_manifest = MagicMock(spec=BaseAssetManifest)
        mock_merge_asset_manifests.return_value = mock_merged_manifest

        # Test with different conflict resolutions
        for resolution in [FileConflictResolution.OVERWRITE, FileConflictResolution.SKIP]:
            mock_attachment_download.reset_mock()

            # Call the function under test
            result = aggregate_manifest_and_download_outputs(
                boto3_session=mock_boto3_session,
                session_action_mappings=session_action_mappings[
                    :1
                ],  # Just use one mapping for this test
                farm_id="farm-123",
                queue_id="queue-123",
                file_conflict_resolution=resolution,
                path_mapping_rules=None,
                print_function_callback=lambda msg: None,
            )

            # Verify the results
            assert result == ["session-action-123"]

            # Verify _attachment_download_with_root_manifests was called with the correct conflict resolution
            mock_attachment_download.assert_called_once()
            assert mock_attachment_download.call_args[1]["conflict_resolution"] == resolution
