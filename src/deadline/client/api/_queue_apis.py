# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
from __future__ import annotations

__all__ = ["_incremental_output_download"]

from .. import api
import datetime
from typing import Optional
from deadline.job_attachments.download import (
    get_output_manifests_by_asset_root,
    merge_asset_manifests,
)
from deadline.job_attachments.api.attachment import _attachment_download_with_root_manifests
import boto3
from deadline.client.api._session import get_default_client_config
from botocore.exceptions import ClientError  # type: ignore[import]
from deadline.job_attachments.models import (
    JobAttachmentS3Settings,
)
from ..exceptions import DeadlineOperationError
from deadline.client.api._session import _get_queue_user_boto3_session
from deadline.job_attachments.asset_manifests.base_manifest import BaseAssetManifest
from deadline.client.cli._groups.click_logger import ClickLogger


@api.record_function_latency_telemetry_event()
def _incremental_output_download(farm_id: str,
                                 queue_id: str,
                                 boto3_session: boto3.Session,
                                 lookback_in_minutes: Optional[int] = 60,
                                 path_mapping_rules: Optional[str] = None,
                                 logger: ClickLogger = ClickLogger(False)
                                 ):
    """
    Downloads incremental output for all completed tasks in the last lookback_in_minutes minutes

    :param farm_id: farm id for the output download
    :param queue_id: queue for scoping output download
    :param lookback_in_minutes: lookback period in minutes for fetching successful tasks
    :param path_mapping_rules: path mapping rules for cross OS path mapping
    :param boto3_session: boto3 session
    :param logger: logger component
    :return:
    """
    # Get the current datetime and add lookback_in_minutes to download delta of tasks completed successfully
    datetime_now = datetime.datetime.utcnow()
    lookback_timedelta = datetime.timedelta(minutes=int(lookback_in_minutes))
    lookback_since_datetime = datetime_now - lookback_timedelta

    logger.echo(f"search all completed tasks since: {lookback_since_datetime}")

    # Deadline Client and get the Queue to download.
    deadline = boto3_session.client("deadline", config=get_default_client_config())

    # List of all successful tasks
    all_successful_tasks = []
    try:
        # Initialize params for pagination
        item_offset: int = 0
        page_size: int = 100
        response = None

        # Get all succeeded tasks using pagination
        while True:
            logger.echo(
                f"Searching for completed tasks in deadline farm: {farm_id} queue: {queue_id} with offset: {item_offset} and page size: {page_size} since last {lookback_in_minutes} minutes"
            )

            # Search tasks that are marked SUCCESSFUL and were updated in the last delta duration x minutes
            # TODO Use UPDATED_AT instead of ENDED_AT to make this search more granular
            response = deadline.search_tasks(
                farmId=farm_id,
                queueIds=[queue_id],
                itemOffset=item_offset,
                pageSize=page_size,
                filterExpressions={
                    "filters": [
                        {
                            "dateTimeFilter": {
                                "name": "ENDED_AT",
                                "dateTime": lookback_since_datetime,
                                "operator": "GREATER_THAN",
                            }
                        },
                        {
                            "groupFilter": {
                                "filters": [
                                    {
                                        "stringFilter": {
                                            "name": "RUN_STATUS",
                                            "operator": "EQUAL",
                                            "value": "SUCCEEDED",
                                        }
                                    }
                                ],
                                "operator": "AND",
                            }
                        },
                    ],
                    "operator": "AND",
                },
            )
            # Append tasks from search response to successful tasks
            all_successful_tasks.extend(response["tasks"])
            if "nextItemOffset" not in response or response["nextItemOffset"] is None:
                # If we found no next item offset this was the last page
                logger.echo(
                    f"Found {len(all_successful_tasks)} successfully completed tasks in the last {lookback_in_minutes} minutes"
                )
                break
            item_offset = response["nextItemOffset"]

    except ClientError as exc:
        raise DeadlineOperationError(f"Failed to get Tasks from Deadline:\n{exc}") from exc

    # Get queue for assuming queue role session to access JA bucket
    queue = deadline.get_queue(farmId=farm_id, queueId=queue_id)
    queue_role_session: boto3.Session = _get_queue_user_boto3_session(
        deadline=deadline,
        base_session=boto3_session,
        farm_id=farm_id,
        queue_id=queue_id,
        queue_display_name=queue["displayName"],
    )
    # Iterate through each successfully completed task and get output paths for each
    for task in all_successful_tasks:
        output_manifests_by_root: Dict[str, List[BaseAssetManifest]] = (
            get_output_manifests_by_asset_root(
                s3_settings=JobAttachmentS3Settings(**queue["jobAttachmentSettings"]),
                farm_id=farm_id,
                queue_id=queue_id,
                job_id=task["jobId"],
                step_id=task["stepId"],
                task_id=task["taskId"],
                session=queue_role_session,
            )
        )

        merged_output_manifests: Dict[str, BaseAssetManifest] = {}
        for root in output_manifests_by_root.keys():
            merged_output_manifest = merge_asset_manifests(output_manifests_by_root[root])
            if merged_output_manifest:
                merged_output_manifests[root] = merged_output_manifest

        # If no output paths were found, log a message and exit.
        if merged_output_manifests == {}:
            logger.echo(
                f"Found no output paths for job {task['jobId']}, step {task['stepId']}, task {task['taskId']}"
            )
            continue

        logger.echo(
            f"Found output paths for job {task['jobId']}, step {task['stepId']}, task {task['taskId']}: {merged_output_manifests}"
        )

        # Download attachments using merged output manifests
        _attachment_download_with_root_manifests(
            manifests_by_root=merged_output_manifests,
            s3_root_uri=JobAttachmentS3Settings(**queue["jobAttachmentSettings"]).to_s3_root_uri(),
            boto3_session=queue_role_session,
            path_mapping_rules=path_mapping_rules,
            logger=logger,
        )
