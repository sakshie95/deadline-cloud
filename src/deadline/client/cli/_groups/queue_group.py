# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
All the `deadline queue` commands.
"""

import click
from botocore.exceptions import ClientError  # type: ignore[import]

from ... import api
from ...config import config_file
from ...exceptions import DeadlineOperationError
from .._common import _apply_cli_options_to_config, _cli_object_repr, _handle_error
from deadline.job_attachments.models import (
    JobAttachmentS3Settings,
    FileConflictResolution,
)
from .click_logger import ClickLogger
from deadline.client.api._queue_apis import _incremental_output_download
from configparser import ConfigParser


@click.group(name="queue")
@_handle_error
def cli_queue():
    """
    Commands for queues.
    """


@cli_queue.command(name="list")
@click.option("--profile", help="The AWS profile to use.")
@click.option("--farm-id", help="The farm to use.")
@_handle_error
def queue_list(**args):
    """
    Lists the available queues.
    """
    # Get a temporary config object with the standard options handled
    config = _apply_cli_options_to_config(required_options={"farm_id"}, **args)

    farm_id = config_file.get_setting("defaults.farm_id", config=config)

    try:
        response = api.list_queues(farmId=farm_id, config=config)
    except ClientError as exc:
        raise DeadlineOperationError(f"Failed to get Queues from Deadline:\n{exc}") from exc

    # Select which fields to print and in which order
    structured_queue_list = [
        {field: queue[field] for field in ["queueId", "displayName"]}
        for queue in response["queues"]
    ]

    click.echo(_cli_object_repr(structured_queue_list))


@cli_queue.command(name="paramdefs")
@click.option("--profile", help="The AWS profile to use.")
@click.option("--farm-id", help="The farm to use.")
@click.option("--queue-id", help="The queue to use.")
@_handle_error
def queue_paramdefs(**args):
    """
    Lists a Queue's Parameters Definitions.
    """
    # Get a temporary config object with the standard options handled
    config = _apply_cli_options_to_config(required_options={"farm_id", "queue_id"}, **args)

    farm_id = config_file.get_setting("defaults.farm_id", config=config)
    queue_id = config_file.get_setting("defaults.queue_id", config=config)

    try:
        response = api.get_queue_parameter_definitions(farmId=farm_id, queueId=queue_id)
    except ClientError as exc:
        raise DeadlineOperationError(
            f"Failed to get Queue Parameter Definitions from Deadline:\n{exc}"
        ) from exc

    click.echo(_cli_object_repr(response))


@cli_queue.command(name="get")
@click.option("--profile", help="The AWS profile to use.")
@click.option("--farm-id", help="The farm to use.")
@click.option("--queue-id", help="The queue to use.")
@_handle_error
def queue_get(**args):
    """
    Get the details of a queue.

    If Queue ID is not provided, returns the configured default Queue.
    """
    # Get a temporary config object with the standard options handled
    config = _apply_cli_options_to_config(required_options={"farm_id", "queue_id"}, **args)

    farm_id = config_file.get_setting("defaults.farm_id", config=config)
    queue_id = config_file.get_setting("defaults.queue_id", config=config)

    deadline = api.get_boto3_client("deadline", config=config)
    response = deadline.get_queue(farmId=farm_id, queueId=queue_id)
    response.pop("ResponseMetadata", None)

    click.echo(_cli_object_repr(response))


@cli_queue.command(
    name="incremental_output_download",
    help="BETA - Download Job Output data incrementally for all jobs running on a queue as tasks finish successfully,\n"
         "since a lookback period specified in minutes",
)
@click.option("--farm-id", help="The AWS Deadline Cloud Farm to use. ")
@click.option("--queue-id", help="The AWS Deadline Cloud Queue to use.")
@click.option("--path-mapping-rules", help="Path to a file with the path mapping rules to use.")
@click.option("--json", default=None, is_flag=True, help="Output is printed as JSON for scripting.")
@click.option(
    "--lookback-in-minutes",
    default=60,
    help="Downloads outputs for job-tasks that have been successfully completed since these many duration of minutes.\n"
         "Default value is 60 minutes",
)
@click.option(
    "--conflict-resolution",
    type=click.Choice(
        [
            FileConflictResolution.SKIP.name,
            FileConflictResolution.OVERWRITE.name,
            FileConflictResolution.CREATE_COPY.name,
        ],
        case_sensitive=False,
    ),
    help="How to handle downloads if a file already exists:\n"
         "CREATE_COPY (default): Download the file with a new name, appending '(1)' to the end\n"
         "SKIP: Do not download the file\n"
         "OVERWRITE: Download and replace the existing file",
)
@_handle_error
def incremental_output_download(
    path_mapping_rules: str,
    json: bool,
    lookback_in_minutes: int,
    **args,
):
    logger: ClickLogger = ClickLogger(is_json=json)
    logger.echo("processing " + args.__str__())

    logger.echo("Started incremental download....")

    # Get a temporary config object with the standard options handled
    config: Optional[ConfigParser] = _apply_cli_options_to_config(
        required_options={"farm_id", "queue_id"}, **args
    )

    farm_id = config_file.get_setting("defaults.farm_id", config=config)
    queue_id = config_file.get_setting("defaults.queue_id", config=config)

    boto3_session: boto3.Session = api.get_boto3_session(config=config)

    # Call the incremental output download api
    _incremental_output_download(boto3_session=boto3_session,
                                 farm_id=farm_id,
                                 queue_id=queue_id,
                                 lookback_in_minutes=lookback_in_minutes,
                                 path_mapping_rules=path_mapping_rules,
                                 logger=logger)
