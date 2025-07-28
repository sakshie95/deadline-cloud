# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Job template utilities for incremental download tests.
Uses local copies of job templates from BealineJobBundles.
"""

import re
import subprocess
from pathlib import Path


def get_job_bundle_path(template_name: str) -> str:
    """
    Get the path to a job bundle template directory.

    Args:
        template_name: Name of the template (e.g., 'make_many_small_files')

    Returns:
        Path to the template directory
    """
    current_dir = Path(__file__).parent
    bundle_path = current_dir / "job_bundles" / template_name

    if not bundle_path.exists():
        raise FileNotFoundError(f"Job bundle template not found: {bundle_path}")

    return str(bundle_path)


def submit_job_bundle(
    farm_id: str,
    queue_id: str,
    template_name: str,
    parameters: dict = None
) -> str:
    """
    Submit a job using a local job bundle template.

    Args:
        farm_id: The farm ID to use
        queue_id: The queue ID to use
        template_name: Name of the template directory
        parameters: Optional parameters to pass to the job

    Returns:
        The job ID of the submitted job
    """
    bundle_path = get_job_bundle_path(template_name)

    # Build the command
    cmd = [
        "deadline", "bundle", "submit",
        "--farm-id", farm_id,
        "--queue-id", queue_id
    ]

    # Add parameters if provided
    if parameters:
        for key, value in parameters.items():
            cmd.extend(["--parameter", f"{key}={value}"])

    # Add the bundle path
    cmd.append(bundle_path)

    result = subprocess.run(cmd, capture_output=True, text=True, check=False)

    if result.returncode != 0:
        raise Exception(f"Failed to submit job: {result.stderr}\nCommand: {' '.join(cmd)}\nOutput: {result.stdout}")

    # Extract job ID from output
    output = result.stdout
    # Look for job ID at the end of the output (format: job-xxxxxxxx)
    match = re.search(r"job-([a-zA-Z0-9]+)", output)
    if not match:
        raise Exception(f"Could not find job ID in output: {output}")

    return match.group(0)  # Return the full job ID including "job-" prefix


def submit_make_many_small_files_job(
    farm_id: str,
    queue_id: str,
    files_per_task: int = 100,
    task_count: int = 100,
    output_dir: str = "output"
) -> str:
    """
    Submit a job that creates many small files.

    Args:
        farm_id: The farm ID to use
        queue_id: The queue ID to use
        files_per_task: Number of files to create per task
        task_count: Number of tasks to run
        output_dir: The output directory to use (defaults to "output")

    Returns:
        The job ID of the submitted job
    """
    parameters = {
        "FilesPerTask": files_per_task,
        "Tasks": f"1-{task_count}",
        "DataDir": output_dir
    }

    return submit_job_bundle(farm_id, queue_id, "make_many_small_files", parameters)


def submit_dep_data_flow_job(
    farm_id: str,
    queue_id: str,
    output_dir: str = None
) -> str:
    """
    Submit a job with branching and merging step dependencies.

    Args:
        farm_id: The farm ID to use
        queue_id: The queue ID to use
        output_dir: The output directory to use (optional, not used - kept for compatibility)

    Returns:
        The job ID of the submitted job
    """
    # Note: We don't override DataDir because the template expects it to point to
    # the existing ./data_dir directory in the bundle which contains the required input files
    parameters = {
        "JobName": "Step-Step Dependency Test",
        # DataDir uses default ./data_dir from template
        # InputDir uses default ./input_dir from template
        "Frames": "8-11"
    }

    return submit_job_bundle(farm_id, queue_id, "dep_data_flow", parameters)


def submit_dep_chain_job(
    farm_id: str,
    queue_id: str
) -> str:
    """
    Submit a job with a chain of step dependencies.

    Args:
        farm_id: The farm ID to use
        queue_id: The queue ID to use

    Returns:
        The job ID of the submitted job
    """
    parameters = {
        "JobName": "Step-Step Chain JA Output Test",
        "OutputPath": "output",
        # JobScriptDir uses default "scripts" from template
    }

    return submit_job_bundle(farm_id, queue_id, "dep_chain", parameters)


def submit_cli_job(
    farm_id: str,
    queue_id: str,
    bash_script: str,
    data_dir: str = None
) -> str:
    """
    Submit a job with a custom bash script.

    Args:
        farm_id: The farm ID to use
        queue_id: The queue ID to use
        bash_script: The bash script to execute
        data_dir: The data directory to use (optional, defaults to "output")

    Returns:
        The job ID of the submitted job
    """
    if data_dir is None:
        data_dir = "output"

    parameters = {
        "DataDir": data_dir,
        "BashScript": bash_script
    }

    return submit_job_bundle(farm_id, queue_id, "cli_job", parameters)
