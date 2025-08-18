# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Integration tests for the incremental download CLI functionality.
"""

import os
import shutil
import subprocess
import time
from pathlib import Path
from typing import Optional, Tuple

import boto3
import pytest

from .job_templates import (
    submit_dep_chain_job,
    submit_dep_data_flow_job,
    submit_make_many_small_files_job,
)
from .test_utils import DeadlineCliTest


class IncrementalDownloadTest:
    """
    Class for Incremental Download Integration Tests.
    """

    def __init__(self, farm_id: str, queue_id: str):
        self.farm_id = farm_id
        self.queue_id = queue_id
        self.deadline_client = boto3.client("deadline")

    def wait_for_job_completion(
        self, job_id: str, timeout: int = 600, poll_interval: int = 5
    ) -> Tuple[bool, str]:
        """Wait for a job to complete. Returns (isSuccess, final_status)."""
        start_time = time.time()
        while time.time() - start_time < timeout:
            job = self.deadline_client.get_job(
                farmId=self.farm_id, queueId=self.queue_id, jobId=job_id
            )

            task_run_status = job.get("taskRunStatus")

            if task_run_status == "SUCCEEDED":
                return True, task_run_status
            elif task_run_status in ["FAILED", "CANCELED"]:
                return False, task_run_status

            time.sleep(poll_interval)

        return False, "TIMEOUT"

    def run_incremental_download_without_storage_profiles(
        self,
        output_dir: str,
        force_bootstrap: bool = False,
        conflict_resolution: Optional[str] = None,
        lookback_window: Optional[int] = None,
        test_name: Optional[str] = None,
    ) -> subprocess.CompletedProcess:
        """Run the incremental download CLI command."""
        # Use test-specific checkpoint directory to avoid conflicts between parallel tests
        checkpoint_suffix = f"_{test_name}" if test_name else ""
        checkpoint_dir = os.path.join(output_dir, f"checkpoints{checkpoint_suffix}")
        os.makedirs(checkpoint_dir, exist_ok=True)

        cmd = [
            "deadline",
            "queue",
            "incremental-output-download",
            "--farm-id",
            self.farm_id,
            "--queue-id",
            self.queue_id,
            "--checkpoint-dir",
            checkpoint_dir,
            "--ignore-storage-profiles",
        ]

        if force_bootstrap:
            cmd.append("--force-bootstrap")

        if conflict_resolution:
            cmd.extend(["--conflict-resolution", conflict_resolution])

        if lookback_window is not None:
            cmd.extend(["--bootstrap-lookback-minutes", str(lookback_window)])

        env = os.environ.copy()
        env["ENABLE_INCREMENTAL_OUTPUT_DOWNLOAD"] = "1"

        # Run from workspace directory so files are downloaded to their manifest paths
        # The CLI will create the necessary directory structure based on job manifests
        return subprocess.run(
            cmd, capture_output=True, text=True, check=False, env=env, cwd=Path.cwd()
        )


@pytest.fixture(scope="session")
def incremental_download_test(deadline_cli_test: DeadlineCliTest):
    """Fixture to get the IncrementalDownloadTest object."""
    return IncrementalDownloadTest(
        farm_id=deadline_cli_test.farm_id, queue_id=deadline_cli_test.queue_id
    )


@pytest.fixture(scope="session", autouse=True)
def cleanup_test_files():
    """Post-session fixture to cleanup any dangling files from incremental download tests."""
    yield  # Run all tests first

    # Cleanup patterns from test analysis
    workspace = Path.cwd()

    # Clean up directories matching test patterns
    patterns = [
        "many_small_files_*",
        "data_dir_*",
        "input_dir_*",
        "dep_chain_*",
        "output_*",
        "requeue_*",
    ]

    for pattern in patterns:
        for path in workspace.glob(pattern):
            if path.is_dir():
                shutil.rmtree(path, ignore_errors=True)


@pytest.mark.integ
@pytest.mark.timeout(900)  # 15 minutes timeout
def test_incremental_download_many_small_files(incremental_download_test):
    """Test incremental download with many small files (10,000 files total)."""

    files_per_task = 100
    task_count = 100
    total_files = files_per_task * task_count  # 10,000 files

    # Use the isolated unique output dir for this test
    unique_output_dir = f"many_small_files_{int(time.time())}"

    print(
        f"[make_many_small_files] Submitting job with {total_files} files ({task_count} tasks × {files_per_task} files)"
    )
    job_id = submit_make_many_small_files_job(
        farm_id=incremental_download_test.farm_id,
        queue_id=incremental_download_test.queue_id,
        files_per_task=files_per_task,
        task_count=task_count,
        output_dir=unique_output_dir,
    )
    print(f"[make_many_small_files] Job submitted with ID: {job_id}")

    try:
        # Run incremental download in a loop until job completes
        job_complete = False
        incremental_download_iteration_number = 0
        while not job_complete:
            incremental_download_iteration_number += 1

            job = incremental_download_test.deadline_client.get_job(
                farmId=incremental_download_test.farm_id,
                queueId=incremental_download_test.queue_id,
                jobId=job_id,
            )

            task_run_status = job.get("taskRunStatus")

            # Check if job failed
            if task_run_status == "FAILED":
                print(f"[make_many_small_files] Job {job_id} FAILED - stopping test")
                assert False, f"Job {job_id} failed with status: {task_run_status}"

            incremental_download_test.run_incremental_download_without_storage_profiles(
                unique_output_dir,
                force_bootstrap=(incremental_download_iteration_number == 1),
                lookback_window=2,
                test_name="many_small_files",
            )

            job_complete = task_run_status in ["SUCCEEDED", "FAILED", "CANCELED"]
            # Wait 5 secs if job's still not complete
            if not job_complete:
                time.sleep(5)

        print(
            f"[make_many_small_files] Job completed after {incremental_download_iteration_number} download iterations"
        )

        # Run final incremental download to ensure all files are captured
        time.sleep(5)
        final_result = incremental_download_test.run_incremental_download_without_storage_profiles(
            unique_output_dir, test_name="make_many_small_files"
        )
        assert final_result.returncode == 0, (
            f"Final incremental download failed: {final_result.stderr}"
        )

        # Find all downloaded files
        downloaded_files = []

        # Check workspace directory with unique output dir name
        workspace_files = list(Path.cwd().glob(f"{unique_output_dir}/**/*.txt"))
        downloaded_files.extend(workspace_files)

        # Remove duplicates
        downloaded_files = list(set(downloaded_files))

        # Validate exact file count, 10,000
        if len(downloaded_files) != total_files:
            print(
                f"[make_many_small_files] ERROR: Expected {total_files} files, found {len(downloaded_files)}"
            )
            assert False, f"Expected exactly {total_files} files, but found {len(downloaded_files)}"

    finally:
        # Clean up test-specific output files after validation (always runs, even if test fails)
        print(f"[make_many_small_files] Cleaning up {unique_output_dir}...")
        output_path = Path.cwd() / unique_output_dir
        if output_path.exists():
            shutil.rmtree(output_path, ignore_errors=True)


@pytest.mark.integ
@pytest.mark.timeout(900)  # 15 minutes timeout
def test_incremental_download_dep_data_flow(incremental_download_test):
    """Test incremental download with dep_data_flow template."""

    # Create a unique data_dir in the workspace to avoid interference between tests
    unique_data_dir = f"data_dir_{int(time.time())}"
    unique_data_path = Path.cwd() / unique_data_dir
    unique_data_path.mkdir(parents=True, exist_ok=True)

    # Create the required input files for the job template
    # The template expects exactly 2 files in both data_dir and input_dir initially
    create_job_input_content = "1. Input to CreateJob from data_dir\n"

    # Create 2 files in the unique data directory (template expects exactly 2)
    (unique_data_path / "create_job_in.txt").write_text(create_job_input_content)
    (unique_data_path / "initial_data.txt").write_text("Initial data file for job\n")

    # Also create input_dir with 2 required files (template expects exactly 2)
    unique_input_dir_name = f"input_dir_{int(time.time())}"
    unique_input_dir = unique_data_path.parent / unique_input_dir_name
    unique_input_dir.mkdir(parents=True, exist_ok=True)
    (unique_input_dir / "create_job_in.txt").write_text(create_job_input_content)
    (unique_input_dir / "initial_input.txt").write_text("Initial input file for job\n")

    try:
        job_id = submit_dep_data_flow_job(
            farm_id=incremental_download_test.farm_id,
            queue_id=incremental_download_test.queue_id,
            data_dir=f"./{unique_data_dir}",
            input_dir=f"./{unique_input_dir_name}",
        )
        print(f"[dep_data_flow] Job submitted with ID: {job_id}")

        # Run incremental download in a loop until job completes
        job_complete = False
        incremental_download_iteration_number = 0
        force_bootstrap_first = True  # Force bootstrap on first run only
        while not job_complete:
            incremental_download_iteration_number += 1
            print(
                f"[dep_data_flow] Running incremental download iteration {incremental_download_iteration_number}..."
            )

            job = incremental_download_test.deadline_client.get_job(
                farmId=incremental_download_test.farm_id,
                queueId=incremental_download_test.queue_id,
                jobId=job_id,
            )

            task_run_status = job.get("taskRunStatus")

            # Check if job failed
            if task_run_status == "FAILED":
                print(f"[dep_data_flow] Job {job_id} FAILED - stopping test")
                assert False, f"Job {job_id} failed with status: {task_run_status}"

            incremental_download_test.run_incremental_download_without_storage_profiles(
                unique_data_dir,
                force_bootstrap=force_bootstrap_first,
                lookback_window=2,
                test_name="dep_data_flow",
            )
            force_bootstrap_first = False  # Only bootstrap on first iteration

            job_complete = task_run_status in ["SUCCEEDED", "FAILED", "CANCELED"]

            # Wait 5 secs if job's still not complete
            if not job_complete:
                time.sleep(5)

        print(
            f"[dep_data_flow] Job completed after {incremental_download_iteration_number} download iterations"
        )

        # Run final incremental download to ensure all files are captured
        time.sleep(5)
        final_result = incremental_download_test.run_incremental_download_without_storage_profiles(
            unique_data_dir, test_name="dep_data_flow"
        )  # Reduced from 1200
        assert final_result.returncode == 0, (
            f"Final incremental download failed: {final_result.stderr}"
        )

        # Verify expected output files from dep_data_flow template
        # Based on template: Step1 + Step1-2 (frames 8-11) + Step1-2-3 + Step1-2-4 + Step1-2-34-5 = 8 files total
        expected_files = {
            "Step1.out": "2. Processed in Step1",
            "Step1-2.8.out": "3.8 Processed in Step1-2.8",
            "Step1-2.9.out": "3.9 Processed in Step1-2.9",
            "Step1-2.10.out": "3.10 Processed in Step1-2.10",
            "Step1-2.11.out": "3.11 Processed in Step1-2.11",
            "Step1-2-3.out": "4. Processed in Step1-2-3",
            "Step1-2-4.out": "4. Processed in Step1-2-4",
            "Step1-2-34-5.out": "5. Processed in Step1-2-34-5",
        }

        # Check unique data dir explicitly (these might be output files)
        data_files = list(unique_data_path.glob("**/*.out"))

        # Remove duplicates by file path (not just name) to avoid counting the same file twice
        unique_file_paths = list(set(data_files))
        downloaded_files = list(unique_file_paths)

        verified_files = []

        for downloaded_file in downloaded_files:
            filename = downloaded_file.name
            if filename in expected_files:
                expected_marker = expected_files[filename]
                content = downloaded_file.read_text()
                # Verify file contains the original input
                if "1. Input to CreateJob from data_dir" in content and expected_marker in content:
                    verified_files.append(filename)
                else:
                    print(f"[dep_data_flow] WARNING: {filename} missing expected content")

        # Verify all 8 expected files were created and have correct content
        assert len(verified_files) == 8, (
            f"Expected exactly 8 verified output files, found {len(verified_files)}: {verified_files}"
        )

        # Verify we have the complete set of expected files
        missing_files = set(expected_files.keys()) - set(verified_files)
        assert not missing_files, f"Missing expected files: {missing_files}"

        print(
            f"[dep_data_flow] Successfully verified all {len(verified_files)} expected output files with correct content"
        )

    finally:
        # Clean up test-specific output files after validation (always runs, even if test fails)
        print(f"[dep_data_flow] Cleaning up unique data dir: {unique_data_path}")
        if unique_data_path.exists():
            shutil.rmtree(unique_data_path, ignore_errors=True)

        # Clean up the unique input directory
        if unique_input_dir.exists():
            shutil.rmtree(unique_input_dir, ignore_errors=True)


@pytest.mark.integ
@pytest.mark.timeout(900)  # 15 minutes timeout
def test_incremental_download_dep_chain(incremental_download_test):
    """Test incremental download with dep_chain template."""

    # Use unique output dir to avoid conflicts between parallel tests
    unique_output_dir = f"dep_chain_{int(time.time())}"

    job_id = submit_dep_chain_job(
        farm_id=incremental_download_test.farm_id,
        queue_id=incremental_download_test.queue_id,
        output_dir=unique_output_dir,
    )

    try:
        # Run incremental download in a loop until job completes
        job_complete = False
        force_bootstrap = True
        while not job_complete:
            job = incremental_download_test.deadline_client.get_job(
                farmId=incremental_download_test.farm_id,
                queueId=incremental_download_test.queue_id,
                jobId=job_id,
            )

            task_run_status = job.get("taskRunStatus")

            # Check if job failed
            if task_run_status == "FAILED":
                assert False, f"Job {job_id} failed with status: {task_run_status}"

            incremental_download_test.run_incremental_download_without_storage_profiles(
                unique_output_dir,
                force_bootstrap=force_bootstrap,
                lookback_window=2,
                test_name="dep_chain",
            )
            force_bootstrap = False

            job_complete = task_run_status in ["SUCCEEDED", "FAILED", "CANCELED"]
            if not job_complete:
                time.sleep(5)

        # Run final incremental download to ensure all files are captured
        time.sleep(5)
        final_result = incremental_download_test.run_incremental_download_without_storage_profiles(
            unique_output_dir, test_name="dep_chain"
        )
        assert final_result.returncode == 0, (
            f"Final incremental download failed: {final_result.stderr}"
        )

        # Verify expected output files from dep_chain template
        expected_files = {}
        for i in range(6):  # A through F = 6 files
            step_name = chr(ord("A") + i)
            filename = f"{step_name}.txt"
            expected_content = f"Step {step_name} is correct"
            expected_files[filename] = expected_content

        # Check workspace directory where CLI downloads files
        downloaded_files = list(Path.cwd().glob("**/*.txt"))

        verified_files = []

        for downloaded_file in downloaded_files:
            filename = downloaded_file.name
            if filename in expected_files:
                expected_content = expected_files[filename]
                content = downloaded_file.read_text().strip()
                if content == expected_content:
                    verified_files.append(filename)

        # Verify all 6 expected files were created with correct content
        assert len(verified_files) == 6, (
            f"Expected exactly 6 verified chain files (A.txt-F.txt), found {len(verified_files)}: {sorted(verified_files)}"
        )

        # Verify we have the complete chain
        expected_filenames = set(expected_files.keys())
        verified_filenames = set(verified_files)
        missing_files = expected_filenames - verified_filenames
        assert not missing_files, f"Missing expected chain files: {sorted(missing_files)}"

    finally:
        # Clean up test-specific output files
        output_path = Path.cwd() / unique_output_dir
        if output_path.exists():
            shutil.rmtree(output_path, ignore_errors=True)


@pytest.mark.integ
@pytest.mark.timeout(900)  # 15 minutes timeout
@pytest.mark.xfail(reason="Known bug with create_copy conflict resolution causes this to fail")
@pytest.mark.parametrize("requeue_level", ["job", "step", "task"])
def test_requeue_with_conflict_resolution(incremental_download_test, requeue_level):
    """Test incremental download with re-queuing at different levels and conflict resolution."""

    files_per_task = 10
    task_count = 2
    expected_initial_files = files_per_task * task_count

    timestamp = int(time.time() * 1000000)
    test_dir = Path.cwd() / f"requeue_{requeue_level}_{timestamp}"
    unique_output_dir = f"output_{requeue_level}_{timestamp}"

    test_dir.mkdir(parents=True, exist_ok=True)

    try:
        # Submit and wait for initial job
        job_id = submit_make_many_small_files_job(
            farm_id=incremental_download_test.farm_id,
            queue_id=incremental_download_test.queue_id,
            files_per_task=files_per_task,
            task_count=task_count,
            output_dir=unique_output_dir,
        )

        job_completed, final_status = incremental_download_test.wait_for_job_completion(
            job_id, timeout=600
        )
        assert job_completed, f"Initial job failed with status: {final_status}"

        # Download initial files
        _run_download_until_complete(
            incremental_download_test,
            test_dir,
            unique_output_dir,
            expected_initial_files,
            requeue_level,
            "initial",
        )

        # Requeue at specified level
        _requeue_at_level(incremental_download_test, job_id, requeue_level)

        # Wait for requeue completion
        requeue_completed, requeue_status = incremental_download_test.wait_for_job_completion(
            job_id, timeout=600
        )
        assert requeue_completed, f"Requeue failed with status: {requeue_status}"

        # Download files after requeue
        expected_final = _get_expected_file_count(
            requeue_level, expected_initial_files, files_per_task
        )
        _run_download_until_complete(
            incremental_download_test,
            test_dir,
            unique_output_dir,
            expected_final,
            requeue_level,
            "requeue",
        )

    finally:
        # Cleanup
        for path in [test_dir, Path.cwd() / unique_output_dir]:
            if path.exists():
                shutil.rmtree(path, ignore_errors=True)


def _run_download_until_complete(test_instance, test_dir, output_dir, expected_count, level, phase):
    """Run incremental download until expected file count is reached."""
    for iteration in range(1, 11):  # Max 10 iterations
        result = test_instance.run_incremental_download_without_storage_profiles(
            str(test_dir),
            force_bootstrap=(phase == "initial" and iteration == 1),
            conflict_resolution="create_copy",
            lookback_window=1,
            test_name=f"requeue_{level}",
        )

        if result.returncode != 0 and "had incorrect size 0" not in result.stdout:
            assert False, f"{phase.title()} download failed: {result.stderr}"

        files = list(Path.cwd().glob(f"{output_dir}/**/file_*"))
        if len(files) >= expected_count:
            break
        time.sleep(2)

    assert len(files) == expected_count, (
        f"Expected {expected_count} files after {phase}, got {len(files)}"
    )


def _requeue_at_level(test_instance, job_id, level):
    """Requeue job at the specified level."""
    client = test_instance.deadline_client
    farm_id = test_instance.farm_id
    queue_id = test_instance.queue_id

    if level == "job":
        client.update_job(
            farmId=farm_id, queueId=queue_id, jobId=job_id, targetTaskRunStatus="READY"
        )
    else:
        steps = client.list_steps(farmId=farm_id, queueId=queue_id, jobId=job_id)["steps"]
        assert steps, "No steps found in job"
        step_id = steps[0]["stepId"]

        if level == "step":
            client.update_step(
                farmId=farm_id,
                queueId=queue_id,
                jobId=job_id,
                stepId=step_id,
                targetTaskRunStatus="READY",
            )
        else:  # task
            tasks = client.list_tasks(
                farmId=farm_id, queueId=queue_id, jobId=job_id, stepId=step_id
            )["tasks"]
            assert tasks, "No tasks found in step"
            task_id = tasks[0]["taskId"]
            client.update_task(
                farmId=farm_id,
                queueId=queue_id,
                jobId=job_id,
                stepId=step_id,
                taskId=task_id,
                targetRunStatus="READY",
            )


def _get_expected_file_count(level, initial_count, files_per_task):
    """Calculate expected file count after requeue based on level."""
    return initial_count * 2 if level in ["job", "step"] else initial_count + files_per_task
