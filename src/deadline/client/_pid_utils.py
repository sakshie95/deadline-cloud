# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import os
import psutil
from typing import Callable


def check_and_obtain_pid_lock_if_available(
    pid_file_full_path: str, print_function_callback: Callable[[str], None]
) -> bool:
    """
    Checks if the specified pid lock file exists and if it does, it checks if the process is still running.
    If the process is still running, it raises an exception.
    If the process is not running, it deletes the pid lock file.
    If the pid lock file does not exist, it creates a new one and acquires lock for this pid.
    :param pid_file_full_path: full path of the pid lock file
    :param print_function_callback: Callback to print messages produced in this function.
                Used in the CLI to print to stdout using click.echo. By default, ignores messages.
    :return: boolean, True if pid lock was obtained successfully, throws an exception otherwise
    """
    print_function_callback(f"Checking if another download is in progress at {pid_file_full_path}")

    # Get the current process's id to obtain lock
    current_process_pid: int = os.getpid()

    try:
        # Check if download progress file does not exist.
        if not os.path.exists(pid_file_full_path):
            print_function_callback(
                f"Download progress file does not exist at {pid_file_full_path}"
            )
            # Create a new pid file with the current process id
            return _obtain_pid_lock_atomically(
                pid_file_full_path, current_process_pid, print_function_callback
            )

        # Try to open pid file at download progress location in read mode
        with open(pid_file_full_path, "r") as f:
            # Read pid file and obtain the process id from file contents
            pid = f.read()
            try:
                # Get process using the process id
                psutil.Process(int(pid))
                # Process with the pid exists, so we cannot obtain a lock
                raise RuntimeError(
                    f"Another download is in progress at {pid_file_full_path}, use --force-bootstrap or wait for previous download to finish"
                )
            except psutil.NoSuchProcess:
                # No such process exists with the process id, so we can delete the pid file
                print_function_callback(
                    f"Process with pid {pid} is not running. Deleting pid file."
                )

                f.close()  # Close the file before over-writing it

                # Create a new pid file with the current process id
                return _obtain_pid_lock_atomically(
                    pid_file_full_path, current_process_pid, print_function_callback
                )

    except Exception:
        # We already checked the file exists before reading it.
        # For any other unexpected exceptions, we should not delete the pid file and raise an error.
        raise


def release_pid_lock(
    pid_file_full_path: str, print_function_callback: Callable[[str], None]
) -> bool:
    """
    Releases the pid lock by deleting the pid file.
    :param pid_file_full_path: full path of the pid lock file
    :param print_function_callback: print_function_callback (Callable str -> None, optional): Callback to print messages produced in this function.
                Used in the CLI to print to stdout using click.echo. By default, ignores messages.
    :return: boolean, True if pid lock released successfully
    """
    print_function_callback(f"Releasing pid lock at {pid_file_full_path}")

    # Get the current process's id to obtain lock
    current_process_pid: int = os.getpid()

    # Check if pid lock file does not exist. Returns True if file doesn't exist as there is no lock to be released.
    if not os.path.exists(pid_file_full_path):
        print_function_callback(f"Pid lock file does not exist at {pid_file_full_path}")
        return True

    # Try to open pid file at download progress location in read mode
    with open(pid_file_full_path, "r") as f:
        # Read pid file and obtain the process id from file contents
        pid = f.read()

    # Process pid from file is same as current process pid - release pid lock
    if int(pid) == current_process_pid:
        print_function_callback(
            f"Process with pid {pid} is the current process. Deleting pid file."
        )
        os.remove(pid_file_full_path)
        return True
    # Process pid from file is different from current process pid - do not release lock
    else:
        print_function_callback(
            f"Process with pid {pid} is not the current process. Skipping pid file deletion."
        )
        return False


def _obtain_pid_lock_atomically(
    pid_file_full_path: str,
    current_process_pid: int,
    print_function_callback: Callable[[str], None],
) -> bool:
    """
    Obtains a lock on the pid file by writing the current process id to the file.
    :param current_process_pid: current process's pid
    :param pid_file_full_path: full path of pid file
    :param print_function_callback: print_function_callback (Callable str -> None, optional): Callback to print messages produced in this function.
                Used in the CLI to print to stdout using click.echo. By default, ignores messages.
    :return: boolean, True if pid lock was obtained successfully
    """

    # Generate a tmp file for writing the pid file as a whole and making the pid locking atomic
    tmp_file_name = pid_file_full_path + f"{current_process_pid}~tmp"

    # Get the current process id to write to pid file
    text = str(current_process_pid)

    print_function_callback(f"Creating new pid file at {pid_file_full_path} with pid {text}")

    # Open tmp file in write mode and write the current process id to it
    with open(tmp_file_name, "w+") as f:
        f.write(text)
        f.flush()
        os.fsync(f.fileno())

    # Replace pid_file_full_path with tmp_file
    os.replace(tmp_file_name, pid_file_full_path)

    return True
