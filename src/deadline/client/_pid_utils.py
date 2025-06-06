# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import os
import sys
import psutil
from typing import Callable


def try_acquire_pid_lock(
    pid_file_full_path: str, print_function_callback: Callable[[str], None]
) -> bool:
    """
    Checks if the specified pid lock file exists and executes as per the following:
    If the pid lock file does not exist:
        It creates a new pid file and acquires lock for this pid.
        It handles concurrent processes trying to obtain lock such that only one process gets the lock and others fail

    If the pid lock file exists:
        If the process with its pid is still running, it raises an exception that a download is in progress already
        If the process with its pid is not running it deletes the pid lock file after taking a primitive file lock on it
        to handle concurrent processes making the same check. This will not work if primitive file locks are disabled.

    :param pid_file_full_path: full path of the pid lock file
    :param print_function_callback: Callback to print messages produced in this function.
                Used in the CLI to print to stdout using click.echo. By default, ignores messages.
    :return: boolean, True if pid lock was obtained successfully, throws an exception otherwise
    """
    print_function_callback(f"Checking if another download is in progress at {pid_file_full_path}")

    # 1. Get the current process's id to obtain lock
    current_process_pid: int = os.getpid()
    can_obtain_pid_lock: bool = False

    try:
        # 2. Check if pid file does not exist at pid file path in which case we can obtain a pid lock for this process
        if not os.path.exists(pid_file_full_path):
            print_function_callback(
                f"Download pid lock file does not exist at {pid_file_full_path}"
            )

            can_obtain_pid_lock = True

        # 3. Try to read existing pid file to verify process with the pid lock is active & hasn't ended pre-maturely
        else:
            with open(pid_file_full_path, "r") as f:
                pid = f.read()
                try:
                    psutil.Process(int(pid))
                    # Process with the pid exists, so we cannot obtain a lock
                    print_function_callback(
                        f"Unable to acquire pid lock as process with pid {pid} exists on {pid_file_full_path}"
                    )
                    raise RuntimeError(
                        f"Unable to acquire pid lock as process with pid {pid} exists on {pid_file_full_path}"
                    )
                except psutil.NoSuchProcess:
                    # No such process exists with the process id, so we can delete the pid file
                    print_function_callback(
                        f"Process with pid {pid} is not running. Deleting pid file."
                    )

                    # Obtain a lock on the pid file to avoid race conditions from another concurrent process
                    # Read the pid again from the file and validate it hasn't changed since the first read
                    # Delete the pid file
                    _lock_pid_file_and_release_dangling_pid_lock(pid, pid_file_full_path)

                    can_obtain_pid_lock = True

                    f.close()  # Close the file at the end - required for windows

    except Exception:
        # For any other unexpected exceptions, we should raise an error.
        raise

    # 4. If we've determined we can obtain lock for this process, we do so now.
    # We handle concurrency for processes racing for this pid lock by using atomic primitives
    # The pid lock is obtained successfully for one process and fails for the other process
    # We use os.rename() for windows and os.link() for linux/macOS as atomic primitives
    if can_obtain_pid_lock:
        # Generate a tmp file for writing the pid file as a whole and prevent corrupt data
        tmp_file_name = pid_file_full_path + f"{current_process_pid}~tmp"

        print_function_callback(
            f"Creating new pid file at {pid_file_full_path} with pid {current_process_pid}"
        )

        # Open tmp file in write mode and write the current process id to it
        with open(tmp_file_name, "w+") as f:
            f.write(str(current_process_pid))
            f.flush()
            os.fsync(f.fileno())

        # Atomic write to pid file while handling concurrency for parallel processes trying to race for the pid lock
        try:
            if sys.platform.startswith("linux") or sys.platform.startswith("darwin"):
                os.link(tmp_file_name, pid_file_full_path)
                os.remove(tmp_file_name)
            else:
                os.rename(tmp_file_name, pid_file_full_path)
        except FileExistsError:
            print_function_callback(
                f"Concurrency issue when trying to obtain pid lock at {pid_file_full_path} for process id {current_process_pid}"
            )
            raise

    return True


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


def _lock_pid_file_and_release_dangling_pid_lock(pid: str, pid_file_full_path: str) -> bool:
    """
    Helper method to lock the pid file and delete it to release a dangling pid lock for a pid which is not active
    The primitive lock is obtained on the pid file so concurrent processes cant delete it
    This would be caught in race conditions if primitive file locking is disabled on a customer's machine.
    :param pid: inactive pid in the file
    :param pid_file_full_path: full path of the pid file
    :return: returns True if it was able to release the inactive pid dangling lock.
        Throws a runtime exception if the pid was updated by a different concurrent process and does not try to delete file
        Throws an unexpected exception if we get into an error while trying to obtain lock on file.
    """
    file_size: int = os.path.getsize(os.path.realpath(pid_file_full_path))

    with open(pid_file_full_path, "a+") as file_locked_for_delete:
        try:
            # Acquire an exclusive lock
            if sys.platform.startswith("win32") or sys.platform.startswith("cygwin"):
                import msvcrt

                msvcrt.locking(file_locked_for_delete.fileno(), msvcrt.LK_RLCK, file_size)

            else:
                import fcntl

                fcntl.flock(file_locked_for_delete.fileno(), fcntl.LOCK_EX)

            # This is required to read the contents of the original file opened in write/append mode in Windows
            file_locked_for_delete.seek(0)

            # Verify locked file has expected data before delete
            if pid == str(file_locked_for_delete.read()):
                # Need to close the file for deleting it in Windows
                file_locked_for_delete.close()
                os.remove(file_locked_for_delete.name)  # Delete the pid file

            # If the pid changed before we locked the file to release the inactive pid lock, we throw a runtime error and exit
            # This could happen if another concurrent process overrode the inactive pid lock before we released it.
            else:
                raise RuntimeError(
                    f"Unable to acquire pid lock as process with pid {pid} exists on {pid_file_full_path}"
                )

        finally:
            # Release the lock always if the file hasn't been deleted by this process already
            if os.path.exists(file_locked_for_delete.name):
                if sys.platform.startswith("win32"):
                    import msvcrt

                    msvcrt.locking(file_locked_for_delete.fileno(), msvcrt.LK_UNLCK, file_size)

                else:
                    import fcntl

                    fcntl.flock(file_locked_for_delete.fileno(), fcntl.LOCK_UN)

            file_locked_for_delete.close()  # Close the file at the end, always required for Windows

    return True
