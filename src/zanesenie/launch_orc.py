import os
import subprocess
import sys
from pathlib import Path
from typing import cast

import win32api
import win32con
import win32job

project_folder = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(project_folder))
sys.path.append(str(project_folder / "src"))
os.chdir(str(project_folder))


def main() -> None:
    proc = subprocess.Popen(
        ["./venv/Scripts/python.exe", "-OO", "./src/zanesenie/main.py"]
    )

    job = cast(int, win32job.CreateJobObject(None, ""))
    info = win32job.QueryInformationJobObject(
        job, win32job.JobObjectExtendedLimitInformation
    )
    info["BasicLimitInformation"]["LimitFlags"] |= (
        win32job.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
    )
    win32job.SetInformationJobObject(
        job, win32job.JobObjectExtendedLimitInformation, info
    )

    handle = win32api.OpenProcess(win32con.PROCESS_ALL_ACCESS, False, proc.pid)
    win32job.AssignProcessToJobObject(job, handle)

    return_code = proc.wait()
    sys.exit(return_code)


if __name__ == "__main__":
    main()
