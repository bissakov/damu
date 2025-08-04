from contextlib import contextmanager
from typing import TYPE_CHECKING

import win32com.client as win32

from utils.utils import kill_all_processes

if TYPE_CHECKING:
    from typing import Generator
    from pathlib import Path


@contextmanager
def dispatch(application: str) -> Generator[win32.Dispatch, None, None]:
    app = win32.Dispatch(application)
    app.DisplayAlerts = False
    try:
        yield app
    finally:
        kill_all_processes(proc_name="EXCEL")


@contextmanager
def workbook_open(
    excel: win32.Dispatch, file_path: str
) -> Generator[win32.Dispatch, None, None]:
    wb = excel.Workbooks.Open(file_path)
    try:
        yield wb
    finally:
        wb.Close()


def xls_to_xlsx(source: Path, dest: Path):
    kill_all_processes("EXCEL")
    if dest.exists():
        dest.unlink()
    with dispatch(application="Excel.Application") as excel:
        with workbook_open(excel=excel, file_path=str(source)) as wb:
            wb.SaveAs(str(dest), FileFormat=51)
    source.unlink()
