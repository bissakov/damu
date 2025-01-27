import logging
import shutil
from enum import Enum
from pathlib import Path
from typing import Any, Union

import win32com.client as win32
import win32com

from src.utils.utils import kill_all_processes


class OfficeType(Enum):
    ExcelType: str = "Excel.Application"
    WordType: str = "Word.Application"


class UnsupportedOfficeAppError(Exception):
    def __init__(self, office_type: OfficeType) -> None:
        message = f"Unknown {office_type!r}"
        super().__init__(message)


class Office:
    def __init__(self, file_path: Union[str, Path], office_type: OfficeType) -> None:
        self.office_type = office_type

        self.file_path: str = (
            str(file_path) if isinstance(file_path, Path) else file_path
        )
        try:
            self.app = win32.Dispatch(office_type.value)
        except AttributeError:
            shutil.rmtree(win32com.__gen_path__)
            self.app = win32.Dispatch(office_type.value)

        self.app.Visible = False
        self.app.DisplayAlerts = False

        self.potential_error = UnsupportedOfficeAppError(office_type=office_type)

        match office_type:
            case OfficeType.ExcelType:
                self.doc = self.open_workbook()
            case OfficeType.WordType:
                self.doc = self.open_doc()
            case _:
                raise self.potential_error

    def open_doc(self) -> Any:
        if self.office_type != OfficeType.WordType:
            raise self.potential_error
        return self.app.Documents.Open(self.file_path)

    def open_workbook(self) -> Any:
        if self.office_type != OfficeType.ExcelType:
            raise self.potential_error
        return self.app.Workbooks.Open(self.file_path)

    def save_as(self, file_path: Union[str, Path], file_format: int) -> None:
        file_path: str = str(file_path) if isinstance(file_path, Path) else file_path
        self.doc.SaveAs(file_path, FileFormat=file_format)

    def close_doc(self) -> None:
        if not self.doc:
            return

        try:
            self.doc.Close()
        except (Exception, BaseException) as err:
            logging.exception(err)
            kill_all_processes(proc_name="WINWORD")

    def quit_app(self) -> None:
        if not self.app:
            return

        try:
            self.app.Quit()
        except (Exception, BaseException) as err:
            logging.exception(err)
            kill_all_processes(proc_name="WINWORD")
        del self.app

    def __enter__(self) -> "Office":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close_doc()
        self.quit_app()
