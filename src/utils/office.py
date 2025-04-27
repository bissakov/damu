import logging
import os
import shutil
from enum import Enum
from pathlib import Path
from typing import Any, Union

import psutil
import win32com.client as win32
import win32com


def kill_all_processes(proc_name: str) -> None:
    for proc in psutil.process_iter():
        try:
            if proc_name in proc.name():
                proc.terminate()
        except (psutil.AccessDenied, psutil.NoSuchProcess):
            continue


class Office:
    class UnsupportedOfficeAppError(Exception):
        def __init__(self, office_type: "Office.Type") -> None:
            message = f"Unknown {office_type!r}"
            super().__init__(message)

    class Type(Enum):
        ExcelType: str = "Excel.Application"
        WordType: str = "Word.Application"

    class Format(Enum):
        DOCX: int = 16
        PDF: int = 17

    def __init__(self, file_path: Union[str, Path], office_type: Type) -> None:
        self.office_type = office_type

        self.file_path: str = str(file_path) if isinstance(file_path, Path) else file_path
        self.project_folder = os.getenv("project_folder")
        if self.project_folder:
            self.file_path = os.path.join(self.project_folder, self.file_path)
        try:
            self.app = win32.Dispatch(office_type.value)
        except AttributeError:
            shutil.rmtree(win32com.__gen_path__)
            self.app = win32.Dispatch(office_type.value)

        self.app.Visible = False
        self.app.DisplayAlerts = False

        self.potential_error = Office.UnsupportedOfficeAppError(office_type=office_type)

        match office_type:
            case Office.Type.ExcelType:
                self.doc = self.open_workbook()
            case Office.Type.WordType:
                self.doc = self.open_doc()
            case _:
                raise self.potential_error

    def open_doc(self) -> Any:
        if self.office_type != Office.Type.WordType:
            raise self.potential_error
        return self.app.Documents.Open(self.file_path)

    def open_workbook(self) -> Any:
        if self.office_type != Office.Type.ExcelType:
            raise self.potential_error
        return self.app.Workbooks.Open(self.file_path)

    @staticmethod
    def validate_format(file_path: str, file_format: Format) -> bool:
        file_extension = file_path.rsplit(".")[-1]

        match file_format:
            case file_format.DOCX:
                return file_extension == "docx"
            case file_format.PDF:
                return file_extension == "pdf"
            case _:
                return False

    def save_as(self, file_path: Union[str, Path], file_format: Format) -> None:
        file_path: str = str(file_path) if isinstance(file_path, Path) else file_path
        if not self.validate_format(file_path=file_path, file_format=file_format):
            raise ValueError(f"File format and extension mismatch - {file_path!r} {file_format!r}")

        if self.project_folder:
            file_path = os.path.join(self.project_folder, file_path)
        self.doc.SaveAs(file_path, FileFormat=file_format.value)

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
