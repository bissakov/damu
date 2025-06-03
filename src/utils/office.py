from __future__ import annotations

import logging
import os
import shutil
from enum import Enum
from pathlib import Path
from typing import Any

import win32com
import win32com.client as win32
from utils.utils import kill_all_processes

logger = logging.getLogger("DAMU")


class Office:
    class UnsupportedOfficeAppError(Exception):
        def __init__(self, office_type: Office.Type) -> None:
            message = f"Unknown {office_type!r}"
            super().__init__(message)

    class Type(Enum):
        ExcelType = "Excel.Application"
        WordType = "Word.Application"

    class Format(Enum):
        DOCX = 16
        PDF = 17

    def __init__(self, file_path: str | Path, office_type: Type) -> None:
        self.office_type = office_type

        self.file_path: str = (
            str(file_path) if isinstance(file_path, Path) else file_path
        )
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

        self.potential_error = Office.UnsupportedOfficeAppError(
            office_type=office_type
        )

        match office_type:
            case Office.Type.ExcelType:
                self.doc = self.open_workbook()
            case Office.Type.WordType:
                self.doc = self.open_doc()

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

    def save_as(
        self, output_file_path: str | Path, file_format: Format
    ) -> None:
        output_file_path = (
            str(output_file_path)
            if isinstance(output_file_path, Path)
            else output_file_path
        )
        if not self.validate_format(
            file_path=output_file_path, file_format=file_format
        ):
            raise ValueError(
                f"File format and extension mismatch - {output_file_path!r} {file_format!r}"
            )

        if self.project_folder:
            output_file_path = os.path.join(
                self.project_folder, output_file_path
            )
        self.doc.SaveAs(output_file_path, FileFormat=file_format.value)

    def close_doc(self) -> None:
        if not self.doc:
            return

        try:
            self.doc.Close()
        except (Exception, BaseException) as err:
            logger.exception(err)
            kill_all_processes(proc_name="WINWORD")

    def quit_app(self) -> None:
        if not self.app:
            return

        try:
            self.app.Quit()
        except (Exception, BaseException) as err:
            logger.exception(err)
            kill_all_processes(proc_name="WINWORD")
        del self.app

    def __enter__(self) -> Office:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close_doc()
        self.quit_app()


def close_doc(doc) -> None:
    if not doc:
        return

    try:
        doc.Close()
    except (Exception, BaseException) as err:
        logging.exception(err)
        kill_all_processes(proc_name="WINWORD")


def quit_app(app) -> None:
    if not app:
        return

    try:
        app.Quit()
    except (Exception, BaseException) as err:
        logging.exception(err)
        kill_all_processes(proc_name="WINWORD")


def docx_to_pdf(docx_path: Path, pdf_path: Path) -> None:
    app = None
    doc = None

    try:
        app = win32.Dispatch("Word.Application")
        app.Visible = False
        app.DisplayAlerts = False

        doc = app.Documents.Open(str(docx_path))
        doc.SaveAs(str(pdf_path), FileFormat=17)
    except Exception as e:
        try:
            print(docx_path)
            app = win32.gencache.EnsureDispatch("Word.Application")
            app.Visible = False

            doc = app.Documents.Open(str(docx_path), OpenAndRepair=True)
            doc.SaveAs(str(pdf_path), FileFormat=17)
        except Exception as e2:
            print(docx_path)
            close_doc(doc)
            quit_app(app)

            raise e2
    finally:
        close_doc(doc)
        quit_app(app)
