from __future__ import annotations

import logging
import os
import shutil
from enum import Enum
from os.path import join, dirname, abspath, exists
from pathlib import Path
from typing import TYPE_CHECKING

import win32com
import win32com.client as win32
from utils.utils import kill_all_processes

if TYPE_CHECKING:
    from typing import Any

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
            self.app = win32.DispatchEx(office_type.value)
        except AttributeError:
            shutil.rmtree(win32com.__gen_path__)
            self.app = win32.DispatchEx(office_type.value)

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

        assert self.doc is not None, f"{self.doc=!r}, {office_type=!r}"

    def open_doc(self) -> Any:
        if self.office_type != Office.Type.WordType:
            raise self.potential_error
        return self.app.Documents.Open(
            self.file_path,
            OpenAndRepair=True,
            ConfirmConversions=False,
            ReadOnly=True,
            AddToRecentFiles=False,
            Visible=False,
            NoEncodingDialog=True,
        )

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


def recover_docx(file_path: str) -> None:
    recover_path = abspath(join(dirname(file_path), "recovering.docx"))
    if exists(recover_path):
        os.unlink(recover_path)

    logger.info(recover_path)

    shutil.copy(file_path, recover_path)

    app, doc = None, None
    try:
        app = win32.DispatchEx("Word.Application")
        logger.info("Opened Word.Application")
        app.Visible = 0
        logger.info("Set Visible to 0")
        app.DisplayAlerts = 0
        logger.info("Set DisplayAlerts to 0")

        app.AutomationSecurity = 3
        logger.info("Set AutomationSecurity to 3")

        doc = app.Documents.Open(
            recover_path,
            OpenAndRepair=True,
            ConfirmConversions=False,
            AddToRecentFiles=False,
            Visible=False,
            NoEncodingDialog=True,
        )
        if doc is None:
            raise Exception(f"Failed to open document: {file_path}")

        logger.info("Opened document")

        doc.SaveAs(recover_path, FileFormat=16, AddToRecentFiles=False)
        logger.info("Saved as DOCX")

        doc.Close(False)
        doc = None
        logger.info("Closed document")

        app.Quit()
        app = None
        logger.info("Closed Word.Application")

        if exists(file_path):
            os.unlink(file_path)
        os.rename(recover_path, file_path)
    except (Exception, BaseException) as err:
        if doc:
            doc.Close(False)
        if app:
            app.Quit()
        raise err


def docx_to_pdf(docx_path: str, pdf_path: str) -> None:
    recover_path = abspath(join(dirname(docx_path), "recovering.docx"))
    if exists(recover_path):
        os.unlink(recover_path)

    logger.info(recover_path)

    shutil.copy(docx_path, recover_path)

    app, doc = None, None
    try:
        app = win32.DispatchEx("Word.Application")
        logger.info("Opened Word.Application")
        app.Visible = 0
        logger.info("Set Visible to 0")
        app.DisplayAlerts = 0
        logger.info("Set DisplayAlerts to 0")

        app.AutomationSecurity = 3
        logger.info("Set AutomationSecurity to 3")

        doc = app.Documents.Open(
            str(docx_path),
            OpenAndRepair=True,
            ConfirmConversions=False,
            ReadOnly=True,
            AddToRecentFiles=False,
            Visible=False,
            NoEncodingDialog=True,
        )
        logger.info("Opened document")

        doc.SaveAs(pdf_path, FileFormat=17, AddToRecentFiles=False)
        logger.info("Saved as PDF")

        doc.Close(False)
        doc = None
        logger.info("Closed document")

        app.Quit()
        app = None
        logger.info("Closed Word.Application")

        if exists(recover_path):
            os.unlink(recover_path)
    except (Exception, BaseException) as err:
        if doc:
            doc.Close(False)
        if app:
            app.Quit()
        raise err


def main():
    recover_docx(
        Path(
            r"C:\Users\robot2\Desktop\robots\damu\downloads\sverka\2025-07-31\a6708a6a-e3b1-4062-b29e-6889bd8e03ba\documents\ДС по внутрен торговли_ИП Урдабаева М-05-28-2025_2 транш.docx"
        )
    )


if __name__ == "__main__":
    main()
