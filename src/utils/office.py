from __future__ import annotations

import logging
import os
import shutil
import tempfile
from os.path import join, abspath, exists
from typing import TYPE_CHECKING

import win32com.client as win32

if TYPE_CHECKING:
    from typing import Protocol

    class DocumentProto(Protocol):
        def SaveAs(
            self, FileName: str, FileFormat: int, AddToRecentFiles: bool = True
        ) -> None: ...

        def Close(self, SaveChanges: bool) -> None: ...

    class DocumentsProto(Protocol):
        def Open(
            self,
            FileName: str,
            OpenAndRepair: bool,
            ConfirmConversions: bool,
            AddToRecentFiles: bool,
            Visible: bool,
            NoEncodingDialog: bool,
            ReadOnly: bool = False,
        ) -> DocumentProto: ...

    class WordProto(Protocol):
        def Quit(self) -> None: ...

        Visible: bool | int
        DisplayAlerts: bool | int
        AutomationSecurity: int
        Documents: DocumentsProto


logger = logging.getLogger("DAMU")


def recover_docx(file_path: str) -> None:
    with tempfile.TemporaryDirectory() as tmp_dir:
        recover_path = abspath(join(tmp_dir, "recovering.docx"))
        logger.info(recover_path)

        shutil.copy(file_path, recover_path)

        word, doc = None, None
        try:
            word = win32.DispatchEx("Word.Application")
            logger.info("Opened Word.Application")
            word.Visible = 0
            logger.info("Set Visible to 0")
            word.DisplayAlerts = 0
            logger.info("Set DisplayAlerts to 0")

            word.AutomationSecurity = 3
            logger.info("Set AutomationSecurity to 3")

            doc = word.Documents.Open(
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

            word.Quit()
            word = None
            logger.info("Closed Word.Application")

            if exists(file_path):
                os.unlink(file_path)
            os.rename(recover_path, file_path)
        except (Exception, BaseException) as err:
            if doc:
                doc.Close(False)
            if word:
                word.Quit()

            if exists(recover_path):
                os.remove(recover_path)

            raise Exception(str(err))


def docx_to_pdf(word: WordProto, docx_path: str, pdf_path: str) -> None:
    with tempfile.TemporaryDirectory() as tmp_dir:
        recover_path = abspath(join(tmp_dir, "recovering.docx"))
        if exists(recover_path):
            os.unlink(recover_path)

        logger.info(recover_path)

        shutil.copy(docx_path, recover_path)

        doc = None
        try:
            doc = word.Documents.Open(
                recover_path,
                OpenAndRepair=True,
                ConfirmConversions=False,
                AddToRecentFiles=False,
                Visible=False,
                NoEncodingDialog=True,
            )
            if doc is None:
                raise Exception(f"Failed to open document: {recover_path=!r}")
            logger.info("Opened document")

            doc.SaveAs(recover_path, FileFormat=16)
            logger.info(f"Saved recovered DOCX to: {recover_path}")
            doc.Close(False)
            doc = None

            doc = word.Documents.Open(
                recover_path,
                OpenAndRepair=True,
                ConfirmConversions=False,
                AddToRecentFiles=False,
                Visible=False,
                NoEncodingDialog=True,
                ReadOnly=True,
            )
            if doc is None:
                raise Exception(f"Failed to open document: {recover_path=!r}")
            logger.info("Opened document")

            doc.SaveAs(pdf_path, FileFormat=17, AddToRecentFiles=False)
            logger.info(f"Saved as PDF: {pdf_path}")

            doc.Close(False)
            doc = None
            logger.info("Closed document")

            if exists(recover_path):
                os.unlink(recover_path)
        except (Exception, BaseException) as err:
            if doc:
                doc.Close(False)

            if exists(recover_path):
                os.remove(recover_path)

            raise Exception(str(err))
