import dataclasses
import logging
import os
import random
import re
from pathlib import Path
from time import sleep
from types import TracebackType
from typing import Literal, Type

import pyautogui
import pywinauto
import pywinauto.base_wrapper
import pywinauto.timings
import win32con
import win32gui
from PIL import ImageDraw, ImageFont, ImageGrab
from pywinauto import mouse, win32functions

from utils.utils import kill_all_processes

pyautogui.FAILSAFE = False


@dataclasses.dataclass(slots=True)
class AppInfo:
    app_path: Path
    user: str
    password: str


class AppUtils:
    def __init__(self, app: pywinauto.Application | None):
        self.app = app

    @staticmethod
    def take_screenshot(
        path: str, text: str = "", img_format: str = "JPEG"
    ) -> None:
        img_format = img_format.upper()

        match img_format:
            case "JPEG" | "JPG":
                img_format = "JPEG"
                params = {"optimize": True}
            case "PNG":
                params = {"optimize": True}
            case _:
                raise ValueError(
                    f"Unsupported format '{img_format}'. Supported formats: 'PNG', 'JPEG', 'JPG'"
                )

        img = ImageGrab.grab()

        if text:
            draw = ImageDraw.Draw(img)
            font = ImageFont.truetype("arial.ttf", size=34)

            img_width, _ = img.size
            bbox = draw.textbbox((0, 0), text, font=font)

            text_width = bbox[2] - bbox[0]

            x_position = (img_width - text_width) // 2
            y_position = 8

            text_color = (0, 0, 0)
            draw.text(
                (x_position, y_position), text, font=font, fill=text_color
            )

        img.save(path, format=img_format, **params)

    @staticmethod
    def wiggle_mouse(duration: int) -> None:
        def get_random_coords() -> tuple[int, int]:
            screen = pyautogui.size()
            width = screen[0]
            height = screen[1]

            return random.randint(100, width - 200), random.randint(
                100, height - 200
            )

        max_wiggles = random.randint(4, 9)
        step_sleep = duration / max_wiggles

        for _ in range(1, max_wiggles):
            coords = get_random_coords()
            pyautogui.moveTo(x=coords[0], y=coords[1], duration=step_sleep)

    @staticmethod
    def close_window(
        win: pywinauto.WindowSpecification, raise_error: bool = False
    ) -> None:
        if win.exists():
            win.close()
            return

        if raise_error:
            raise pywinauto.findwindows.ElementNotFoundError(
                f"Window {win} does not exist"
            )

    @staticmethod
    def set_focus_win32(win: pywinauto.WindowSpecification) -> None:
        if win.has_focus():
            return

        handle = win.handle

        mouse.move(coords=(-10000, 500))
        if win.is_minimized():
            if win.was_maximized():
                win.maximize()
            else:
                win.restore()
        else:
            win32gui.ShowWindow(handle, win32con.SW_SHOW)
        win32gui.SetForegroundWindow(handle)

        win32functions.WaitGuiThreadIdle(handle)

    def set_focus(
        self,
        win: pywinauto.WindowSpecification,
        backend: str | None = None,
        retries: int = 20,
    ) -> None:
        old_backend = self.app.backend.name
        if backend:
            self.app.backend.name = backend

        while retries > 0:
            try:
                if retries % 2 == 0:
                    AppUtils.set_focus_win32(win)
                else:
                    if not win.has_focus():
                        win.set_focus()
                if backend:
                    self.app.backend.name = old_backend
                break
            except (Exception, BaseException):
                retries -= 1
                sleep(5)
                continue

        if retries <= 0:
            if backend:
                self.app.backend.name = old_backend
            raise Exception("Failed to set focus")

    @staticmethod
    def press(
        win: pywinauto.WindowSpecification, key: str, pause: float = 0
    ) -> None:
        AppUtils.set_focus(win)
        win.type_keys(key, pause=pause, set_foreground=False)

    @staticmethod
    def type_keys(
        window: pywinauto.WindowSpecification,
        keystrokes: str,
        step_delay: float = 0.1,
        delay_before: float = 0.5,
        delay_after: float = 0.5,
    ) -> None:
        sleep(delay_before)

        AppUtils.set_focus(window)
        for command in list(filter(None, re.split(r"({.+?})", keystrokes))):
            try:
                window.type_keys(command, set_foreground=False)
            except pywinauto.base_wrapper.ElementNotEnabled:
                sleep(1)
                window.type_keys(command, set_foreground=False)
            sleep(step_delay)

        sleep(delay_after)

    def bi_click_input(
        self,
        window: pywinauto.WindowSpecification,
        delay_before: float = 0.0,
        delay_after: float = 0.0,
    ) -> None:
        sleep(delay_before)
        self.set_focus(window)
        window.click_input()
        sleep(delay_after)

    def get_window(
        self,
        title: str,
        wait_for: str = "exists",
        timeout: int = 20,
        regex: bool = False,
        found_index: int = 0,
    ) -> pywinauto.WindowSpecification:
        if regex:
            window = self.app.window(title_re=title, found_index=found_index)
        else:
            window = self.app.window(title=title, found_index=found_index)
        window.wait(wait_for=wait_for, timeout=timeout)
        sleep(0.5)
        return window

    def persistent_win_exists(self, title_re: str, timeout: float) -> bool:
        try:
            self.app.window(title_re=title_re).wait(
                wait_for="enabled", timeout=timeout
            )
        except pywinauto.timings.TimeoutError:
            return False
        return True

    def close_dialog(self) -> None:
        dialog_win = self.app.Dialog
        if dialog_win.exists() and dialog_win.is_enabled():
            dialog_win.close()


class App:
    def __init__(
        self, app_path: str, app: pywinauto.Application | None = None
    ) -> None:
        if not app:
            kill_all_processes("1cv8.exe")
        self.app_path = app_path
        self._app = app
        self.utils = AppUtils(app=self._app)

    @property
    def app(self) -> pywinauto.Application:
        if not self._app:
            raise RuntimeError("No running application")
        return self._app

    def switch_backend(self, backend: Literal["uia", "win32"]) -> None:
        self._app = pywinauto.Application(backend=backend).connect(
            path=r"C:\Program Files (x86)\1cv8\8.3.25.1394\bin\1cv8.exe"
        )

    def open_app(self) -> None:
        for _ in range(10):
            try:
                os.startfile(self.app_path)
                sleep(2)

                pywinauto.Desktop(backend="uia")

                self._app = pywinauto.Application(backend="uia").connect(
                    path=r"C:\Program Files (x86)\1cv8\8.3.25.1394\bin\1cv8.exe"
                )
                self.utils.app = self._app
                break
            except (Exception, BaseException) as err:
                logging.exception(err)
                kill_all_processes("1cv8.exe")
                continue
        assert self._app is not None, Exception("max_retries exceeded")
        self.utils.app = self._app

    def reload(self) -> None:
        self.exit()
        self.open_app()

    def exit(self) -> None:
        if self.app and not self.app.kill():
            kill_all_processes("1cv8.exe")

    def __enter__(self) -> "App":
        self.open_app()
        return self

    def __exit__(
        self,
        exc_type: Type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None: ...
