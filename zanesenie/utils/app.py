import dataclasses
import logging
import os
import random
import re
from pathlib import Path
from time import sleep
from types import TracebackType
from typing import Literal, Optional, Type, Union

import pyautogui
import pyperclip
import pywinauto
import pywinauto.base_wrapper
import pywinauto.timings
import win32con
import win32gui
from PIL import ImageDraw, ImageFont, ImageGrab
from pywinauto import mouse, win32functions

from zanesenie.notification import TelegramAPI
from zanesenie.utils.utils import kill_all_processes

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

            img_width, img_height = img.size
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
        backend: Optional[None] = None,
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
        self,
        app_path: str,
        app: Optional[pywinauto.Application] = None,
        bot: Optional[TelegramAPI] = None,
    ) -> None:
        if not app:
            kill_all_processes("1cv8.exe")
        self.app_path = app_path
        self.app = app
        self.utils = AppUtils(app=self.app)
        self.bot = bot

    def switch_backend(self, backend: Literal["uia", "win32"]) -> None:
        self.app = pywinauto.Application(backend=backend).connect(
            path=r"C:\Program Files (x86)\1cv8\8.3.25.1394\bin\1cv8.exe"
        )

    def open_app(self) -> None:
        for _ in range(10):
            try:
                os.startfile(self.app_path)
                sleep(2)

                pywinauto.Desktop(backend="uia")

                self.app = pywinauto.Application(backend="uia").connect(
                    path=r"C:\Program Files (x86)\1cv8\8.3.25.1394\bin\1cv8.exe"
                )
                self.utils.app = self.app
                break
            except (Exception, BaseException) as err:
                logging.exception(err)
                kill_all_processes("1cv8.exe")
                continue
        assert self.app is not None, Exception("max_retries exceeded")
        self.utils.app = self.app

    def dialog_text(
        self,
        dialog_win: pywinauto.WindowSpecification | None = None,
    ) -> str:
        if not dialog_win:
            dialog_win = self.app.Dialog
        if not dialog_win.exists() or not dialog_win.is_enabled():
            return ""

        pyperclip.copy("")
        dialog_win.type_keys("^C")
        sleep(2)
        dialog_text = pyperclip.paste()
        pyperclip.copy("")
        logging.info(f"{dialog_text=}")

        if not dialog_text:
            return ""

        dialog_items = re.split("[\r\n]+", dialog_text)
        dialog_text = dialog_items[-2]
        return dialog_text

    def find_and_click_button(
        self,
        window: pywinauto.WindowSpecification,
        toolbar: pywinauto.WindowSpecification,
        target_button_name: str,
        horizontal: bool = True,
        offset: int = 5,
        step: int = 5,
    ) -> tuple[int, int]:
        self.utils.set_focus(window)

        status_win = self.app.window(title_re="Банковская система.+")
        rectangle = toolbar.rectangle()
        mid_point = rectangle.mid_point()
        mouse.move(coords=(mid_point.x, mid_point.y))

        start_point = rectangle.left if horizontal else rectangle.top
        end_point = mid_point.x if horizontal else mid_point.y

        x, y = mid_point.x, mid_point.y
        point = 0

        x_offset = offset if horizontal else 0
        y_offset = offset if not horizontal else 0

        error_count = 0

        i = 0
        while (
            active_button := status_win["StatusBar"].window_text().strip()
        ) != target_button_name:
            if point > end_point:
                logging.error(f"{point=}, {end_point=}")
                logging.error(f"{active_button=}, f{target_button_name=}")

                point = 0
                i = 0
                if step > 5:
                    step = 5

                error_count += 1

                if error_count >= 3:
                    raise pywinauto.findwindows.ElementNotFoundError
                continue

            point = start_point + i * step

            if horizontal:
                x = point
            else:
                y = point

            mouse.move(coords=(x, y))
            i += 1

        x += x_offset
        y += y_offset

        mouse.click(button="left", coords=(x, y))

        return x, y

    def reload(self) -> None:
        self.exit()
        self.open_app()

    def exit(self) -> None:
        if not self.app.kill():
            kill_all_processes("1cv8.exe")

    def __enter__(self) -> "App":
        self.open_app()
        return self

    def __exit__(
        self,
        exc_type: Type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ):
        # if exc_val is not None or exc_type is not None or exc_tb is not None:
        #     if self.bot:
        #         self.bot.send_message(media=ImageGrab.grab())
        # self.exit()
        pass


def close_entry_without_saving(
    utils: AppUtils, order_win: pywinauto.WindowSpecification
) -> None:
    dialog_win = utils.app.Dialog
    if dialog_win.exists() and dialog_win.is_enabled():
        dialog_win.close()

    order_win.type_keys("{ESC}")
    confirm_win = utils.get_window(title="Подтверждение")
    confirm_win["&Нет"].click()


def save_excel(colvir: App, work_folder: Path) -> Path:
    file_win = colvir.utils.get_window(title="Выберите файл для экспорта")

    orders_file_path = work_folder / "orders.xls"

    if orders_file_path.exists():
        orders_file_path.unlink()

    file_win["Edit4"].set_text(str(orders_file_path))
    colvir.utils.bi_click_input(file_win["&Save"])

    sort_win = colvir.utils.get_window(title="Сортировка")
    sort_win["OK"].click()

    sleep(1)
    if (error_win := colvir.app.window(title="Произошла ошибка")).exists():
        error_win.close()

    while not orders_file_path.exists():
        sleep(5)
    sleep(1)

    if (error_win := colvir.app.window(title="Произошла ошибка")).exists():
        error_win.close()

    kill_all_processes("EXCEL")

    if (error_win := colvir.app.window(title="Произошла ошибка")).exists():
        error_win.close()
    return orders_file_path
