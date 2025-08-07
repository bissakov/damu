from __future__ import annotations

import os
from time import sleep, time
from typing import cast, overload, TYPE_CHECKING

from _ctypes import COMError
from pywinauto import Application, keyboard


if TYPE_CHECKING:
    from collections.abc import Callable, Generator
    from typing import Any, Literal, NewType, TypeAlias

    from pywinauto import WindowSpecification

    from pywinauto.controls.uia_controls import (
        ButtonWrapper as _ButtonWrapper,
        EditWrapper as _EditWrapper,
        ListItemWrapper as _ListItemWrapper,
        ListViewWrapper as _ListViewWrapper,
        MenuItemWrapper as _MenuItemWrapper,
        MenuWrapper as _MenuWrapper,
        ToolbarWrapper as _ToolbarWrapper,
        TabControlWrapper as _TabControlWrapper,
    )
    from pywinauto.controls.uiawrapper import UIAWrapper as _UIAWrapper

    UIAWrapper = NewType("UIAWrapper", _UIAWrapper)
    ButtonWrapper = NewType("ButtonWrapper", _ButtonWrapper)
    CheckBoxWrapper = NewType("CheckBoxWrapper", _ButtonWrapper)
    UIACustomWrapper = NewType("UIACustomWrapper", _UIAWrapper)
    UIADocumentWrapper = NewType("UIADocumentWrapper", _UIAWrapper)
    EditWrapper = NewType("EditWrapper", _EditWrapper)
    ListViewWrapper = NewType("ListViewWrapper", _ListViewWrapper)
    ListItemWrapper = NewType("ListItemWrapper", _ListItemWrapper)
    UIAPaneWrapper = NewType("UIAPaneWrapper", _UIAWrapper)
    UIATabWrapper = NewType("UIATabWrapper", _TabControlWrapper)
    UIATabItemWrapper = NewType("UIATabItemWrapper", _UIAWrapper)
    UIATableWrapper = NewType("UIATableWrapper", _ListViewWrapper)
    UIAMenuWrapper = NewType("UIAMenuWrapper", _MenuWrapper)
    UIAMenuItemWrapper = NewType("UIAMenuItemWrapper", _MenuItemWrapper)
    UIAToolbarWrapper = NewType("UIAToolbarWrapper", _ToolbarWrapper)

    UiaElement: TypeAlias = (
        WindowSpecification
        | UIAWrapper
        | ButtonWrapper
        | CheckBoxWrapper
        | UIACustomWrapper
        | UIADocumentWrapper
        | EditWrapper
        | ListViewWrapper
        | ListItemWrapper
        | UIAPaneWrapper
        | UIATabWrapper
        | UIATabItemWrapper
        | UIATableWrapper
        | UIAMenuWrapper
        | UIAMenuItemWrapper
        | UIAToolbarWrapper
    )
else:
    UIAWrapper = object
    ButtonWrapper = object
    CheckBoxWrapper = object
    UIACustomWrapper = object
    UIADocumentWrapper = object
    EditWrapper = object
    ListViewWrapper = object
    ListItemWrapper = object
    UIAPaneWrapper = object
    UIATabWrapper = object
    UIATabItemWrapper = object
    UIATableWrapper = object
    UIAMenuWrapper = object
    UIAMenuItemWrapper = object
    UIAToolbarWrapper = object
    UiaElement = object


@overload
def child(
    parent: UiaElement,
    ctrl: Literal["Button"],
    title: str | None = None,
    idx: int = 0,
) -> WindowSpecification | _ButtonWrapper: ...
@overload
def child(
    parent: UiaElement,
    ctrl: Literal["CheckBox"],
    title: str | None = None,
    idx: int = 0,
) -> WindowSpecification | CheckBoxWrapper: ...
@overload
def child(
    parent: UiaElement,
    ctrl: Literal["Custom"],
    title: str | None = None,
    idx: int = 0,
) -> WindowSpecification | UIACustomWrapper: ...
@overload
def child(
    parent: UiaElement,
    ctrl: Literal["Document"],
    title: str | None = None,
    idx: int = 0,
) -> WindowSpecification | UIADocumentWrapper: ...
@overload
def child(
    parent: UiaElement,
    ctrl: Literal["Edit"],
    title: str | None = None,
    idx: int = 0,
) -> WindowSpecification | EditWrapper: ...
@overload
def child(
    parent: UiaElement,
    ctrl: Literal["List"],
    title: str | None = None,
    idx: int = 0,
) -> WindowSpecification | ListViewWrapper: ...
@overload
def child(
    parent: UiaElement,
    ctrl: Literal["ListItem"],
    title: str | None = None,
    idx: int = 0,
) -> WindowSpecification | ListItemWrapper: ...
@overload
def child(
    parent: UiaElement,
    ctrl: Literal["Pane"],
    title: str | None = None,
    idx: int = 0,
) -> WindowSpecification | UIAPaneWrapper: ...
@overload
def child(
    parent: UiaElement,
    ctrl: Literal["Tab"],
    title: str | None = None,
    idx: int = 0,
) -> WindowSpecification | UIATabWrapper: ...
@overload
def child(
    parent: UiaElement,
    ctrl: Literal["TabItem"],
    title: str | None = None,
    idx: int = 0,
) -> WindowSpecification | UIATabItemWrapper: ...
@overload
def child(
    parent: UiaElement,
    ctrl: Literal["Table"],
    title: str | None = None,
    idx: int = 0,
) -> WindowSpecification | UIATableWrapper: ...
@overload
def child(
    parent: UiaElement,
    ctrl: Literal["Menu"],
    title: str | None = None,
    idx: int = 0,
) -> WindowSpecification | UIAMenuWrapper: ...
@overload
def child(
    parent: UiaElement,
    ctrl: Literal["MenuItem"],
    title: str | None = None,
    idx: int = 0,
) -> WindowSpecification | UIAMenuItemWrapper: ...
@overload
def child(
    parent: UiaElement,
    ctrl: Literal["ToolBar"],
    title: str | None = None,
    idx: int = 0,
) -> WindowSpecification | UIAToolbarWrapper: ...


def child(
    parent: UiaElement,
    ctrl: Literal[
        "Button",
        "CheckBox",
        "Custom",
        "Document",
        "Edit",
        "List",
        "ListItem",
        "Pane",
        "Tab",
        "TabItem",
        "Table",
        "Menu",
        "MenuItem",
        "ToolBar",
    ],
    title: str | None = None,
    idx: int = 0,
) -> Any:
    return cast("WindowSpecification", parent).child_window(
        title=title, control_type=ctrl, found_index=idx
    )


def window(
    app: Application, title: str, regex: bool = False
) -> WindowSpecification:
    if regex:
        return cast("WindowSpecification", app.window(title_re=title))
    else:
        return cast("WindowSpecification", app.window(title=title))


def focus(element: UiaElement) -> None:
    if not element.is_active():
        element.set_focus()
        cast("WindowSpecification", element).wait(wait_for="active")


@overload
def iter_children(
    parent: WindowSpecification | ListViewWrapper,
) -> Generator[WindowSpecification | ListItemWrapper]: ...
@overload
def iter_children(
    parent: WindowSpecification | UIATableWrapper,
) -> Generator[WindowSpecification | UIACustomWrapper]: ...
@overload
def iter_children(
    parent: WindowSpecification
    | UIAMenuWrapper
    | WindowSpecification
    | UIAMenuItemWrapper,
) -> Generator[WindowSpecification | UIAMenuItemWrapper, None, None]: ...
@overload
def iter_children(parent: UiaElement) -> Generator[UiaElement]: ...


def iter_children(parent):
    return parent.iter_children()


def children(
    parent: WindowSpecification | ListViewWrapper,
) -> list[WindowSpecification | ListItemWrapper]:
    return parent.children()


def _wait_for(
    condition: Callable[[], bool], timeout: float, interval: float
) -> bool:
    start = time()
    while not condition():
        if time() - start > timeout:
            return False
        sleep(interval)
    return True


def wait(
    element: UiaElement,
    wait_for: Literal["is_enabled"],
    timeout: float = 10.0,
    interval: float = 0.1,
) -> bool:
    method = getattr(element, wait_for)
    return _wait_for(lambda: method(), timeout=timeout, interval=interval)


def menu_select_1c(
    win: WindowSpecification, parent_element: UiaElement, trigger_btn_name: str
) -> None:
    click(win, child(parent_element, ctrl="Button", title=trigger_btn_name))
    menu = child(win, ctrl="Menu")

    item_selected = False
    for ch in iter_children(menu):
        if not ch.is_enabled():
            continue

        click(win, ch)

        for item in iter_children(menu):
            if not item.is_enabled():
                continue
            click(win, item)
            item_selected = True
            break

        if item_selected:
            break


if __debug__:

    def outline(element: UiaElement) -> None:
        focus(element)
        element.draw_outline()

    def click(
        main_win: WindowSpecification,
        element: UiaElement,
        button: Literal["left", "right", "middle"] = "left",
        double: bool = False,
    ) -> None:
        focus(main_win)
        element.click_input(button=button, double=double)

    def send_keys(
        win: WindowSpecification,
        keystrokes: str,
        pause: float = 0.05,
        spaces: bool = False,
    ) -> None:
        focus(win)
        keyboard.send_keys(keystrokes, pause=pause, with_spaces=spaces)

    def click_type(
        win: WindowSpecification,
        element: UiaElement,
        keystrokes: str,
        delay: float = 0.1,
        pause: float = 0.05,
        double: bool = False,
        cls: bool = True,
        ent: bool = False,
        spaces: bool = False,
        escape_chars: bool = False,
        coords: tuple[int, int] | None = None,
    ) -> None:
        focus(win)

        if escape_chars:
            keystrokes = (
                keystrokes.replace("\n", "{ENTER}")
                .replace("(", "{(}")
                .replace(")", "{)}")
            )

        if cls:
            keystrokes = "{DELETE}" + keystrokes

        if ent:
            keystrokes = keystrokes + "{ENTER}+{TAB}"

        if not coords:
            coords = (None, None)

        if double:
            element.double_click_input(coords=coords)
        else:
            element.click_input(coords=coords)
        sleep(delay)
        keyboard.send_keys(keystrokes, pause=pause, with_spaces=spaces)
else:

    def click(
        _: WindowSpecification,
        element: UiaElement,
        button: Literal["left", "right", "middle"] = "left",
        double: bool = False,
    ) -> None:
        element.click_input(button=button, double=double)

    def send_keys(
        _: WindowSpecification,
        keystrokes: str,
        pause: float = 0.05,
        spaces: bool = False,
    ) -> None:
        keyboard.send_keys(keystrokes, pause=pause, with_spaces=spaces)

    def click_type(
        _: WindowSpecification,
        element: UiaElement,
        keystrokes: str,
        delay: float = 0.1,
        pause: float = 0.05,
        double: bool = False,
        cls: bool = True,
        ent: bool = False,
        spaces: bool = False,
        escape_chars: bool = False,
        coords: tuple[int, int] | None = None,
    ) -> None:
        if escape_chars:
            keystrokes = (
                keystrokes.replace("\n", "{ENTER}")
                .replace("(", "{(}")
                .replace(")", "{)}")
            )

        if cls:
            keystrokes = "{DELETE}" + keystrokes

        if ent:
            keystrokes = keystrokes + "{ENTER}+{TAB}"

        if not coords:
            coords = (None, None)

        if double:
            element.double_click_input(coords=coords)
        else:
            element.click_input(coords=coords)
        sleep(delay)
        keyboard.send_keys(keystrokes, pause=pause, with_spaces=spaces)


def check(checkbox: WindowSpecification | CheckBoxWrapper) -> None:
    if checkbox.get_toggle_state() == 0:
        checkbox.toggle()


def exists(element: UiaElement) -> bool:
    return cast("WindowSpecification", element).exists()


def contains_text(element: UiaElement) -> bool:
    return any((inner.strip() for outer in element.texts() for inner in outer))


def text(element: UiaElement) -> str:
    return cast(str, element.window_text())


def text_to_float(txt: str, default: float | None = None) -> float:
    try:
        res = float(txt.replace(",", "."))
        return res
    except ValueError as err:
        if isinstance(default, float):
            return default
        raise err


def get_full_text(element: UiaElement) -> str:
    txt = element.window_text().strip() if element.window_text() else ""

    for ch in element.children():
        child_text = get_full_text(ch)
        if child_text:
            txt += " " + child_text

    return txt.strip()


def count_control_types(
    parent: UiaElement,
    ctrl: Literal[
        "Button",
        "CheckBox",
        "Custom",
        "Document",
        "Edit",
        "List",
        "ListItem",
        "Pane",
        "TabItem",
        "Table",
    ],
) -> int:
    count = 1 if parent.friendly_class_name() == ctrl else 0

    for ch in iter_children(parent):
        count += count_control_types(parent=ch, ctrl=ctrl)

    return count


def _print_element_tree(
    element: UiaElement,
    max_depth: int | None = None,
    counters: dict[str, int] | None = None,
    depth: int = 0,
) -> None:
    if counters is None:
        counters = {}

    element_ctrl = element.friendly_class_name()
    counters[element_ctrl] = counters.get(element_ctrl, 0) + 1
    element_idx = counters[element_ctrl] - 1

    element_repr = (
        "▏   " * (depth + 1)
        + f"{element_ctrl}{element_idx} - {text(element)!r} - "
    )

    try:
        element_repr += f"{element.rectangle()}"
        print(element_repr)
    except COMError:
        element_repr += "(COMError)"
        print(element_repr)
        return

    if max_depth is None or depth < max_depth:
        for ch in iter_children(element):
            _print_element_tree(ch, max_depth, counters, depth + 1)


def print_element_tree(
    element: UiaElement, max_depth: int | None = None
) -> None:
    """
    :param element: UiaElement - Root element of the tree
    :param max_depth: Optional[int} = None - Max depth of the tree to print
    :return: None
    """

    if max_depth is not None:
        if not isinstance(max_depth, int) or max_depth < 0:
            raise ValueError("max_depth must be a non-negative integer or None")

    _print_element_tree(element=element, max_depth=max_depth)


def switch_backend(backend: Literal["uia", "win32"]) -> Application:
    return Application(backend=backend).connect(path=os.environ["ONE_C_PATH"])
