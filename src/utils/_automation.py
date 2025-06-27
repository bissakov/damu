from collections.abc import Callable, Generator
from time import sleep, time
from typing import Any, Literal, NewType, TypeAlias, TypeVar, cast, overload

from _ctypes import COMError
from pywinauto import Application, WindowSpecification, keyboard
from pywinauto.controls.uia_controls import (
    ButtonWrapper,
    EditWrapper,
    ListItemWrapper,
    ListViewWrapper,
    MenuItemWrapper,
    MenuWrapper,
    ToolbarWrapper,
)
from pywinauto.controls.uiawrapper import UIAWrapper

_WindowWindowSpecification = NewType(
    "_WindowWindowSpecification", WindowSpecification
)
_ButtonWindowSpecification = NewType(
    "_ButtonWindowSpecification", WindowSpecification
)
_CheckBoxWindowSpecification = NewType(
    "_CheckBoxWindowSpecification", WindowSpecification
)
_CustomWindowSpecification = NewType(
    "_CustomWindowSpecification", WindowSpecification
)
_DocumentWindowSpecification = NewType(
    "_DocumentWindowSpecification", WindowSpecification
)
_EditWindowSpecification = NewType(
    "_EditWindowSpecification", WindowSpecification
)
_ListWindowSpecification = NewType(
    "_ListWindowSpecification", WindowSpecification
)
_ListItemWindowSpecification = NewType(
    "_ListItemWindowSpecification", WindowSpecification
)
_PaneWindowSpecification = NewType(
    "_PaneWindowSpecification", WindowSpecification
)
_TabItemWindowSpecification = NewType(
    "_TabItemWindowSpecification", WindowSpecification
)
_TableWindowSpecification = NewType(
    "_TableWindowSpecification", WindowSpecification
)
_MenuWindowSpecification = NewType(
    "_MenuWindowSpecification", WindowSpecification
)
_MenuItemWindowSpecification = NewType(
    "_MenuItemWindowSpecification", WindowSpecification
)
_ToolbarWindowSpecification = NewType(
    "_ToolbarWindowSpecification", WindowSpecification
)

_UIAWrapper = NewType("_UIAWrapper", UIAWrapper)
_ButtonWrapper = NewType("_ButtonWrapper", ButtonWrapper)
_CheckBoxWrapper = NewType("_CheckBoxWrapper", ButtonWrapper)
_UIACustomWrapper = NewType("_UIACustomWrapper", UIAWrapper)
_UIADocumentWrapper = NewType("_UIADocumentWrapper", UIAWrapper)
_EditWrapper = NewType("_EditWrapper", EditWrapper)
_ListViewWrapper = NewType("_ListViewWrapper", ListViewWrapper)
_ListItemWrapper = NewType("_ListItemWrapper", ListItemWrapper)
_UIAPaneWrapper = NewType("_UIAPaneWrapper", UIAWrapper)
_UIATabItemWrapper = NewType("_UIATabItemWrapper", UIAWrapper)
_UIATableWrapper = NewType("_UIATableWrapper", ListViewWrapper)
_UIAMenuWrapper = NewType("_UIAMenuWrapper", MenuWrapper)
_UIAMenuItemWrapper = NewType("_UIAMenuItemWrapper", MenuItemWrapper)
_UIAToolbarWrapper = NewType("_UIAToolbarWrapper", ToolbarWrapper)


UiaWindow = _WindowWindowSpecification | _UIAWrapper
UiaButton = _ButtonWindowSpecification | _ButtonWrapper
UiaCheckBox = _CheckBoxWindowSpecification | _CheckBoxWrapper
UiaCustom = _CustomWindowSpecification | _UIACustomWrapper
UiaDocument = _DocumentWindowSpecification | _UIADocumentWrapper
UiaEdit = _EditWindowSpecification | _EditWrapper
UiaList = _ListWindowSpecification | _ListViewWrapper
UiaListItem = _ListItemWindowSpecification | _ListItemWrapper
UiaPane = _PaneWindowSpecification | _UIAPaneWrapper
UiaTabItem = _TabItemWindowSpecification | _UIATabItemWrapper
UiaTable = _TableWindowSpecification | _UIATableWrapper
UiaMenu = _MenuWindowSpecification | _UIAMenuWrapper
UiaMenuItem = _MenuItemWindowSpecification | _UIAMenuItemWrapper
UiaToolbar = _ToolbarWindowSpecification | _UIAToolbarWrapper

UiaElement: TypeAlias = (
    WindowSpecification
    | _UIAWrapper
    | _ButtonWrapper
    | _CheckBoxWrapper
    | _UIACustomWrapper
    | _UIADocumentWrapper
    | _EditWrapper
    | _ListViewWrapper
    | _ListItemWrapper
    | _UIAPaneWrapper
    | _UIATabItemWrapper
    | _UIATableWrapper
    | _UIAMenuWrapper
    | _UIAMenuItemWrapper
    | _UIAToolbarWrapper
)


@overload
def child(
    parent: UiaElement,
    ctrl: Literal["Button"],
    title: str | None = None,
    idx: int = 0,
) -> _ButtonWindowSpecification | _ButtonWrapper: ...
@overload
def child(
    parent: UiaElement,
    ctrl: Literal["CheckBox"],
    title: str | None = None,
    idx: int = 0,
) -> _CheckBoxWindowSpecification | _CheckBoxWrapper: ...
@overload
def child(
    parent: UiaElement,
    ctrl: Literal["Custom"],
    title: str | None = None,
    idx: int = 0,
) -> _CustomWindowSpecification | _UIACustomWrapper: ...
@overload
def child(
    parent: UiaElement,
    ctrl: Literal["Document"],
    title: str | None = None,
    idx: int = 0,
) -> _DocumentWindowSpecification | _UIADocumentWrapper: ...
@overload
def child(
    parent: UiaElement,
    ctrl: Literal["Edit"],
    title: str | None = None,
    idx: int = 0,
) -> _EditWindowSpecification | _EditWrapper: ...
@overload
def child(
    parent: UiaElement,
    ctrl: Literal["List"],
    title: str | None = None,
    idx: int = 0,
) -> _ListWindowSpecification | _ListViewWrapper: ...
@overload
def child(
    parent: UiaElement,
    ctrl: Literal["ListItem"],
    title: str | None = None,
    idx: int = 0,
) -> _ListItemWindowSpecification | _ListItemWrapper: ...
@overload
def child(
    parent: UiaElement,
    ctrl: Literal["Pane"],
    title: str | None = None,
    idx: int = 0,
) -> _PaneWindowSpecification | _UIAPaneWrapper: ...
@overload
def child(
    parent: UiaElement,
    ctrl: Literal["TabItem"],
    title: str | None = None,
    idx: int = 0,
) -> _TabItemWindowSpecification | _UIATabItemWrapper: ...
@overload
def child(
    parent: UiaElement,
    ctrl: Literal["Table"],
    title: str | None = None,
    idx: int = 0,
) -> _TableWindowSpecification | _UIATableWrapper: ...
@overload
def child(
    parent: UiaElement,
    ctrl: Literal["Menu"],
    title: str | None = None,
    idx: int = 0,
) -> _MenuWindowSpecification | _UIAMenuWrapper: ...
@overload
def child(
    parent: UiaElement,
    ctrl: Literal["MenuItem"],
    title: str | None = None,
    idx: int = 0,
) -> _MenuItemWindowSpecification | _UIAMenuItemWrapper: ...
@overload
def child(
    parent: UiaElement,
    ctrl: Literal["ToolBar"],
    title: str | None = None,
    idx: int = 0,
) -> _ToolbarWindowSpecification | _UIAToolbarWrapper: ...


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
        "TabItem",
        "Table",
        "Menu",
        "MenuItem",
        "ToolBar",
    ],
    title: str | None = None,
    idx: int = 0,
) -> Any:
    return cast(WindowSpecification, parent).child_window(
        title=title, control_type=ctrl, found_index=idx
    )


def window(
    app: Application, title: str, regex: bool = False
) -> WindowSpecification:
    if regex:
        return cast(WindowSpecification, app.window(title_re=title))
    else:
        return cast(WindowSpecification, app.window(title=title))


def focus(element: UiaElement) -> None:
    if not element.is_active():
        element.set_focus()
        cast(WindowSpecification, element).wait(wait_for="active visible")


def a(main_win: WindowSpecification, action: Callable[[], None]) -> None:
    focus(main_win)
    action()


def click(
    main_win: WindowSpecification,
    element: UiaElement,
    button: Literal["left", "right", "middle"] = "left",
    double: bool = False,
) -> None:
    focus(main_win)
    element.click_input(button=button, double=double)


def _click(element: UiaElement, double: bool = False) -> None:
    focus(element)
    element.click_input(double=double)


@overload
def iter_children(
    parent: _ListWindowSpecification | _ListViewWrapper,
) -> Generator[_ListItemWindowSpecification | _ListItemWrapper]: ...
@overload
def iter_children(
    parent: _TableWindowSpecification | _UIATableWrapper,
) -> Generator[_CustomWindowSpecification | _UIACustomWrapper]: ...
@overload
def iter_children(
    parent: _MenuWindowSpecification
    | _UIAMenuWrapper
    | _MenuItemWindowSpecification
    | _UIAMenuItemWrapper,
) -> Generator[
    _MenuItemWindowSpecification | _UIAMenuItemWrapper, None, None
]: ...
@overload
def iter_children(parent: UiaElement) -> Generator[UiaElement]: ...


def iter_children(parent):
    return parent.iter_children()


def children(
    parent: _ListWindowSpecification | _ListViewWrapper,
) -> list[_ListItemWindowSpecification | _ListItemWrapper]:
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


def menu_select(
    menu: _MenuWindowSpecification | _UIAMenuWrapper, menu_names: list[str]
) -> None:
    for menu_name in menu_names:
        child(menu, ctrl="MenuItem", title=menu_name).click_input()


def menu_select_1c(
    win: WindowSpecification,
    parent_element: UiaElement,
    trigger_btn_name: str,
    menu_names: list[str],
) -> None:
    click(win, child(parent_element, ctrl="Button", title=trigger_btn_name))
    menu = child(win, ctrl="Menu")
    menu_select(menu, menu_names)


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

    if double:
        element.double_click_input()
    else:
        element.click_input()
    sleep(delay)
    keyboard.send_keys(keystrokes, pause=pause, with_spaces=spaces)


def check(checkbox: _CheckBoxWindowSpecification | _CheckBoxWrapper) -> None:
    if checkbox.get_toggle_state() == 0:
        checkbox.toggle()


def exists(element: UiaElement) -> bool:
    return cast(WindowSpecification, element).exists()


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


def outline(element: UiaElement) -> None:
    focus(element)
    element.draw_outline()
