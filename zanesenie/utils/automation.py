from contextlib import suppress
from time import time, sleep
from typing import (
    overload,
    Literal,
    Optional,
    Callable,
    cast,
    Union,
    Generator,
    List,
    TypeVar,
    NewType,
    Dict,
)

from _ctypes import COMError
from pywinauto import WindowSpecification, Application
from pywinauto import keyboard
from pywinauto.controls.uia_controls import (
    ButtonWrapper,
    EditWrapper,
    ListViewWrapper,
    ListItemWrapper,
    MenuWrapper,
    MenuItemWrapper,
    ToolbarWrapper,
)
from pywinauto.controls.uiawrapper import UIAWrapper

_WindowWindowSpecification = NewType("_WindowWindowSpecification", WindowSpecification)
_ButtonWindowSpecification = NewType("_ButtonWindowSpecification", WindowSpecification)
_CheckBoxWindowSpecification = NewType("_CheckBoxWindowSpecification", WindowSpecification)
_CustomWindowSpecification = NewType("_CustomWindowSpecification", WindowSpecification)
_DocumentWindowSpecification = NewType("_DocumentWindowSpecification", WindowSpecification)
_EditWindowSpecification = NewType("_EditWindowSpecification", WindowSpecification)
_ListWindowSpecification = NewType("_ListWindowSpecification", WindowSpecification)
_ListItemWindowSpecification = NewType("_ListItemWindowSpecification", WindowSpecification)
_PaneWindowSpecification = NewType("_PaneWindowSpecification", WindowSpecification)
_TabItemWindowSpecification = NewType("_TabItemWindowSpecification", WindowSpecification)
_TableWindowSpecification = NewType("_TableWindowSpecification", WindowSpecification)
_MenuWindowSpecification = NewType("_MenuWindowSpecification", WindowSpecification)
_MenuItemWindowSpecification = NewType("_MenuItemWindowSpecification", WindowSpecification)
_ToolbarWindowSpecification = NewType("_ToolbarWindowSpecification", WindowSpecification)

_UIAWrapper = NewType("_UIAWrapper", UIAWrapper)
_ButtonWrapper = NewType("_ButtonWrapper", ButtonWrapper)
_CheckBoxWrapper = NewType("_CheckBoxWrapper", ButtonWrapper)
_UIACustomWrapper = NewType("_UIACustomWrapper", UIAWrapper)
_UIADocumentWrapper = NewType("_UIADocumentWrapper", UIAWrapper)
_EditWrapper = NewType("_EditWrapper", EditWrapper)
_ListViewWrapper = NewType("_ListViewWrapper", ListViewWrapper)
_ListItemWrapper = NewType("_ListItemWrapper", ListItemWrapper)
_UIAPaneWrapper = NewType("_UIAWPanerapper", UIAWrapper)
_UIATabItemWrapper = NewType("_UIATabItemWrapper", UIAWrapper)
_UIATableWrapper = NewType("_UIATableWrapper", ListViewWrapper)
_UIAMenuWrapper = NewType("_UIAMenuWrapper", MenuWrapper)
_UIAMenuItemWrapper = NewType("_UIAMenuItemWrapper", MenuItemWrapper)
_UIAToolbarWrapper = NewType("_UIAToolbarWrapper", ToolbarWrapper)


UiaWindow = Union[_WindowWindowSpecification, _UIAWrapper]
UiaButton = Union[_ButtonWindowSpecification, _ButtonWrapper]
UiaCheckBox = Union[_CheckBoxWindowSpecification, _CheckBoxWrapper]
UiaCustom = Union[_CustomWindowSpecification, _UIACustomWrapper]
UiaDocument = Union[_DocumentWindowSpecification, _UIADocumentWrapper]
UiaEdit = Union[_EditWindowSpecification, _EditWrapper]
UiaList = Union[_ListWindowSpecification, _ListViewWrapper]
UiaListItem = Union[_ListItemWindowSpecification, _ListItemWrapper]
UiaPane = Union[_PaneWindowSpecification, _UIAPaneWrapper]
UiaTabItem = Union[_TabItemWindowSpecification, _UIATabItemWrapper]
UiaTable = Union[_TableWindowSpecification, _UIATableWrapper]
UiaMenu = Union[_MenuWindowSpecification, _UIAMenuWrapper]
UiaMenuItem = Union[_MenuItemWindowSpecification, _UIAMenuItemWrapper]
UiaToolbar = Union[_ToolbarWindowSpecification, _UIAToolbarWrapper]

UiaElement = TypeVar(
    "UiaElement",
    UiaWindow,
    UiaButton,
    UiaCheckBox,
    UiaCustom,
    UiaDocument,
    UiaEdit,
    UiaList,
    UiaListItem,
    UiaPane,
    UiaTabItem,
    UiaTable,
    UiaMenu,
    UiaMenuItem,
)


@overload
def child(parent: UiaElement, ctrl: Literal["Button"], title: Optional[str] = None, idx: int = 0) -> UiaButton: ...
@overload
def child(parent: UiaElement, ctrl: Literal["CheckBox"], title: Optional[str] = None, idx: int = 0) -> UiaCheckBox: ...
@overload
def child(parent: UiaElement, ctrl: Literal["Custom"], title: Optional[str] = None, idx: int = 0) -> UiaCustom: ...
@overload
def child(parent: UiaElement, ctrl: Literal["Document"], title: Optional[str] = None, idx: int = 0) -> UiaDocument: ...
@overload
def child(parent: UiaElement, ctrl: Literal["Edit"], title: Optional[str] = None, idx: int = 0) -> UiaEdit: ...
@overload
def child(parent: UiaElement, ctrl: Literal["List"], title: Optional[str] = None, idx: int = 0) -> UiaList: ...
@overload
def child(parent: UiaElement, ctrl: Literal["ListItem"], title: Optional[str] = None, idx: int = 0) -> UiaListItem: ...
@overload
def child(parent: UiaElement, ctrl: Literal["Pane"], title: Optional[str] = None, idx: int = 0) -> UiaPane: ...
@overload
def child(parent: UiaElement, ctrl: Literal["TabItem"], title: Optional[str] = None, idx: int = 0) -> UiaTabItem: ...
@overload
def child(parent: UiaElement, ctrl: Literal["Table"], title: Optional[str] = None, idx: int = 0) -> UiaTable: ...
@overload
def child(parent: UiaElement, ctrl: Literal["Menu"], title: Optional[str] = None, idx: int = 0) -> UiaMenu: ...
@overload
def child(parent: UiaElement, ctrl: Literal["MenuItem"], title: Optional[str] = None, idx: int = 0) -> UiaMenuItem: ...
@overload
def child(parent: UiaElement, ctrl: Literal["ToolBar"], title: Optional[str] = None, idx: int = 0) -> UiaToolbar: ...


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
    title: Optional[str] = None,
    idx: int = 0,
):
    return parent.child_window(title=title, control_type=ctrl, found_index=idx)


def window(app: Application, title: str, regex: bool = False) -> UiaWindow:
    if regex:
        return app.window(title_re=title)
    else:
        return app.window(title=title)


def focus(element: UiaElement) -> None:
    if not element.is_active():
        element.set_focus()
        element.wait(wait_for="active visible")


def a(main_win: UiaWindow, action: Callable[[], None]) -> None:
    focus(main_win)
    action()


def click(main_win: UiaWindow, element: UiaElement, double: bool = False) -> None:
    focus(main_win)
    element.click_input(double=double)


def _click(element: UiaElement, double: bool = False) -> None:
    focus(element)
    element.click_input(double=double)


@overload
def iter_children(parent: UiaList) -> Generator[UiaListItem, None, None]: ...
@overload
def iter_children(parent: UiaTable) -> Generator[UiaCustom, None, None]: ...
@overload
def iter_children(parent: Union[UiaMenu, UiaMenuItem]) -> Generator[UiaMenuItem, None, None]: ...
@overload
def iter_children(parent: UiaElement) -> Generator[UiaElement, None, None]: ...


def iter_children(parent: UiaElement):
    return parent.iter_children()


def children(parent: UiaList) -> List[UiaListItem]:
    return parent.children()


def _wait_for(condition: Callable[[], bool], timeout: float, interval: float) -> bool:
    start = time()
    while not condition():
        if time() - start > timeout:
            return False
        sleep(interval)
    return True


def wait(element: UiaElement, wait_for: Literal["is_enabled"], timeout: float = 10.0, interval: float = 0.1) -> bool:
    method = getattr(element, wait_for)
    return _wait_for(lambda: method(), timeout=timeout, interval=interval)


def menu_select(menu: UiaMenu, menu_names: List[str]) -> None:
    for menu_name in menu_names:
        child(menu, ctrl="MenuItem", title=menu_name).click_input()


def menu_select_1c(win: UiaWindow, parent_element: UiaElement, trigger_btn_name: str, menu_names: List[str]) -> None:
    click(win, child(parent_element, ctrl="Button", title=trigger_btn_name))
    menu = child(win, ctrl="Menu")
    menu_select(menu, menu_names)


def send_keys(win: UiaWindow, keystrokes: str, pause: float = 0.05, spaces: bool = False) -> None:
    focus(win)
    keyboard.send_keys(keystrokes, pause=pause, with_spaces=spaces)


def click_type(
    win: UiaWindow,
    element: UiaElement,
    keystrokes: Union[str, float],
    delay: float = 0.1,
    pause: float = 0.05,
    double: bool = False,
    cls: bool = True,
    ent: bool = False,
    spaces: bool = False,
    escape_chars: bool = False,
) -> None:
    focus(win)

    if not isinstance(keystrokes, str):
        keystrokes = cast(str, str(keystrokes))

    if escape_chars:
        keystrokes = keystrokes.replace("\n", "{ENTER}").replace("(", "{(}").replace(")", "{)}")

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


def check(checkbox: UiaCheckBox) -> None:
    if checkbox.get_toggle_state() == 0:
        checkbox.toggle()


def contains_text(element: UiaElement) -> bool:
    return any((inner.strip() for outer in element.texts() for inner in outer))


def text(element: UiaElement) -> str:
    return cast(str, element.window_text())


def text_to_float(txt: str, default: Optional[float] = None) -> float:
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
    ctrl: Literal["Button", "CheckBox", "Custom", "Document", "Edit", "List", "ListItem", "Pane", "TabItem", "Table"],
) -> int:
    count = 1 if parent.friendly_class_name() == ctrl else 0

    for ch in iter_children(parent):
        count += count_control_types(parent=ch, ctrl=ctrl)

    return count


def _print_element_tree(
    element: UiaElement, max_depth: Optional[int] = None, counters: Optional[Dict[str, int]] = None, depth: int = 0
) -> None:
    if counters is None:
        counters = {}

    element_ctrl = element.friendly_class_name()
    counters[element_ctrl] = counters.get(element_ctrl, 0) + 1
    element_idx = counters[element_ctrl] - 1

    element_repr = "▏   " * (depth + 1) + f"{element_ctrl}{element_idx} - {text(element)!r} - "

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


def print_element_tree(element: UiaElement, max_depth: Optional[int] = None) -> None:
    """
    :param element: UiaElement - Root element of the tree
    :param max_depth: Optional[int} = None - Max depth of the tree to print
    :return: None
    """

    if max_depth is not None:
        if not isinstance(max_depth, int) or max_depth < 0:
            raise ValueError("max_depth must be a non-negative integer or None")

    _print_element_tree(element=element, max_depth=max_depth)


def sub_windows(element: UiaElement, depth: int = 0):
    if element.friendly_class_name() not in ["Dialog", "Pane"]:
        return

    try:
        _children = list(element.children())
    except Exception:
        return

    for ch in iter_children(element):
        # toolbar_found = False
        for gc in iter_children(ch):
            c_name = gc.friendly_class_name()
            title = text(gc)
            # print(c_name, title)
            if c_name == "Button" and title in ["Collapse", "Maximize", "Свернуть", "Восстановить"]:
                print(element)
                element.draw_outline()
                break

            # try:
            #     great_grandkids = list(gc.children())
            # except Exception:
            #     great_grandkids = []
            #
            # for ggc in great_grandkids:
            #     if not (ggc.friendly_class_name() == "Button" and text(ggc) in ["Collapse", "Maximize"]):
            #         continue
            #     print(ch)
            #     ch.draw_outline()
            #     toolbar_found = True
            #     break
            #
            # if toolbar_found:
            #     break

        sub_windows(ch, depth + 1)


def outline(element: UiaElement) -> None:
    focus(element)
    element.draw_outline()
