import logging
from datetime import date, datetime
from pathlib import Path

from rich.console import Console
from rich.highlighter import ReprHighlighter
from rich.logging import RichHandler
from rich.text import Text


class CustomHighlighter(ReprHighlighter):
    def __init__(self) -> None:
        self.keywords = {
            "SUCCESS": "chartreuse1",
            "FAILURE": "red3",
            "WARNING": "yellow",
        }

    def highlight(self, text: Text) -> None:
        super().highlight(text)
        for keyword, color in self.keywords.items():
            if keyword in text:
                text.highlight_words([keyword], style=color)


def setup_logger(project_root: Path, today: date) -> Path:
    logging.Formatter.converter = lambda *args: datetime.now().timetuple()

    log_folder = project_root / "logs"
    log_folder.mkdir(exist_ok=True)

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    log_format = (
        "[%(asctime)s] %(levelname)-7s %(filename)s:%(funcName)s:%(lineno)s %(message)s"
    )
    formatter = logging.Formatter(log_format, datefmt="%H:%M:%S")

    today_str = today.strftime("%d.%m.%y")
    year_month_folder = log_folder / today.strftime("%Y/%B")
    year_month_folder.mkdir(parents=True, exist_ok=True)

    logger_file = year_month_folder / f"{today_str}.log"

    file_handler = logging.FileHandler(logger_file, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)

    logger.addHandler(file_handler)

    rich_console = Console(
        soft_wrap=False,
        tab_size=2,
    )
    highlighter = CustomHighlighter()
    rich_handler = RichHandler(
        console=rich_console,
        omit_repeated_times=False,
        rich_tracebacks=False,
        log_time_format="[%H:%M:%S]",
        highlighter=highlighter,
    )

    logger.addHandler(rich_handler)
    return logger_file
