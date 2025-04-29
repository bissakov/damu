import logging
from datetime import date, datetime
from pathlib import Path
from typing import Optional

from rich.highlighter import ReprHighlighter
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


def setup_logger(today: Optional[date] = None) -> Path:
    log_format = "[%(asctime)s] %(levelname)-8s %(filename)s:%(funcName)s:%(lineno)s %(message)s"
    formatter = logging.Formatter(log_format, datefmt="%H:%M:%S")

    root = logging.getLogger("DAMU")
    root.setLevel(logging.DEBUG)

    logging.Formatter.converter = lambda *args: datetime.now().timetuple()

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.DEBUG)
    stream_handler.setFormatter(formatter)

    log_folder = Path("logs")
    log_folder.mkdir(exist_ok=True)

    if today is None:
        today = date.today()

    today_str = today.strftime("%d.%m.%y")
    year_month_folder = log_folder / today.strftime("%Y/%B")
    year_month_folder.mkdir(parents=True, exist_ok=True)
    logger_file = year_month_folder / f"{today_str}.log"

    file_handler = logging.FileHandler(logger_file, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    root.addHandler(stream_handler)
    root.addHandler(file_handler)

    return logger_file
