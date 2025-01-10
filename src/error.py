import logging
import os
import traceback
from functools import wraps
from time import sleep
from typing import Any, Callable, Optional, Tuple, Type, TypeVar

from PIL import Image, ImageGrab


class HTMLElementNotFound(Exception):
    pass


class LoginError(Exception):
    pass


F = TypeVar("F", bound=Callable[..., Any])


def retry(
    exceptions: Tuple[Type[Exception], ...],
    tries: int = 3,
    delay: float = 1.0,
    backoff: float = 1.0,
) -> Callable[[F], F]:
    def decorator(func: F) -> F:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            nonlocal tries
            remaining_tries = tries
            current_delay = delay

            while remaining_tries > 1:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    logging.warning(
                        f"Retrying {func.__name__} due to {e!r}. "
                        f"Attempts left: {remaining_tries - 1}"
                    )
                    sleep(current_delay)
                    current_delay *= backoff
                    remaining_tries -= 1

            return func(*args, **kwargs)

        return wrapper

    return decorator


class TelegramAPI:
    def __init__(self) -> None: ...

    def reload_session(self) -> None: ...

    def send_message(
        self,
        message: str | None = None,
        media: Image.Image | None = None,
        use_session: bool = True,
        use_md: bool = False,
    ) -> bool: ...

    def send_image(
        self, media: Image.Image | None = None, use_session: bool = True
    ) -> bool: ...


def handle_error(func: Callable[..., any]) -> Callable[..., any]:
    @wraps(func)
    def wrapper(*args, **kwargs) -> any:
        bot: Optional[TelegramAPI] = kwargs.get("bot")

        try:
            return func(*args, **kwargs)
        except KeyboardInterrupt as error:
            raise error
        except (Exception, BaseException) as error:
            logging.exception(error)
            error_traceback = traceback.format_exc()

            developer = os.getenv("DEVELOPER")
            if developer:
                error_traceback = f"@{developer} {error_traceback}"

            if bot:
                bot.send_message(message=error_traceback, media=ImageGrab.grab())
            raise error

    return wrapper
