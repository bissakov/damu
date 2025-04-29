import logging
import os
import traceback
from functools import wraps
from pathlib import Path
from time import sleep
from typing import Any, Callable, Optional, Tuple, Type, TypeVar

from PIL import Image, ImageGrab

logger = logging.getLogger("DAMU")


class ReplyError(Exception): ...


class CRMNotFoundError(Exception): ...


class VypiskaDownloadError(Exception): ...


class HTMLElementNotFound(Exception): ...


class LoginError(Exception): ...


class ParseError(Exception): ...


class ContractsNofFoundError(Exception): ...


class MismatchError(ParseError): ...


class InterestRateMismatchError(ParseError): ...


class InvalidColumnCount(ParseError): ...


class DateConversionError(ParseError): ...


class BankNotSupportedError(ParseError): ...


class DataFrameInequalityError(ParseError): ...


class JoinPDFNotFoundError(ParseError): ...


class JoinProtocolNotFoundError(ParseError): ...


class DateNotFoundError(ParseError):
    def __init__(self, file_name: str, contract_id: str, para: str = "") -> None:
        self.file_name = file_name
        self.contract_id = contract_id
        self.message = f"No dates in {file_name!r} of a {contract_id!r} for {para}..."
        super().__init__(self.message)


class TableNotFound(ParseError):
    def __init__(self, file_name: str, contract_id: str, target: str) -> None:
        self.file_name = file_name
        self.contract_id = contract_id
        self.message = f"No tables in {file_name!r} of a {contract_id!r} for {target}..."
        super().__init__(self.message)


class ExcesssiveTableCountError(ParseError):
    def __init__(self, file_name: str, contract_id: str, table_count: int) -> None:
        self.file_name = file_name
        self.contract_id = contract_id
        self.table_count = table_count
        self.message = (
            f"Expecting to parse 1 or 2 tables, got "
            f"{table_count} in {file_name!r} of a {contract_id!r}..."
        )
        super().__init__(self.message)


def format_error(err: Exception) -> str:
    stack = traceback.extract_stack(limit=2)[0]
    filename = Path(stack.filename).name
    line_number = stack.lineno

    return f"{err.__class__.__name__}({err} {filename}:{line_number})\n"


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
                    logger.warning(
                        f"Retrying {func.__name__} due to {e!r}. "
                        f"Attempts left: {remaining_tries - 1}"
                    )
                    sleep(current_delay)
                    current_delay *= backoff
                    remaining_tries -= 1

            return func(*args, **kwargs)

        return wrapper

    return decorator


def async_retry(
    exceptions: Tuple[Type[Exception], ...],
    tries: int = 3,
    delay: float = 1.0,
    backoff: float = 1.0,
) -> Callable[[F], F]:
    def decorator(func: F) -> F:
        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            nonlocal tries
            remaining_tries = tries
            current_delay = delay

            while remaining_tries > 1:
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    logger.warning(
                        f"Retrying {func.__name__} due to {e!r}. "
                        f"Attempts left: {remaining_tries - 1}"
                    )
                    sleep(current_delay)
                    current_delay *= backoff
                    remaining_tries -= 1

            return await func(*args, **kwargs)

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

    def send_image(self, media: Image.Image | None = None, use_session: bool = True) -> bool: ...


def handle_error(func: Callable[..., any]) -> Callable[..., any]:
    @wraps(func)
    def wrapper(*args, **kwargs) -> any:
        bot: Optional[TelegramAPI] = kwargs.get("bot")

        try:
            return func(*args, **kwargs)
        except KeyboardInterrupt as error:
            raise error
        except (Exception, BaseException) as error:
            logger.exception(error)
            error_traceback = traceback.format_exc()

            developer = os.getenv("DEVELOPER")
            if developer:
                error_traceback = f"@{developer} {error_traceback}"

            if bot:
                bot.send_message(message=error_traceback, media=ImageGrab.grab())
            raise error

    return wrapper
