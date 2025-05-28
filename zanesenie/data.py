import dataclasses
import logging
import os
from datetime import datetime
from enum import Enum, unique
from pathlib import Path
from typing import Iterator, NamedTuple, Union

env = os.getenv("ENV", "prod")

if env == "prod":

    @unique
    class JobType(Enum):
        BUSINESS_TRIP = 0
        VACATION = 1
        VACATION_WITHDRAW = 2
        FIRING = 3
        MENTORSHIP = 4
        VND = 5
        TRIP_ADD_PAY = 6
        VACATION_ADD_PAY = 7

        def __str__(self) -> str:
            return f"JobType(name={self.name}, value={self.value})"

        def __repr__(self) -> str:
            return str(self)

else:
    logging.error("Test environment for BPM is not set")
    raise NotImplementedError("Test environment for BPM is not set")


Order = Union[
    "BusinessTripOrder",
    "VacationOrder",
    "VacationWithdrawOrder",
    "FiringOrder",
    "MentorshipOrder",
    "VNDOrder",
    "TripAddPayOrder",
    "VacationAddPayOrder",
]


class Date(NamedTuple):
    dt: datetime
    long: str
    short: str

    @classmethod
    def to_date(cls, dt: datetime) -> "Date":
        return Date(dt, dt.strftime("%d.%m.%Y"), dt.strftime("%d.%m.%y"))

    def __str__(self) -> str:
        return f"Date<{self.short}>"

    def __repr__(self) -> str:
        return str(self)


class TimeRange(NamedTuple):
    start: Date
    end: Date


class Mail(NamedTuple):
    server: str
    sender: str
    recipients: str
    subject: str
    report_path: Path
    screenshot_folder: Path


class PathRegistry(NamedTuple):
    data_folder: Path
    report_folder: Path
    screenshot_folder: Path
    csv_path: Path
    pickle_path: Path
    report_path: Path
    log_path: Path


class Job(NamedTuple):
    job_type: JobType
    job_id: int
    order_cls: type[Order]
    order_type: str
    download_url: str
    registry: PathRegistry
    t_range: TimeRange
    mail_info: Mail


@dataclasses.dataclass(slots=True, frozen=True)
class Jobs:
    business_trip: Job
    vacation: Job
    vacation_withdraw: Job
    firing: Job
    mentorship: Job
    vnd: Job
    trip_add_pay: Job
    vacation_add_pay: Job

    def __iter__(self) -> Iterator[Job]:
        # noinspection PyTypeChecker
        for field in dataclasses.fields(self):
            yield getattr(self, field.name)


class ParseParams(NamedTuple):
    date_format: str
    blacklist: set[str]
    pattern: str
    statuses_to_skip: set[str]


def convert_date(obj: object, field_name: str, date_format: str) -> None:
    field_value = getattr(obj, field_name)
    if isinstance(field_value, Date):
        return
    elif isinstance(field_value, datetime):
        setattr(obj, field_name, Date.to_date(field_value))
    elif isinstance(field_value, str):
        date = datetime.strptime(field_value, date_format)
        setattr(obj, field_name, Date.to_date(date))
    else:
        error_msg = (
            f"Unknown type for a field {field_name} - {type(field_value)} {field_value}"
        )
        logging.error(error_msg)
        raise ValueError(error_msg)


@dataclasses.dataclass(slots=True)
class BusinessTripOrder:
    employee_fullname: str
    employee_names: tuple[str, str]
    order_number: str
    start_date: Date | str | datetime
    end_date: Date | str | datetime
    trip_place: str
    trip_code: str
    trip_reason: str
    main_order_start_date: Date | str | datetime
    was_done_previously: bool
    screenshot_path: str
    employee_status: str | None = None
    branch_num: str | None = None
    tab_num: str | None = None

    def __str__(self) -> str:
        return f"{self.employee_fullname} {self.order_number}"

    def __post_init__(self):
        convert_date(self, "start_date", "%d.%m.%y")
        convert_date(self, "end_date", "%d.%m.%y")
        convert_date(self, "main_order_start_date", "%d.%m.%y")


@dataclasses.dataclass(slots=True)
class VacationOrder:
    employee_fullname: str
    employee_names: tuple[str, str]
    order_type: str
    start_date: Date | str | datetime
    end_date: Date | str | datetime
    order_number: str
    was_done_previously: bool
    screenshot_path: str
    employee_status: str | None = None
    branch_num: str | None = None
    tab_num: str | None = None

    def __str__(self) -> str:
        return f"{self.employee_fullname} {self.order_number}"

    def __post_init__(self):
        convert_date(self, "start_date", "%d.%m.%y")
        convert_date(self, "end_date", "%d.%m.%y")


@dataclasses.dataclass(slots=True)
class VacationWithdrawOrder:
    employee_fullname: str
    employee_names: tuple[str, str]
    order_number: str
    withdraw_date: Date | str | datetime
    was_done_previously: bool
    screenshot_path: str
    employee_status: str | None = None
    branch_num: str | None = None
    tab_num: str | None = None

    def __str__(self) -> str:
        return f"{self.employee_fullname} {self.order_number}"

    def __post_init__(self):
        convert_date(self, "withdraw_date", "%d.%m.%y")


@dataclasses.dataclass(slots=True)
class FiringOrder:
    employee_fullname: str
    employee_names: tuple[str, str]
    order_number: str
    compensation: str
    firing_date: Date | str | datetime
    main_article: str
    extra_article: str
    was_done_previously: bool
    screenshot_path: str
    employee_status: str | None = None
    branch_num: str | None = None
    tab_num: str | None = None

    def __str__(self) -> str:
        return f"{self.employee_fullname} {self.order_number}"

    def __post_init__(self):
        convert_date(self, "firing_date", "%d.%m.%y")


@dataclasses.dataclass(slots=True)
class MentorshipOrder:
    mentee_fullname: str
    employee_fullname: str
    employee_names: tuple[str, str]
    start_date: Date | str | datetime
    end_date: Date | str | datetime
    order_number: str
    was_done_previously: bool
    screenshot_path: str
    employee_status: str | None = None
    branch_num: str | None = None
    tab_num: str | None = None

    def __str__(self) -> str:
        return f"{self.employee_fullname} {self.order_number}"

    def __post_init__(self):
        convert_date(self, "start_date", "%d.%m.%y")
        convert_date(self, "end_date", "%d.%m.%y")


@dataclasses.dataclass(slots=True)
class VNDOrder:
    employee_fullname: str
    employee_names: tuple[str, str]
    order_number: str
    doplata: str | None
    start_date: Date
    end_date: Date
    was_done_previously: bool
    screenshot_path: str
    employee_status: str | None = None
    branch_num: str | None = None
    tab_num: str | None = None

    def __str__(self) -> str:
        return f"{self.employee_fullname} {self.order_number}"

    def __post_init__(self):
        convert_date(self, "start_date", "%d.%m.%y")
        convert_date(self, "end_date", "%d.%m.%y")


@dataclasses.dataclass(slots=True)
class TripAddPayOrder:
    substitutee_fullname: str
    employee_fullname: str
    employee_names: tuple[str, str]
    order_number: str
    doplata: str
    start_date: Date
    end_date: Date
    was_done_previously: bool
    screenshot_path: str
    employee_status: str | None = None
    branch_num: str | None = None
    tab_num: str | None = None

    def __str__(self) -> str:
        return f"{self.employee_fullname} {self.order_number}"

    def __post_init__(self):
        convert_date(self, "start_date", "%d.%m.%y")
        convert_date(self, "end_date", "%d.%m.%y")


@dataclasses.dataclass(slots=True)
class VacationAddPayOrder:
    substitutee_fullname: str
    employee_fullname: str
    employee_names: tuple[str, str]
    order_number: str
    doplata: str
    start_date: Date
    end_date: Date
    was_done_previously: bool
    screenshot_path: str
    employee_status: str | None = None
    branch_num: str | None = None
    tab_num: str | None = None

    def __str__(self) -> str:
        return f"{self.employee_fullname} {self.order_number}"

    def __post_init__(self):
        convert_date(self, "start_date", "%d.%m.%y")
        convert_date(self, "end_date", "%d.%m.%y")
