import logging

logger = logging.getLogger("DAMU")


class ReplyError(Exception): ...


class CRMNotFoundError(Exception): ...


class ProtocolDateNotInRangeError(Exception): ...


class VypiskaDownloadError(Exception): ...


class CrmContragentNotFound(Exception): ...


class HTMLElementNotFound(Exception): ...


class LoginError(Exception): ...


class ParseError(Exception): ...


class ContractsNofFoundError(Exception):     ...


class MismatchError(ParseError): ...


class WrongDataInColumnError(ParseError): ...


class ProtocolIDNotFoundError(ParseError): ...


class InterestRateMismatchError(ParseError): ...


class InvalidColumnCount(ParseError): ...


class FloatConversionError(ParseError): ...


class DateConversionError(ParseError): ...


class BankNotSupportedError(ParseError): ...


class DataFrameInequalityError(ParseError): ...


class JoinPDFNotFoundError(ParseError): ...


class JoinProtocolNotFoundError(ParseError): ...


class LoanAmountNotFoundError(ParseError): ...


class DateNotFoundError(ParseError):
    def __init__(
        self, file_name: str, contract_id: str, para: str = ""
    ) -> None:
        self.file_name = file_name
        self.contract_id = contract_id
        self.message = (
            f"No dates in {file_name!r} of a {contract_id!r} for {para}..."
        )
        super().__init__(self.message)


class TableNotFound(ParseError):
    def __init__(self, file_name: str, contract_id: str, target: str) -> None:
        self.file_name = file_name
        self.contract_id = contract_id
        self.message = (
            f"No tables in {file_name!r} of a {contract_id!r} for {target}..."
        )
        super().__init__(self.message)


class EmptyTableError(ParseError): ...


class ExcesssiveTableCountError(ParseError):
    def __init__(
        self, file_name: str, contract_id: str, table_count: int
    ) -> None:
        self.file_name = file_name
        self.contract_id = contract_id
        self.table_count = table_count
        self.message = (
            f"Expecting to parse 1 or 2 tables, got "
            f"{table_count} in {file_name!r} of a {contract_id!r}..."
        )
        super().__init__(self.message)


class MacroError(Exception): ...


class BankExcelMismatchError(MacroError): ...


class TotalFalseValueError(MacroError): ...


class BalanceAfterRepaymentFalseValueError(MacroError): ...
