import traceback
from pathlib import Path


class DetailedException(Exception):
    def __init__(self, message):
        super().__init__(message)
        self.message = message
        stack = traceback.extract_stack(limit=2)[0]
        self.filename = Path(stack.filename).name
        self.line_number = stack.lineno

    def __str__(self):
        return f"{self.__class__.__name__}({self.message!r} {self.filename}:{self.line_number})\n"


# Example Usage
def some_function():
    raise DetailedException("An example error occurred")


try:
    some_function()
except DetailedException as e:
    print(str(e))
