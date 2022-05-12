from enum import IntEnum, auto


class Error(IntEnum):
    SUCCESS = auto()
    INVALID_COMMAND = auto()
    ALREADY_EXISTS = auto()
    DOES_NOT_EXIST = auto()
    NO_DATABASE_IN_USE = auto()
    INVALID_JSON = auto()
    INVALID_REFERENCE = auto()
    INVALID_NUMBER_OF_FIELDS = auto()
    INVALID_TYPE = auto()
    DUPLICATE_KEY = auto()
    DUPLICATE_UNIQUE = auto()
    FOREIGN_KEY_CONSTRAINT = auto()
    AMBIGUOUS_REFERENCE = auto()


class ServerError(Exception):

    def __init__(self, code, msg=""):
        self.code = int(code)
        self.message = msg
