from enum import Enum

class DataOperations(Enum):
    READ = "read"
    WRITE = "write"
    CREATE = "create"
    DELETE = "delete"