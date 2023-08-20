from .pandas_helper import PandasLineageHelper
from .snowflake_block import SnowflakeLineageBlock
from enum import Enum

class DataOperations(Enum):
    READ = "read"
    WRITE = "write"
    CREATE = "create"
    DELETE = "delete"