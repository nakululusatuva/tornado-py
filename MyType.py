from enum import Enum
from typing import NewType


Second      = NewType("Second", float)
MBytes      = NewType("MBytes", int)
UINT256_MAX = 2 ** 256 - 1


class ChainID(Enum):
    ETHEREUM = 0x01    # 1
    POLYGON  = 0x89    # 137
