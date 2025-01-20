from enum import Enum
from typing import NewType

from hexbytes import HexBytes
from web3 import Web3
from web3.types import Wei


Second      = NewType("Second", float)
MBytes      = NewType("MBytes", int)
UINT256_MAX = 2 ** 256 - 1


class ChainID(Enum):
    ETHEREUM = 0x01    # 1
    POLYGON  = 0x89    # 137


class LogEvent(object):

    def __init__(self, signature: str) -> None:
        self.signature: str = signature

    def __str__(self) -> str:
        raise NotImplementedError

    def __dict__(self) -> dict:
        raise NotImplementedError


class EventDeposit(LogEvent):

    def __init__(self, timestamp : Second,
                       blk_num   : int,
                       tx_hash   : str,
                       commitment: str,
                       leaf_index: int) -> None:
        super().__init__('Deposit(bytes32,uint32,uint256)')
        self.timestamp : Second = timestamp
        self.blk_num   : int    = blk_num
        self.tx_hash   : str    = '0x' + tx_hash if not tx_hash.startswith('0x') else tx_hash
        self.commitment: str    = '0x' + commitment if not commitment.startswith('0x') else commitment
        self.leaf_index: int    = leaf_index

    @staticmethod
    def event_hash() -> HexBytes:
        return Web3.keccak(text='Deposit(bytes32,uint32,uint256)')

    @staticmethod
    def from_dict(_dict: dict) -> 'EventDeposit':
        return EventDeposit(
            _dict['timestamp'],
            _dict['blk_num'],
            _dict['tx_hash'],
            _dict['commitment'],
            _dict['leaf_index'])

    def __str__(self) -> str:
        return (f'timestamp={self.timestamp}, '
                f'blk_num={self.blk_num}, '
                f'tx_hash={self.tx_hash}, '
                f'commitment={self.commitment}, '
                f'leaf_index={self.leaf_index}')

    def __dict__(self) -> dict:
        return {
            'timestamp' : self.timestamp,
            'blk_num'   : self.blk_num,
            'tx_hash'   : self.tx_hash,
            'commitment': self.commitment,
            'leaf_index': self.leaf_index,
        }


class EventWithdraw(LogEvent):

    def __init__(self, blk_num       : int,
                       tx_hash       : str,
                       nullifier_hash: str,
                       to            : str,
                       fee           : Wei) -> None:
        super().__init__('Withdrawal(address,bytes32,address,uint256)')
        self.blk_num       : int = blk_num
        self.tx_hash       : str = '0x' + tx_hash if not tx_hash.startswith('0x') else tx_hash
        self.nullifier_hash: str = '0x' + nullifier_hash if not nullifier_hash.startswith('0x') else nullifier_hash
        self.to            : str = '0x' + to if not to.startswith('0x') else to
        self.fee           : Wei = fee

    @staticmethod
    def event_hash() -> HexBytes:
        return Web3.keccak(text='Withdrawal(address,bytes32,address,uint256)')

    @staticmethod
    def from_dict(_dict: dict) -> 'EventWithdraw':
        return EventWithdraw(
            _dict['blk_num'],
            _dict['tx_hash'],
            _dict['nullifier_hash'],
            _dict['to'],
            _dict['fee'])

    def __str__(self) -> str:
        return (f'blk_num={self.blk_num}, '
                f'tx_hash={self.tx_hash}, '
                f'nullifier_hash={self.nullifier_hash}, '
                f'to={self.to}, '
                f'fee={self.fee}')

    def __dict__(self) -> dict:
        return {
            'blk_num'       : self.blk_num,
            'tx_hash'       : self.tx_hash,
            'nullifier_hash': self.nullifier_hash,
            'to'            : self.to,
            'fee'           : self.fee,
        }
