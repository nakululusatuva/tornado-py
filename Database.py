import os
import sqlite3
from enum import Enum
from web3.types import Wei

import Log
from MyType import Second


class EventDeposit(object):

    def __init__(self, timestamp : Second,
                       blk_num   : int,
                       tx_hash   : str,
                       commitment: str,
                       leaf_index: int) -> None:
        self.timestamp : Second = timestamp
        self.blk_num   : int    = blk_num
        self.tx_hash   : str    = '0x' + tx_hash if not tx_hash.startswith('0x') else tx_hash
        self.commitment: str    = '0x' + commitment if not commitment.startswith('0x') else commitment
        self.leaf_index: int    = leaf_index

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


class EventWithdraw(object):

    def __init__(self, blk_num       : int,
                       tx_hash       : str,
                       nullifier_hash: str,
                       to            : str,
                       fee           : Wei) -> None:
        self.blk_num       : int = blk_num
        self.tx_hash       : str = '0x' + tx_hash if not tx_hash.startswith('0x') else tx_hash
        self.nullifier_hash: str = '0x' + nullifier_hash if not nullifier_hash.startswith('0x') else nullifier_hash
        self.to            : str = '0x' + to if not to.startswith('0x') else to
        self.fee           : Wei = fee

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


TABLE_STRUCTURE: dict = {
    'EventDeposit' : {
        'columns': ['timestamp', 'blk_num', 'tx_hash', 'commitment', 'leaf_index'],
        'types'  : ['INTEGER', 'INTEGER', 'TEXT', 'TEXT', 'INTEGER'],
    },
    'EventWithdraw': {
        'columns': ['blk_num', 'tx_hash', 'nullifier_hash', 'to', 'fee'],
        'types'  : ['INTEGER', 'TEXT', 'TEXT', 'TEXT', 'INTEGER'],
    },
    'MerkleTreeLeaf': {
        'columns': ['leaf_index', 'commitment'],
        'types'  : ['INTEGER', 'TEXT'],
    }
}


class Backend(Enum):
    SQLITE = 'sqlite'


class InterfaceClient(object):

    def __init__(self, backend: Backend) -> None:
        self.backend: Backend = backend

    def open(self, url: str) -> bool:
        raise NotImplementedError

    def close(self) -> None:
        raise NotImplementedError


class SQLiteClient(InterfaceClient):

    def __init__(self):
        super().__init__(Backend.SQLITE)
        self.TAG: str = 'SQLiteClient'

    def open(self, url: str) -> bool:
        if not os.path.exists(url):
            Log.Error(self.TAG, f'open({self}) failed, url={url} not exists')
            return False
        connected_mem: bool = False
        connected_per: bool = False
        # Start worker threads
        if self.connected:
            Log.Warn(self.TAG, f'connect({self}) already connected')
            return True
        else:
            for q in self.queue_memory:
                q.start()
            self.queue_persist.start()
        # Close all connections if falied
        def revert() -> None:
            for connection in self.conn_memory:
                if connection is not None:
                    connection.close()
            self.conn_memory.clear()
            self.cursor_memory.clear()
            def _():
                if self.conn_persist is not None:
                    self.conn_persist.close()
                    self.conn_persist = None
            self.queue_persist.run_sync(Job('close_db_persistent', _))
            self.cursor_persist = None
        # Create and connect to persistent database
        def connect_db_persistent() -> None:
            nonlocal connected_per
            try:
                self.conn_persist = sqlite3.connect(url)
                self.conn_persist.execute('PRAGMA cache_size=20971520;')  # 20 GB
                self.conn_persist.execute('PRAGMA synchronous=OFF;')  # Or 'NORMAL' for better safety but slower
                self.conn_persist.execute('PRAGMA journal_mode=WAL;')
                self.conn_persist.execute('PRAGMA temp_store=MEMORY;')
                self.conn_persist.execute('CREATE TABLE IF NOT EXISTS records ('
                                          'unix_timestamp_sec INTEGER, '
                                          'token_enum_name TEXT, '
                                          'checksum_address_victim TEXT, '
                                          'checksum_address_poisoned TEXT, '
                                          'checksum_address_phish TEXT, '
                                          'private_key_phish TEXT'
                                          ')')
                self.conn_persist.commit()
                self.cursor_persist = self.conn_persist.cursor()
                connected_per = True
            except Exception as e_:
                Log.Error(self.TAG, f'connect_db_persistent() exception, error: {e_}')
                connected_per = False

    def close(self) -> None:
        raise NotImplementedError


def client(type: Backend, **args) -> InterfaceClient:
    if type == Backend.SQLITE:
        return SQLiteClient()
    else:
        raise NotImplementedError
