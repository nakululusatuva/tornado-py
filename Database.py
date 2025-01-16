import os
import sqlite3
import threading as TR
from enum import Enum
from hexbytes import HexBytes
from web3.types import Wei

import Log
from Executor import Job, TaskQueue
from Types import Second


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

    '''
    Open or Create a database if not exists
    '''
    def open(self, url: str) -> bool:
        raise NotImplementedError

    def close(self) -> None:
        raise NotImplementedError

    '''
    Get commitments by leaf index
    @param  index_start     Start from which leaf index (inclusive)
    @param  index_end       End at which leaf index (inclusive)
    @return List of commitments
            None if error occurred
    '''
    def get_leafs(self, index_start: int, index_end: int) -> list[HexBytes] | None:
        raise NotImplementedError


class SQLiteClient(InterfaceClient):

    def __init__(self):
        super().__init__(Backend.SQLITE)
        self.TAG: str = __class__.__name__
        self.mutex      : TR.Lock                   = TR.Lock()
        self.opened     : bool                      = False
        self.taskq      : TaskQueue                 = TaskQueue()
        self.connection : sqlite3.Connection | None = None
        self.cursor     : sqlite3.Cursor | None     = None

    def open(self, url: str) -> bool:
        with self.mutex:
            # Create directory recursively if not exists
            directory: str = os.path.dirname(url)
            if not os.path.exists(directory):
                os.makedirs(directory, exist_ok=True)

            # Start worker threads
            if self.opened:
                Log.Warn(self.TAG, f'already opened')
                return True
            else:
                self.taskq.start()

            # Create and open database
            def _() -> None:
                try:
                    self.connection = sqlite3.connect(url)
                    self.connection.execute('PRAGMA cache_size=20971520;')  # 20 GB
                    self.connection.execute('PRAGMA synchronous=OFF;')  # Or 'NORMAL' for better safety but slower
                    self.connection.execute('PRAGMA journal_mode=WAL;')
                    self.connection.execute('PRAGMA temp_store=MEMORY;')
                    for table_name, table_structure in TABLE_STRUCTURE.items():
                        sql: str = f'CREATE TABLE IF NOT EXISTS {table_name} ('
                        for column, type_ in zip(table_structure['columns'], table_structure['types']):
                            sql += f'{column} {type_}, '
                        sql = sql[:-2] + ')'
                        self.connection.execute(sql)
                    self.connection.commit()
                    self.cursor = self.connection.cursor()
                    self.opened = True
                except Exception as e_:
                    Log.Error(self.TAG, f'open database exception, error: {e_}')
            self.taskq.run_sync(Job('open_db', _))

            return self.opened

    def close(self) -> None:
        with self.mutex:
            if not self.opened:
                Log.Warn(self.TAG, f'already closed')
                return
            def _() -> None:
                try:
                    self.cursor.close()
                    self.connection.close()
                except Exception as e:
                    Log.Error(self.TAG, f'close database exception, error: {e}')
                self.opened     = False
                self.cursor     = None
                self.connection = None
            self.taskq.run_sync(Job('close_db', _))

    def get_leafs(self, index_start: int, index_end: int) -> list[HexBytes] | None:
        with self.mutex:
            if not self.opened:
                Log.Error(self.TAG, f'database not opened')
                return []

            leafs: list[HexBytes] = []
            def _() -> None:
                try:
                    sql: str = f'SELECT commitment FROM MerkleTreeLeaf WHERE leaf_index BETWEEN {index_start} AND {index_end}'
                    self.cursor.execute(sql)
                    for row in self.cursor.fetchall():
                        leafs.append(HexBytes(row[0]))
                except Exception as e:
                    Log.Error(self.TAG, f'get leafs exception, error: {e}')
            self.taskq.run_sync(Job('get_leafs', _))

            return leafs


class Factory(object):

    @staticmethod
    def client(backend: Backend) -> InterfaceClient:
        if backend == Backend.SQLITE:
            return SQLiteClient()
        else:
            raise NotImplementedError
