import os
import sqlite3
import threading as TR
from enum import Enum
from typing import Any

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
    'Info': {
        'columns': ['latest_blk_num', 'unspent'],
        'types'  : ['INTEGER', 'INTEGER'],
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
    Get latest block number
    @return Latest synced block number
            None if error occurred
    '''
    def get_latest(self) -> int | None:
        raise NotImplementedError

    '''
    Get how many deposit remain unspent
    @return Number of unspent deposits
            None if error occurred
    '''
    def get_unspent(self) -> int | None:
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

    def add_deposit(self, event: EventDeposit) -> bool:
        raise NotImplementedError

    def add_withdraw(self, event: EventWithdraw) -> bool:
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
                Log.Warn(self.TAG, f'Already opened')
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
                    self.connection.execute('INSERT INTO Info (latest_blk_num, unspent) SELECT 0, 0 WHERE NOT EXISTS (SELECT * FROM Info);')
                    self.connection.commit()
                    self.cursor = self.connection.cursor()
                    self.opened = True
                except Exception as e_:
                    Log.Error(self.TAG, f'Open database exception, error: {e_}')
            self.taskq.run_sync(Job('open', _))

            return self.opened

    def close(self) -> None:
        with self.mutex:
            if not self.opened:
                Log.Warn(self.TAG, f'Already closed')
                return
            def _() -> None:
                try:
                    self.cursor.close()
                    self.connection.close()
                except Exception as e:
                    Log.Error(self.TAG, f'Close database exception, error: {e}')
                self.opened     = False
                self.cursor     = None
                self.connection = None
            self.taskq.run_sync(Job('close', _))

    def get_latest(self) -> int | None:
        sql: str = 'SELECT latest_blk_num FROM Info;'
        with self.mutex:
            result: list[int] | None = self._query(sql)
            if result is None:
                return None
            return result[0]

    def get_unspent(self) -> int | None:
        sql: str = 'SELECT unspent FROM Info;'
        with self.mutex:
            result: list[int] | None = self._query(sql)
            if result is None:
                return None
            return result[0]

    def get_leafs(self, index_start: int, index_end: int) -> list[HexBytes] | None:
        sql: str = f'SELECT commitment FROM EventDeposit WHERE leaf_index BETWEEN {index_start} AND {index_end};'
        with self.mutex:
            result: list[str] | None = self._query(sql)
            if result is None:
                return None
            return [HexBytes.fromhex(x[2:] if x.startswith('0x') else x) for x in result]

    def add_deposit(self, event: EventDeposit) -> bool:
        sql: list[str] = [
            f'INSERT INTO EventDeposit VALUES ({event.timestamp}, {event.blk_num}, "{event.tx_hash}", "{event.commitment}", {event.leaf_index});',
            f'UPDATE Info SET unspent = unspent + 1;',
            f'UPDATE Info SET latest_blk_num = {event.blk_num} WHERE latest_blk_num < {event.blk_num};',
        ]
        with self.mutex:
            return self._insert(sql)

    def add_withdraw(self, event: EventWithdraw) -> bool:
        sql: list[str] = [
            f'INSERT INTO EventWithdraw VALUES ({event.blk_num}, "{event.tx_hash}", "{event.nullifier_hash}", "{event.to}", {event.fee});',
            f'UPDATE Info SET unspent = unspent - 1;',
        ]
        with self.mutex:
            return self._insert(sql)

    def _query(self, sql: str) -> list[Any] | None:
        if not self.opened:
            Log.Error(self.TAG, f'Database not opened')
            return None

        result: list[Any] | None = None
        def _() -> None:
            nonlocal result
            try:
                self.cursor.execute(sql)
                result = self.cursor.fetchall()
            except Exception as e:
                Log.Error(self.TAG, f'Query exception, sql: {sql}, error: {e}')
        self.taskq.run_sync(Job('Query', _))

        return result

    def _insert(self, sql: list[str]) -> bool:
        if not self.opened:
            Log.Error(self.TAG, f'Database not opened')
            return False

        succeed: bool = True
        def _() -> None:
            nonlocal succeed
            try:
                for q in sql:
                    self.cursor.execute(q)
                self.connection.commit()
            except Exception as e:
                Log.Error(self.TAG, f'Insert exception, sql: {sql}, error: {e}')
                self.connection.rollback()
                succeed = False
        self.taskq.run_sync(Job('Insert', _))

        return succeed


class Factory(object):

    @staticmethod
    def client(backend: Backend) -> InterfaceClient:
        if backend == Backend.SQLITE:
            return SQLiteClient()
        else:
            raise NotImplementedError
