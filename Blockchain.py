import threading as TR
from eth_typing import ChecksumAddress
from hexbytes import HexBytes
from typing import Callable
from web3 import Web3
from web3.types import LogReceipt, Wei

import Log
import Var
from Executor import Job, TaskQueue
from Types import EventDeposit, EventWithdraw, LogEvent, Second
from Utils import Sleep, UnixTimestamp


class EventPoller(object):

    def __init__(self, rpc_url: str, interval: Second):
        self.TAG     : str                             = __class__.__name__
        self.rpc_url : str                             = rpc_url
        self.w3      : Web3 | None                     = None
        self.interval: Second                          = interval
        self.contract: ChecksumAddress | None          = None
        self.events  : list[HexBytes] | None           = None
        self.block   : int                             = 0
        self.on_event: set[Callable[[LogEvent], None]] = set()
        self.on_block: set[Callable[[int], None]]      = set()
        self.off     : bool                            = True
        self.synced  : bool                            = False
        self.time    : Second                          = Second(0)
        self.cond    : TR.Condition                    = TR.Condition()
        self.worker  : TR.Thread | None                = None
        self.sinker  : TaskQueue                       = TaskQueue()

    '''
    Start polling
    @param contract     Contract address, 0.1/1/10/100 ETH
    @param start_block  Start block number, inclusive
    @param events       List of event hashes to poll
    '''
    def start(self, contract: ChecksumAddress, start_block: int, events: list[HexBytes]) -> bool:
        if not self.off:
            Log.Warn(self.TAG, 'start() already started')
            return False
        if self.rpc_url.startswith('http'):
            self.w3 = Web3(Web3.HTTPProvider(self.rpc_url))
        elif self.rpc_url.startswith('ws'):
            self.w3 = Web3(Web3.LegacyWebSocketProvider(self.rpc_url))
        else:
            Log.Error(self.TAG, f'Unsupported RPC URL: {self.rpc_url}')
            return False
        self.off       = False
        self.events    = events
        self.contract  = contract
        self.block     = start_block
        self.worker    = TR.Thread(target=self._loop)
        self.worker.start()
        Log.Debug(self.TAG, "start() done")
        return True

    def stop(self) -> None:
        if self.off:
            Log.Warn(self.TAG, 'stop() already stopped')
            return
        Log.Debug(self.TAG, 'stop() shutting down')
        self.off = True
        with self.cond:
            self.cond.notify_all()
        self.worker.join()
        self.worker   = None
        self.events   = None
        self.contract = None
        self.w3       = None
        Log.Debug(self.TAG, 'stop() done')

    def add_event_handler(self, callback: Callable[[LogEvent], None]) -> None:
        with self.cond:
            self.on_event.add(callback)

    def add_block_handler(self, callback: Callable[[int], None]) -> None:
        with self.cond:
            self.on_block.add(callback)

    '''
    Wait until catch up to latest block
    '''
    def catchup(self) -> None:
        with self.cond:
            self.cond.notify_all()
        while not self.synced:
            Sleep(Second(0.1))

    def _loop(self) -> None:
        self.time = UnixTimestamp()
        while not self.off:
            latest              : int = 0
            count_block         : int = 0
            count_event_deposit : int = 0
            count_event_withdraw: int = 0

            # Get latest block number
            while latest <= 0:
                try:
                    latest = self.w3.eth.block_number
                except Exception as e:
                    Log.Error(self.TAG, f'Failed to get latest block number, error: {e}')
                    Log.Info(self.TAG, f'Wait {Var.RPC_RETRY_INTERVAL}s and retry')
                    Sleep(Var.RPC_RETRY_INTERVAL, self.cond)
                    continue

            # Sleep interval if no new block
            if latest < self.block:
                self.synced = True
                Sleep(self.interval, self.cond)
                continue
            self.synced = False
            count_block = latest - self.block + 1

            # Split into 1000 blocks per request
            chunks: list[tuple[int, int]] = []
            if latest == self.block:
                chunks.append((self.block, latest))
            else:
                for i in range(self.block, latest, 1000):
                    chunks.append((i, min(i + 1000, latest)))

            # Process each chunk
            for chunk in chunks:
                # Get logs
                logs: list[LogReceipt] | None = None
                while logs is None:
                    try:
                        logs = self.w3.eth.get_logs({
                            'address'  : self.contract,
                            'fromBlock': chunk[0],
                            'toBlock'  : chunk[1],
                            'topics'   : self.events,
                        })
                    except Exception as e:
                        Log.Error(self.TAG, f'Failed to get logs, error: {e}')
                        Log.Info(self.TAG, f'Wait {Var.RPC_RETRY_INTERVAL}s and retry')
                        Sleep(Var.RPC_RETRY_INTERVAL, self.cond)

                # Process logs
                for log in logs:
                    if EventDeposit.event_hash() == log['topics'][0]:
                        timestamp : int = int.from_bytes(log['data'][:32], byteorder='big')
                        blk_num   : int = log['blockNumber']
                        tx_hash   : str = log['transactionHash'].to_0x_hex()
                        commitment: str = log['topics'][1].to_0x_hex()
                        leaf_index: int = int.from_bytes(log['data'][32:], byteorder='big')
                        for call in self.on_event:
                            self.sinker.run_async(Job('EventDeposit', lambda: call(EventDeposit(timestamp, blk_num, tx_hash, commitment, leaf_index))))
                        count_event_deposit += 1
                    elif EventWithdraw.event_hash() == log['topics'][0]:
                        blk_num       : int = log['blockNumber']
                        tx_hash       : str = log['transactionHash'].to_0x_hex()
                        nullifier_hash: str = log['data'][32:64].to_0x_hex()
                        to            : str = log['data'][12:32].to_0x_hex()
                        fee           : Wei = Wei(int.from_bytes(log['data'][64:], byteorder='big'))
                        for call in self.on_event:
                            self.sinker.run_async(Job('EventWithdraw', lambda: call(EventWithdraw(blk_num, tx_hash, nullifier_hash, to, fee))))
                        count_event_withdraw += 1
                    else:
                        Log.Warn(self.TAG, f'Unknown event: {log}')

                # Prevent reach the rate limit
                Sleep(Var.RPC_QUERY_INTERVAL, self.cond)

            # Log and notify
            Log.Info(self.TAG, f'Poll {count_block} blocks, {count_event_deposit} deposits, {count_event_withdraw} withdraws')

            # Callback and update
            for call in self.on_block:
                self.sinker.run_async(Job('Progress', lambda: call(latest)))
            self.block = latest + 1

            # Sleep interval
            self.time: Second = Second(self.time + self.interval)
            gap: Second = Second(self.time - UnixTimestamp())
            if gap > 0:
                self.synced = True
                Sleep(gap, self.cond)
                self.synced = False

        # Reset timestamp
        self.time = Second(0)
