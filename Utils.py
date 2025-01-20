import os
import signal
import time
import threading as TR

from Types import Second


def UnixTimestamp() -> Second:
    return Second(time.time_ns() / 1000.0 / 1000.0 / 1000.0)


def SignalInterrupt() -> None:
    os.kill(os.getpid(), signal.SIGINT)


def WaitInterrupt() -> None:
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass

def Sleep(interval: Second, cond: TR.Condition | None = None, interruptable: bool = True) -> None:
    time_point: Second = Second(UnixTimestamp() + interval)
    if cond is None:
        cond = TR.Condition()
    with cond:
        while UnixTimestamp() < time_point:
            try:
                cond.wait(time_point - UnixTimestamp())
            except KeyboardInterrupt:
                if interruptable:
                    break
