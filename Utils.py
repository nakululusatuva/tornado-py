import os
import signal
import time

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
