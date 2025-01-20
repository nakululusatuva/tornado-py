import threading as TR
from typing import Callable

import Log


class Job:
    def __init__(self, name: str, task: Callable[[], None], on_exception: Callable[[Exception], None] = None) -> None:
        self.done        : TR.Event                    = TR.Event()
        self.name        : str                         = name
        self.task        : Callable[[], None]          = task
        self.on_exception: Callable[[Exception], None] = on_exception


class TaskQueue:

    def __init__(self) -> None:
        self.TAG   : str          = __class__.__name__
        self.off   : bool         = True
        self.cond  : TR.Condition = TR.Condition()
        self.worker: TR.Thread    = None
        self.queue : list[Job]    = []

    def start(self) -> None:
        if not self.off:
            Log.Warn(self.TAG, 'start() already started')
            return
        self.off = False
        self.worker = TR.Thread(target=self._loop)
        self.worker.start()
        self.run_sync(Job('TaskQueue', lambda: Log.Debug(self.TAG, 'start() done')))

    def stop(self) -> None:
        if self.off:
            Log.Warn(self.TAG, 'stop() already stopped')
            return
        Log.Debug(self.TAG, 'stop() shutting down')
        self.off = True
        with self.cond:
            self.cond.notify_all()
        self.worker.join()
        self.worker = None
        Log.Debug(self.TAG, 'stop() done')

    def queue_size(self, lock: bool) -> int:
        if lock:
            with self.cond:
                return int(len(self.queue))
        else:
            return int(len(self.queue))

    def run_sync(self, job: Job) -> bool:
        if self.off:
            Log.Warn(self.TAG, 'run_sync(), queue is stopped')
            return False
        with self.cond:
            self.queue.append(job)
            self.cond.notify_all()
        job.done.wait()
        return True

    def run_async(self, job: Job) -> bool:
        if self.off:
            Log.Warn(self.TAG, 'run_async(), queue is turned off')
            return False
        with self.cond:
            self.queue.append(job)
            self.cond.notify_all()
        return True

    def _loop(self) -> None:
        while True:
            # Get task
            job: Job | None = None
            with self.cond:
                # Stop loop if turn off and queue is empty
                if 0 == len(self.queue) and self.off:
                    break
                # Wait for task
                while 0 == len(self.queue) and not self.off:
                    try:
                        self.cond.wait(0.01)  # 10ms
                    except KeyboardInterrupt:
                        pass
                if len(self.queue) > 0:
                    job = self.queue.pop(0)
            # Execute task
            if job is not None:
                try:
                    job.task()
                except Exception as e0:
                    if job.on_exception is None:
                        Log.Error(self.TAG, f'Exception in task {job.name}: {e0}')
                    else:
                        try:
                            job.on_exception(e0)
                        except Exception as e1:
                            Log.Error(self.TAG, f'During \'on_exception({e0})\' of task {job.name}, another exception occurred: {e1}')
                job.done.set()
