import ctypes
import inspect
import os
import queue
import threading
import time
from concurrent.futures import Future
from queue import Empty
from threading import Event
from typing import Iterable


class KThread(threading.Thread):
    """Killable thread.  See terminate() for details"""

    def _get_my_tid(self):
        """Determines the instance's thread ID"""
        if not self.is_alive():
            raise threading.ThreadError("Thread is not active")

        # do we have it cached?
        if hasattr(self, "_thread_id"):
            return self._thread_id

        # no, look for it in the _active dict
        for tid, tobj in threading._active.items():
            if tobj is self:
                self._thread_id = tid
                return tid

        raise AssertionError("Could not determine the thread's ID")

    def raise_exc(self, exctype):
        """raises the given exception type in the context of this thread"""
        _async_raise(self._get_my_tid(), exctype)

    def terminate(self):
        """raises SystemExit in the context of the given thread, which should
        cause the thread to exit silently (unless caught)"""
        # WARNING: using terminate(), kill(), or exit() can introduce instability in your programs
        # It is worth noting that terminate() will NOT work if the thread in question is blocked by a syscall (accept(), recv(), etc.)
        self.raise_exc(SystemExit)

    # alias functions
    def kill(self):
        self.terminate()

    def exit(self):
        self.terminate()


class WorkItem:
    """ Defines a single unit of work. Can be executed using run(). This will not return anything but
    instead fill a concurrency.Future object.
    """
    def __init__(self, future: Future, function, *args, **kwargs):
        self.future = future
        self.callback = function
        self.args = args
        self.kwargs = kwargs

    def run(self):
        if not self.future.set_running_or_notify_cancel():
            return

        try:
            self.future.set_result(self.callback(*self.args, **self.kwargs))
        except Exception as e:
            self.future.set_exception(e)


def _new_kthread(target, queue: queue.Queue, stop_event: Event) -> KThread:
    """ Default thread initializer. Returns a killable thread. """
    return KThread(target=target, args=[queue, stop_event], daemon=True)


def _worker(queue: queue.Queue, stop_event: Event):
    """ Worker procedure that awaits new WorkItems to execute. """
    while not stop_event.is_set():
        try:
            work_item = queue.get(block=True, timeout=1)
            work_item.run()

            # Clean up
            del work_item
        except Empty:
            continue


class DaemonThreadPool:
    """ Thread pool that prevents locking I/O resources to hang indefinitely in test sequences. This pool
    is faster and closes more reliably BUT is not as safe and can cause GIL locks if misused. """

    def __init__(self, max_workers=None, initializer: callable = _new_kthread):
        if max_workers is None:
            # We use cpu_count + 4 for both types of tasks.
            # But we limit it to 32 to avoid consuming surprisingly large resource
            # on many core machine.
            max_workers = min(32, (os.cpu_count() or 1) + 4)
        if max_workers <= 0:
            raise ValueError("max_workers must be greater than 0")

        self._shutdown_event = Event()
        self._max_workers = max_workers
        self._work_queue = queue.Queue()
        self._threads = set()
        self._initializer = initializer

        self._adjust_thread_count()

    def submit(self, function, *args, **kwargs) -> Future:
        """ Submit a new task to the workers. The first idle worker will pick it up. """
        future = Future()
        self._work_queue.put(WorkItem(future, function, *args, **kwargs))

        return future

    def map(self, function, *iterables, timeout: int = None) -> Iterable[Future]:
        if timeout is not None:
            end_time = timeout + time.monotonic()

        futures = [self.submit(function, *args) for args in zip(*iterables)]

        def result_iterator():
            try:
                futures.reverse()
                while futures:
                    if timeout is None:
                        yield futures.pop().result()
                    else:
                        yield futures.pop().result(end_time - time.monotonic())
            finally:
                for future in futures:
                    future.cancel()

        return result_iterator()

    def shutdown(self, wait=False):
        """ Shutdown all threads by breaking their loop with the shutdown event. """
        self._shutdown_event.set()

        if wait:
            for thread in self._threads:
                if thread.is_alive():
                    thread.join()

    def _adjust_thread_count(self):
        self._remove_dead_threads()

        while len(self._threads) < self._max_workers:
            thread = self._initializer(_worker, self._work_queue, self._shutdown_event)
            self._threads.add(thread)
            thread.start()

    def _remove_dead_threads(self):
        to_remove = []
        for thread in self._threads:
            if not thread.is_alive():
                to_remove.append(thread)

        for thread in to_remove:
            self._threads.remove(thread)
            del thread


def _async_raise(tid, exctype):
    """Raises the exception, causing the thread to exit"""
    if not inspect.isclass(exctype):
        raise TypeError("Only types can be raised (not instances)")
    res = ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_long(tid), ctypes.py_object(exctype))
    if res == 0:
        raise ValueError("Invalid thread ID")
    elif res != 1:
        # """if it returns a number greater than one, you're in trouble,
        # and you should call it again with exc=NULL to revert the effect"""
        ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, 0)
        raise SystemError("PyThreadState_SetAsyncExc failed")
