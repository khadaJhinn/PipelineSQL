"""Durées et pic RSS du processus, échantillonné toutes les 20 ms."""

from contextlib import contextmanager
from threading import Event, Thread
from time import perf_counter

import psutil


@contextmanager
def measure():
    process = psutil.Process()
    stopped = Event()
    report = {"peak_rss_mb": process.memory_info().rss / 1024**2}

    def sample():
        while not stopped.wait(0.02):
            report["peak_rss_mb"] = max(report["peak_rss_mb"], process.memory_info().rss / 1024**2)

    thread = Thread(target=sample, daemon=True)
    started = perf_counter()
    thread.start()
    try:
        yield report
    finally:
        stopped.set()
        thread.join()
        report["seconds"] = perf_counter() - started
        report["peak_rss_mb"] = max(report["peak_rss_mb"], process.memory_info().rss / 1024**2)
