# 05_benchmarks/io_bound.py

import asyncio
import threading
import time
from multiprocessing import Pool, cpu_count

from rich import print
from rich.console import Console
from rich.table import Table

console = Console()

NUM_TASKS = 20
DELAY = 0.5


def blocking_io(task_id: int):
    """Simulate blocking I/O (e.g. requests.get) with time.sleep."""
    time.sleep(DELAY)


def run_sequential() -> float:
    start = time.perf_counter()
    for i in range(NUM_TASKS):
        blocking_io(i)
    end = time.perf_counter()
    return end - start


def run_threaded() -> float:
    threads = []
    start = time.perf_counter()
    for i in range(NUM_TASKS):
        t = threading.Thread(target=blocking_io, args=(i,))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
    end = time.perf_counter()
    return end - start


async def async_io(task_id: int):
    await asyncio.sleep(DELAY)


async def async_runner():
    tasks = [asyncio.create_task(async_io(i)) for i in range(NUM_TASKS)]
    await asyncio.gather(*tasks)


def run_asyncio() -> float:
    start = time.perf_counter()
    asyncio.run(async_runner())
    end = time.perf_counter()
    return end - start


def _mp_wrapper(task_id: int):
    blocking_io(task_id)
    return True


def run_multiprocessing() -> float:
    start = time.perf_counter()
    with Pool(processes=cpu_count()) as pool:
        list(pool.imap_unordered(_mp_wrapper, range(NUM_TASKS)))
    end = time.perf_counter()
    return end - start


def main():
    print(
        f"[blue]I/O-bound benchmark with {NUM_TASKS} tasks, delay={DELAY}s "
        "(simulated network/db calls)[/blue]\n"
    )

    seq_time = run_sequential()
    thr_time = run_threaded()
    async_time = run_asyncio()
    mp_time = run_multiprocessing()

    print(f"[bold]Sequential:     [/bold] {seq_time:.2f}s")
    print(f"[bold]Threads:        [/bold] {thr_time:.2f}s")
    print(f"[bold]Asyncio:        [/bold] {async_time:.2f}s")
    print(f"[bold]Multiprocessing:[/bold] {mp_time:.2f}s\n")

    table = Table(title="I/O-bound strategies comparison", show_lines=True)
    table.add_column("Mode", style="cyan", no_wrap=True)
    table.add_column("Total time (s)", justify="right", style="green")
    table.add_column("Concurrency model", style="magenta")

    table.add_row(
        "sequential",
        f"{seq_time:.2f}",
        "1 task at a time",
    )
    table.add_row(
        "threads",
        f"{thr_time:.2f}",
        "Many OS threads; GIL released while waiting on I/O",
    )
    table.add_row(
        "asyncio",
        f"{async_time:.2f}",
        "Single thread; event loop + await",
    )
    table.add_row(
        "multiprocessing",
        f"{mp_time:.2f}",
        "Multiple processes; heavy for mostly-waiting I/O",
    )

    console.print(table)

    print(
        "\n[italic]Takeaway:[/italic] For I/O-bound workloads, threads and asyncio "
        "shine because they overlap waiting in a single process. "
        "Multiprocessing adds overhead and doesn't buy you much here — "
        "it's designed for CPU-bound work."
    )


if __name__ == "__main__":
    main()
