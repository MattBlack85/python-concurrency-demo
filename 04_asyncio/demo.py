import asyncio
import time
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import cpu_count

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

console = Console()


def blocking_io_task(delay: float = 1.0) -> float:
    time.sleep(delay)
    return delay


async def async_io_task(delay: float = 1.0) -> float:
    await asyncio.sleep(delay)
    return delay


def run_sequential(num_tasks: int, delay: float) -> tuple[list[float], float]:
    start = time.perf_counter()
    results = [blocking_io_task(delay) for _ in range(num_tasks)]
    end = time.perf_counter()
    return results, round(end - start, 2)


def run_threaded(
    num_tasks: int, delay: float, max_workers: int
) -> tuple[list[float], float]:
    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        # fire off all tasks at once
        futures = [pool.submit(blocking_io_task, delay) for _ in range(num_tasks)]
        # collect results
        results = [f.result() for f in futures]
    end = time.perf_counter()
    return results, round(end - start, 2)


async def run_asyncio(num_tasks: int, delay: float) -> tuple[list[float], float]:
    start = time.perf_counter()
    coros = [async_io_task(delay) for _ in range(num_tasks)]
    # schedule them concurrently
    results = await asyncio.gather(*coros)
    end = time.perf_counter()
    return results, round(end - start, 3)


def show_header(num_tasks: int, delay: float, workers: int):
    console.print(
        Panel.fit(
            "[bold cyan]🐍 Python Concurrency Demo: async vs threads vs sequential[/bold cyan]",
            border_style="cyan",
        )
    )

    console.print(
        f"[bold cyan]Task definition:[/bold cyan] pretend API call that 'waits' {delay:.1f}s\n"
        f"[bold cyan]Number of calls:[/bold cyan] {num_tasks}\n"
        f"[bold cyan]Thread pool size:[/bold cyan] {workers}\n"
    )


def show_results(seq_t, thr_t, async_t, num_tasks):
    table = Table(title="Wall-clock Time Comparison", title_style="bold magenta")
    table.add_column("Mode", style="bold white")
    table.add_column("Time (s)", justify="right", style="bold cyan")
    table.add_column("Notes", style="bold yellow")

    table.add_row(
        "Sequential",
        f"{seq_t:.2f}",
        "1 call after another (blocking)",
    )
    table.add_row(
        "ThreadPool",
        f"{thr_t:.2f}",
        "Blocking I/O in many threads at once",
    )
    table.add_row(
        "asyncio",
        f"{async_t:.2f}",
        "Single thread, cooperative awaits",
    )

    console.print(table)

    console.print(
        Panel.fit(
            "[bold green]For I/O-bound work:[/bold green] Threads and asyncio overlap waits.\n"
            f"[bold white]Result:[/bold white] ~same total time as ONE request, not {num_tasks}.",
            border_style="green",
        )
    )

    console.print(
        Panel.fit(
            "[bold magenta]For CPU-bound work:[/bold magenta] asyncio and threads do NOT help.\n"
            "Use multiprocessing (separate processes, separate GILs).",
            border_style="magenta",
        )
    )


def main():
    num_tasks = 100
    delay = 1.327218
    workers = min(num_tasks, cpu_count() * 2)

    show_header(num_tasks, delay, workers)

    # Sequential
    with console.status(
        "[bold yellow]Running sequential (blocking calls one by one)...", spinner="dots"
    ):
        _, seq_time = run_sequential(num_tasks, delay)

    # Thread pool
    with console.status(
        "[bold green]Running ThreadPoolExecutor (many blocking calls in parallel)...",
        spinner="earth",
    ):
        _, thr_time = run_threaded(num_tasks, delay, max_workers=workers)

    # asyncio
    with console.status(
        "[bold cyan]Running asyncio (single-threaded concurrency)...", spinner="line"
    ):
        async_results, async_time = asyncio.run(run_asyncio(num_tasks, delay))

    console.print()
    show_results(seq_time, thr_time, async_time, num_tasks)

    console.print(
        Panel.fit(
            f"[bold white]All returned values length:[/bold white] {len(async_results)}\n"
            f"[dim]We didn't skip work. We just overlapped the waiting.[/dim]",
            border_style="white",
        )
    )


if __name__ == "__main__":
    main()
