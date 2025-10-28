import random
import threading
import time

from rich import print
from rich.console import Console
from rich.table import Table

console = Console()


def fake_io_task(task_id: int, base_delay: float, results: list):
    """
    Simulate I/O by sleeping with a random delay.
    Threads can overlap this waiting.
    We'll log start/end times and which thread handled the task.
    """
    thread_name = threading.current_thread().name

    # Add slight randomness to simulate variable network latency
    delay = base_delay + random.uniform(-0.2, 0.4)

    # Optional random startup delay (simulate scheduling jitter)
    time.sleep(random.uniform(0, 0.05))

    start_ts = time.perf_counter()
    print(
        f"[yellow]Task {task_id} started on {thread_name} (delay={delay:.2f}s)[/yellow]"
    )

    time.sleep(delay)  # blocking sleep simulating I/O wait

    end_ts = time.perf_counter()
    print(f"[green]Task {task_id} finished on {thread_name}[/green]")

    # Store metrics so we can present them in a table later
    results.append(
        {
            "task_id": task_id,
            "thread": thread_name,
            "delay": delay,
            "start": start_ts,
            "end": end_ts,
            "duration": end_ts - start_ts,
        }
    )


def main():
    NUM_TASKS = 5
    BASE_DELAY = 1.0

    results = []

    start_all = time.perf_counter()

    threads = []
    for i in range(NUM_TASKS):
        t = threading.Thread(
            target=fake_io_task,
            args=(i, BASE_DELAY, results),
            name=f"worker-{i}",
        )
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    end_all = time.perf_counter()
    total_duration = end_all - start_all

    # how many distinct threads actually ran work
    unique_threads = sorted({r["thread"] for r in results})

    print(f"\n[bold]Total wall-clock time (threads): {total_duration:.2f}s[/bold]")
    print(f"[bold]Threads used:[/bold] {len(unique_threads)} -> {unique_threads}\n")

    # Build rich table
    table = Table(
        title="Threaded I/O Task Execution",
        show_lines=True,
    )

    table.add_column("Task ID", justify="right", style="cyan", no_wrap=True)
    table.add_column("Thread", style="magenta")
    table.add_column("Delay (s)", justify="right")
    table.add_column("Started (s)", justify="right")
    table.add_column("Ended (s)", justify="right")
    table.add_column("Duration (s)", justify="right", style="green")

    if results:
        t0 = min(r["start"] for r in results)
    else:
        t0 = start_all

    # Sort rows by actual start time
    for r in sorted(results, key=lambda x: x["start"]):
        table.add_row(
            str(r["task_id"]),
            r["thread"],
            f"{r['delay']:.2f}",
            f"{r['start'] - t0:.3f}",
            f"{r['end'] - t0:.3f}",
            f"{r['duration']:.3f}",
        )

    console.print(table)

    print(
        "\n[italic]Takeaway:[/italic] All these tasks overlapped. "
        "Even with random latency (~1s+ each), total wall time is ~the slowest task, "
        "not sum-of-all-tasks.\n"
        "This is perfect for I/O-bound workloads."
    )


if __name__ == "__main__":
    main()
