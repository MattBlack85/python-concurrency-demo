import time

from rich import print


def fake_io_task(task_id: int, delay: float = 1.0):
    """Simulate an I/O-bound task (e.g. network call) by sleeping."""
    print(f"[yellow]Starting task {task_id}[/yellow]")
    time.sleep(delay)
    print(f"[green]Finished task {task_id}[/green]")


def main():
    start = time.perf_counter()
    for i in range(5):
        fake_io_task(i)
    end = time.perf_counter()
    print(f"[bold]Total time (sequential): {end - start:.2f}s[/bold]")


if __name__ == "__main__":
    main()
