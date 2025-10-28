import math
import time
from multiprocessing import Pool, cpu_count

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

console = Console()


def is_prime(n: int) -> bool:
    if n < 2:
        return False
    if n % 2 == 0:
        return n == 2
    limit = int(math.sqrt(n))
    i = 3
    while i <= limit:
        if n % i == 0:
            return False
        i += 2
    return True


def count_primes_below(limit: int) -> int:
    count = 0
    for x in range(limit):
        if is_prime(x):
            count += 1
    return count


def run_sequential(tasks: list[int]) -> tuple[list[int], float]:
    start = time.perf_counter()
    results = [count_primes_below(n) for n in tasks]
    end = time.perf_counter()
    return results, end - start


def run_multiprocessing(tasks: list[int]) -> tuple[list[int], float]:
    start = time.perf_counter()
    with Pool(processes=cpu_count()) as pool:
        results = pool.map(count_primes_below, tasks)
    end = time.perf_counter()
    return results, end - start


def show_header(tasks):
    console.print(
        Panel.fit(
            Text("🚀 Python Multiprocessing Demo", style="bold cyan"),
            border_style="cyan",
        )
    )
    console.print(
        f"[bold cyan]Tasks:[/bold cyan] {tasks}\n"
        f"[bold cyan]Total work (~sum N):[/bold cyan] {sum(tasks):,}\n"
        f"[bold cyan]CPU cores detected:[/bold cyan] {cpu_count()}\n"
    )


def show_results(seq_results, seq_time, par_results, par_time):
    table = Table(title="Results Comparison", title_style="bold magenta")
    table.add_column("Mode", style="bold white", justify="center")
    table.add_column("Results", style="bold yellow")
    table.add_column("Time (s)", style="bold cyan", justify="right")

    table.add_row("Sequential", str(seq_results), f"{seq_time:.2f}")
    table.add_row("Multiprocessing", str(par_results), f"{par_time:.2f}")

    console.print(table)

    if par_time > 0:
        speedup = seq_time / par_time
        console.print(
            Panel.fit(
                f"[bold green]✅ Speedup:[/bold green] ~{speedup:.2f}× faster "
                f"with multiprocessing\n\n"
                f"[dim]Same Python code, same math, same inputs.[/dim]\n"
                f"[bold white]Only difference:[/bold white] 1 process vs many CPU cores.",
                border_style="green",
            )
        )


def main():
    tasks = [
        1_100_000,
        1_200_000,
        1_300_000,
        1_400_000,
        1_500_000,
        1_600_000,
        1_700_000,
        1_800_000,
    ]

    show_header(tasks)

    with console.status("[bold yellow]Running sequential version...", spinner="dots"):
        seq_results, seq_time = run_sequential(tasks)

    with console.status(
        "[bold green]Running multiprocessing version...", spinner="earth"
    ):
        par_results, par_time = run_multiprocessing(tasks)

    console.print()
    show_results(seq_results, seq_time, par_results, par_time)


if __name__ == "__main__":
    main()
