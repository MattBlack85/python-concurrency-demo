import random
import threading
import time
from multiprocessing import Manager, Pool, cpu_count, current_process

from rich import print
from rich.console import Console
from rich.progress import Progress
from rich.table import Table

console = Console()

# -----------------------
# Workload: CPU-bound job
# -----------------------


def burn_cpu(n: int, progress_hook=None) -> int:
    """
    Artificial CPU-heavy function.
    It's intentionally pure Python math in a tight loop
    to hammer the GIL (in threads) or cores (in processes).

    progress_hook: optional callable(steps_done, steps_total)
    used only in multiprocessing mode to report progress back
    to the parent process via a Queue.
    """
    total = 0
    steps_total = n
    # We'll chunk the work so we can occasionally report progress.
    chunk = max(n // 20, 1)  # 5% increments-ish

    done = 0
    for i in range(n):
        total += (i * i) % 97
        done += 1
        # periodically emit progress
        if progress_hook and (done % chunk == 0 or done == steps_total):
            progress_hook(done, steps_total)

    return total


# We'll run multiple "jobs" (units of work).
WORK_UNITS = [9_000_000] * 8  # 8 heavy jobs; tune this if it's too slow/fast


# -----------------------
# Sequential version
# -----------------------


def run_sequential():
    """
    Run jobs one after another in the current thread.
    Return per-job timing info + total wall time.
    """
    results = []
    job_timings = []
    start_all = time.perf_counter()

    for job_id, n in enumerate(WORK_UNITS):
        start_ts = time.perf_counter()
        out = burn_cpu(n)
        end_ts = time.perf_counter()

        results.append(out)
        job_timings.append(
            {
                "job_id": job_id,
                "worker": "main-thread",
                "start": start_ts,
                "end": end_ts,
                "duration": end_ts - start_ts,
            }
        )

    end_all = time.perf_counter()

    return {
        "label": "sequential",
        "results": results,
        "job_timings": job_timings,
        "total_time": end_all - start_all,
        "workers_used": ["main-thread"],
    }


# -----------------------
# Threaded version
# -----------------------


def _thread_wrapper(job_id: int, n: int, sink: list):
    """
    Wrapper so we can measure per-thread timing.
    Important detail: this is still CPU-bound Python,
    so threads will fight over the GIL.
    """
    worker_name = threading.current_thread().name
    start_ts = time.perf_counter()
    out = burn_cpu(n)
    end_ts = time.perf_counter()
    sink.append(
        {
            "job_id": job_id,
            "worker": worker_name,
            "start": start_ts,
            "end": end_ts,
            "duration": end_ts - start_ts,
            "result": out,
        }
    )


def run_threaded():
    """
    Launch one thread per job.
    In theory this 'does work in parallel', but because of the GIL
    they mostly just time-slice CPU.
    """
    sink = []
    threads = []
    start_all = time.perf_counter()

    for job_id, n in enumerate(WORK_UNITS):
        t = threading.Thread(
            target=_thread_wrapper,
            args=(job_id, n, sink),
            name=f"thread-{job_id}",
        )
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    end_all = time.perf_counter()

    # Sort sink by job_id for stable table output
    sink.sort(key=lambda r: r["job_id"])

    return {
        "label": "threaded",
        "results": [r["result"] for r in sink],
        "job_timings": [
            {
                "job_id": r["job_id"],
                "worker": r["worker"],
                "start": r["start"],
                "end": r["end"],
                "duration": r["duration"],
            }
            for r in sink
        ],
        "total_time": end_all - start_all,
        "workers_used": sorted({r["worker"] for r in sink}),
    }


# -----------------------
# Multiprocessing version (with live progress)
# -----------------------


def _mp_worker(payload):
    """
    Runs in a child process.

    payload = (job_id, n, queue)
    - job_id: which job this is
    - n: how much CPU work
    - queue: multiprocessing.Manager().Queue for progress + timing info
    """
    job_id, n, queue = payload
    proc_name = current_process().name

    # local helper: push incremental progress events back to parent
    def progress_hook(done, total):
        # we send ('progress', job_id, done, total)
        queue.put(("progress", job_id, done, total))

    start_ts = time.perf_counter()
    out = burn_cpu(n, progress_hook=progress_hook)
    end_ts = time.perf_counter()

    # final result event: ('done', job_id, worker, start, end, duration, out)
    queue.put(
        (
            "done",
            job_id,
            proc_name,
            start_ts,
            end_ts,
            end_ts - start_ts,
            out,
        )
    )
    return True  # return value not super important, parent uses queue data


def run_multiprocessing():
    """
    Spawn a process pool, show a Rich progress bar for all jobs,
    and collect per-job timing from the queue.
    """
    manager = Manager()
    queue = manager.Queue()

    jobs_payload = [(job_id, n, queue) for job_id, n in enumerate(WORK_UNITS)]
    num_jobs = len(jobs_payload)

    start_all = time.perf_counter()

    # We'll consume progress messages in the parent while Pool is running.
    # Trick: use "imap_unordered" in a background thread while we drive progress loop.
    results_meta = []  # we will fill this with per-job dicts when "done" arrives

    # We'll track per-job progress locally in parent so we can render bars.
    job_progress_state = {
        job_id: {"done": 0, "total": WORK_UNITS[job_id]} for job_id in range(num_jobs)
    }

    def pool_runner():
        # run in a thread so main thread can stay in charge of Rich rendering
        with Pool(processes=cpu_count()) as pool:
            # force evaluation so processes actually start work
            for _ in pool.imap_unordered(_mp_worker, jobs_payload):
                pass

    runner_thread = threading.Thread(target=pool_runner, name="mp-pool-runner")
    runner_thread.start()

    # Live progress bars while workers churn
    with Progress(console=console, transient=True) as progress:
        task_ids = {
            job_id: progress.add_task(
                f"[magenta]Job {job_id}[/magenta]",
                total=job_progress_state[job_id]["total"],
            )
            for job_id in range(num_jobs)
        }

        # keep looping while the pool thread is alive OR queue not empty
        while runner_thread.is_alive() or not queue.empty():
            try:
                msg = queue.get(timeout=0.05)
            except Exception:
                # timeout, just refresh progress
                pass
            else:
                kind = msg[0]
                if kind == "progress":
                    _, job_id, done, total = msg
                    job_progress_state[job_id]["done"] = done
                    job_progress_state[job_id]["total"] = total
                    progress.update(task_ids[job_id], completed=done, total=total)
                elif kind == "done":
                    (
                        _,
                        job_id,
                        proc_name,
                        start_ts,
                        end_ts,
                        duration,
                        out_val,
                    ) = msg
                    # record final timing/result for tables
                    results_meta.append(
                        {
                            "job_id": job_id,
                            "worker": proc_name,
                            "start": start_ts,
                            "end": end_ts,
                            "duration": duration,
                            "result": out_val,
                        }
                    )
                    # ensure bar is full
                    st = job_progress_state[job_id]
                    progress.update(
                        task_ids[job_id], completed=st["total"], total=st["total"]
                    )

            # tiny sleep to not busy-spin like crazy
            time.sleep(0.01)

    runner_thread.join()

    end_all = time.perf_counter()

    # sort for deterministic output
    results_meta.sort(key=lambda r: r["job_id"])

    return {
        "label": "multiprocessing",
        "results": [r["result"] for r in results_meta],
        "job_timings": [
            {
                "job_id": r["job_id"],
                "worker": r["worker"],
                "start": r["start"],
                "end": r["end"],
                "duration": r["duration"],
            }
            for r in results_meta
        ],
        "total_time": end_all - start_all,
        "workers_used": sorted({r["worker"] for r in results_meta}),
    }


# -----------------------
# Pretty printing
# -----------------------


def print_mode_summary(mode_result: dict):
    """
    Print a summary table for a single mode (sequential/threaded/multiprocessing)
    showing timings per job.
    """
    label = mode_result["label"]
    job_timings = mode_result["job_timings"]
    total_time = mode_result["total_time"]
    workers_used = mode_result["workers_used"]

    print(f"\n[bold underline]{label.upper()} MODE[/bold underline]")
    print(f"[bold]{label} total wall-clock time:[/bold] {total_time:.2f}s")
    print(f"[bold]{label} workers used:[/bold] {len(workers_used)} -> {workers_used}\n")

    table = Table(
        title=f"{label.capitalize()} job breakdown",
        show_lines=True,
    )

    table.add_column("Job ID", justify="right", style="cyan", no_wrap=True)
    table.add_column("Worker", style="magenta")
    table.add_column("Start (s)", justify="right")
    table.add_column("End (s)", justify="right")
    table.add_column("Duration (s)", justify="right", style="green")

    # normalize start times to 0 for readability
    real_jobs = [j for j in job_timings if j["start"] is not None]
    if real_jobs:
        t0 = min(j["start"] for j in real_jobs)
    else:
        t0 = None

    for j in job_timings:
        if j["start"] is None:
            start_rel = ""
            end_rel = ""
            dur = ""
        else:
            start_rel = f"{j['start'] - t0:.3f}"
            end_rel = f"{j['end'] - t0:.3f}"
            dur = f"{j['duration']:.3f}"

        table.add_row(
            str(j["job_id"]),
            j["worker"],
            start_rel,
            end_rel,
            dur,
        )

    console.print(table)


def print_comparison_table(results_all: list[dict]):
    """
    Print a high-level comparison (one row per strategy).
    """
    comp = Table(
        title="CPU-bound strategies comparison",
        show_lines=True,
    )

    comp.add_column("Mode", style="bold cyan")
    comp.add_column("Workers Used", style="magenta")
    comp.add_column("Total Time (s)", justify="right", style="green")
    comp.add_column("Parallel?", justify="center")

    for r in results_all:
        label = r["label"]
        total_time = r["total_time"]
        workers_used = r["workers_used"]

        if label == "sequential":
            parallel = "No"
        elif label == "threaded":
            parallel = "Not really (GIL)"
            # Threaded here tends to be close to sequential.
        else:
            parallel = "Yes (true multi-core)"

        comp.add_row(
            label,
            ", ".join(workers_used),
            f"{total_time:.2f}",
            parallel,
        )

    console.print("\n")
    console.print(comp)
    console.print(
        "\n[italic]"
        "Observation: threads are even adding latency to the mix and won't beat sequential on CPU work, "
        "because of the GIL. Multiprocessing did, because each worker process "
        "has its own Python interpreter and its own GIL, so they truly run in parallel."
        "[/italic]\n"
    )


def main():
    # run the three modes
    seq_res = run_sequential()
    thr_res = run_threaded()
    mp_res = run_multiprocessing()

    # detailed per-mode tables
    print_mode_summary(seq_res)
    print_mode_summary(thr_res)
    print_mode_summary(mp_res)

    # summary table
    print_comparison_table([seq_res, thr_res, mp_res])


if __name__ == "__main__":
    main()
