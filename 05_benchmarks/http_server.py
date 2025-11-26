import asyncio
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import httpx
import uvicorn
from fastapi import FastAPI
from rich import print
from rich.console import Console
from rich.table import Table

console = Console()

HOST = "127.0.0.1"
SYNC_PORT = 8081
ASYNC_PORT = 8082

NUM_REQUESTS = 4000
CONCURRENCY = 100
DELAY = 0.1


class SyncHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path != "/sleep":
            self.send_response(404)
            self.end_headers()
            return

        # simulate blocking I/O / work
        time.sleep(DELAY)

        body = b"ok"
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        return


def start_sync_server():
    server = ThreadingHTTPServer((HOST, SYNC_PORT), SyncHandler)
    thread = threading.Thread(
        target=server.serve_forever, name="sync-server", daemon=True
    )
    thread.start()
    return server, thread


app = FastAPI()


@app.get("/sleep")
async def sleep_endpoint():
    # simulate async I/O / work
    await asyncio.sleep(DELAY)
    return {"status": "ok"}


def start_async_server():
    config = uvicorn.Config(
        app,
        host=HOST,
        port=ASYNC_PORT,
        log_level="error",
        loop="asyncio",
    )
    server = uvicorn.Server(config)

    def runner():
        asyncio.run(server.serve())

    thread = threading.Thread(target=runner, name="async-server", daemon=True)
    thread.start()
    return server, thread


def wait_until_healthy(url: str, timeout: float = 5.0):
    deadline = time.perf_counter() + timeout
    while time.perf_counter() < deadline:
        try:
            r = httpx.get(url, timeout=0.2)
            if r.status_code == 200:
                return
        except Exception:
            pass
        time.sleep(0.05)
    raise RuntimeError(f"Server at {url} did not become healthy in time")


async def bench_server(label: str, url: str) -> dict:
    sem = asyncio.Semaphore(CONCURRENCY)

    async with httpx.AsyncClient() as client:

        async def one_request(idx: int):
            async with sem:
                r = await client.get(url)
                r.raise_for_status()

        start = time.perf_counter()
        await asyncio.gather(*(one_request(i) for i in range(NUM_REQUESTS)))
        end = time.perf_counter()

    total_time = end - start
    rps = NUM_REQUESTS / total_time if total_time > 0 else float("inf")
    return {
        "label": label,
        "url": url,
        "total_time": total_time,
        "rps": rps,
    }


def main():
    print(
        f"[blue]HTTP server benchmark[/blue]\n"
        f"- Requests per scenario: [bold]{NUM_REQUESTS}[/bold]\n"
        f"- Client concurrency:    [bold]{CONCURRENCY}[/bold]\n"
        f"- Per-request simulated work: [bold]{DELAY:.3f}s[/bold]\n"
    )

    results = []

    sync_server, sync_thread = start_sync_server()
    sync_url = f"http://{HOST}:{SYNC_PORT}/sleep"
    wait_until_healthy(sync_url)
    print("[yellow]Sync server (ThreadingHTTPServer) is up, benchmarking...[/yellow]")

    sync_res = asyncio.run(bench_server("sync-threaded", sync_url))
    results.append(sync_res)

    sync_server.shutdown()
    sync_thread.join(timeout=1)

    async_server, async_thread = start_async_server()
    async_url = f"http://{HOST}:{ASYNC_PORT}/sleep"
    wait_until_healthy(async_url)
    print("[yellow]Async server (FastAPI + uvicorn) is up, benchmarking...[/yellow]")

    async_res = asyncio.run(bench_server("async-fastapi", async_url))
    results.append(async_res)

    async_server.should_exit = True
    async_thread.join(timeout=1)

    table = Table(title="HTTP server benchmark results", show_lines=True)
    table.add_column("Server", style="cyan", no_wrap=True)
    table.add_column("Impl", style="magenta")
    table.add_column("Total time (s)", justify="right", style="green")
    table.add_column("Requests/sec", justify="right", style="green")

    table.add_row(
        "sync-threaded",
        "ThreadingHTTPServer + time.sleep",
        f"{sync_res['total_time']:.3f}",
        f"{sync_res['rps']:.1f}",
    )
    table.add_row(
        "async-fastapi",
        "FastAPI + uvicorn + asyncio.sleep",
        f"{async_res['total_time']:.3f}",
        f"{async_res['rps']:.1f}",
    )

    console.print()
    console.print(table)

    print(
        "\n[italic]Interpretation:[/italic]\n"
        "- Both servers can handle concurrent requests, but they use different models:\n"
        "  • sync-threaded: many OS threads, each blocking on time.sleep.\n"
        "  • async-fastapi: single event loop with non-blocking asyncio.sleep.\n"
        "Exact numbers depend on your machine, but the structure is what matters.\n"
    )


if __name__ == "__main__":
    main()
