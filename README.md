# python-concurrency-demo
Let's dive into python tools to deal with concurrency

## Running the demos
The repo assumes the use of uv. (https://docs.astral.sh/uv/)
If you are a pip user you can definitely upgrade to use uv now :P
Your life will be easier, faster and more secure by using locked dependencies
and you can thank me later.

# Run individual demos:
`uv run 01_sequential/demo.py`

`uv run 02_threads/demo.py`

`uv run 03_multiprocessing/demo.py`

`uv run 04_asyncio/demo.py`

# Run benchmark comparisons:
`uv run 05_benchmarks/cpu_bound.py`

`uv run 05_benchmarks/io_bound.py`

`uv run 05_benchmarks/http_server.py`
