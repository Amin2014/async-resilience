"""Run: python examples/local_demo.py. Uses only loopback and the standard library."""

import asyncio
import json
import logging
from collections import Counter
from contextlib import aclosing
from dataclasses import asdict

from async_resilience import Config, Request, ResilientClient


async def main() -> None:
    counts: Counter[str] = Counter()

    async def serve(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            async with asyncio.timeout(5):
                line = await reader.readline()
                path = line.decode().split()[1]
                while await reader.readline() not in (b"\r\n", b""):
                    pass
                counts[path] += 1
                status, extra = 200, ""
                if path == "/rate-limit" and counts[path] == 1:
                    status, extra = 429, "Retry-After: 1\r\n"
                elif path == "/flaky" and counts[path] <= 2:
                    status = 503
                elif path == "/slow":
                    await asyncio.sleep(0.3)
                body = json.dumps({"path": path, "attempt": counts[path]}).encode()
                writer.write(
                    f"HTTP/1.1 {status} Demo\r\nContent-Length: {len(body)}\r\n"
                    f"Connection: close\r\n{extra}\r\n".encode()
                    + body
                )
                await writer.drain()
        except (ConnectionError, TimeoutError):
            pass
        finally:
            writer.close()
            await writer.wait_closed()

    handlers: set[asyncio.Task[None]] = set()

    def connected(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        task = asyncio.create_task(serve(reader, writer))
        handlers.add(task)
        task.add_done_callback(handlers.discard)

    server = await asyncio.start_server(connected, "127.0.0.1", 0)
    base = f"http://127.0.0.1:{server.sockets[0].getsockname()[1]}"
    try:
        async with (
            server,
            ResilientClient(
                Config(
                    concurrency=4,
                    per_host_concurrency=2,
                    timeout=0.1,
                    max_retries=2,
                    backoff_base=0.02,
                    failure_threshold=20,
                )
            ) as client,
        ):
            jobs = (
                Request("GET", base + path) for path in ["/ok", "/flaky", "/rate-limit", "/slow"]
            )
            async with aclosing(client.map(jobs)) as results:
                async for item in results:
                    outcome = (
                        str(item.response.status_code)
                        if item.response is not None
                        else type(item.error).__name__
                    )
                    print(f"{item.request.url.rsplit('/', 1)[-1]}: {outcome}")
            print(json.dumps(asdict(client.statistics()), sort_keys=True))
    finally:
        for task in handlers:
            task.cancel()
        await asyncio.gather(*handlers, return_exceptions=True)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
