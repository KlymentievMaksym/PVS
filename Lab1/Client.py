import asyncio
import aiohttp
import time


class HttpClient:
    def __init__(self, base_url: str, concurrency: int = 100):
        self.base_url = base_url.rstrip('/')
        self.concurrency = concurrency

    async def _fetch(self, session: aiohttp.ClientSession, endpoint: str):
        async with session.get(f"{self.base_url}/{endpoint}") as resp:
            return await resp.text()

    async def _worker(self, session, endpoint, num_requests):
        tasks = [self._fetch(session, endpoint) for _ in range(num_requests)]
        await asyncio.gather(*tasks)

    async def run_load_test(self, endpoint: str, total_requests: int, name: str = "Memory"):
        start_time = time.perf_counter()
        async with aiohttp.ClientSession() as session:
            per_worker = total_requests // self.concurrency
            tasks = [
                self._worker(session, endpoint, per_worker)
                for _ in range(self.concurrency)
            ]
            await asyncio.gather(*tasks)

            final_value = await self._fetch(session, "cnt")
        duration = time.perf_counter() - start_time
        throughput = total_requests / duration
        print(f"[Name] {name}")
        print(f"[Sent] {total_requests} requests in {duration:.2f} seconds")
        print(f"[Count] {final_value}")
        print(f"[Throughput]: {throughput:.2f} requests/sec")
        print()
        return throughput


if __name__ == "__main__":
    client = HttpClient("http://localhost:8000", concurrency=200)
    asyncio.run(client.run_load_test("inc", total_requests=10_000))
