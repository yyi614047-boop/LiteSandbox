"""Measure a running LiteSandbox service. Use a dedicated service for comparisons."""

import argparse
import json
import math
import time
from concurrent.futures import ThreadPoolExecutor

from litesandbox.client import LiteSandboxClient


def measure(port: int, attempts: int, concurrency: int) -> dict:
    def trial(_):
        client = LiteSandboxClient(port)
        sample = {}
        started = time.perf_counter()
        try:
            info = client.create_info()
            sample.update(sandbox_id=info["sandbox_id"], hit=info["pool_hit"])
            created = time.perf_counter()
            sample["create_ms"] = (created - started) * 1000
            result = client.exec(info["sandbox_id"], ["true"])
            if result["exit_code"] != 0:
                raise RuntimeError(str(result))
            finished = time.perf_counter()
            sample["exec_ms"] = (finished - created) * 1000
            sample["ready_ms"] = (finished - started) * 1000
        except Exception as error:
            sample["error"] = str(error)
        finally:
            if "sandbox_id" in sample:
                try:
                    client.delete(sample["sandbox_id"])
                except Exception as error:
                    sample["cleanup_error"] = str(error)
        return sample

    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        samples = list(executor.map(trial, range(attempts)))
    ready = [row for row in samples if "ready_ms" in row]

    def percentiles(key):
        values = sorted(row[key] for row in ready)
        if not values:
            return {}
        return {f"p{p}": round(values[math.ceil(p / 100 * len(values)) - 1], 1) for p in (50, 95)}

    ids = [row["sandbox_id"] for row in samples if "sandbox_id" in row]
    live = {row["sandbox_id"] for row in LiteSandboxClient(port).list()}
    return {
        "attempts": attempts, "concurrency": concurrency, "ready": len(ready),
        "pool_hits": sum(row.get("hit", False) for row in samples),
        "create_ms": percentiles("create_ms"),
        "exec_ms": percentiles("exec_ms"),
        "ready_ms": percentiles("ready_ms"),
        "duplicate_ids": len(ids) - len(set(ids)), "remaining_leases": len(live.intersection(ids)),
        "errors": [row[key] for row in samples for key in ("error", "cleanup_error") if key in row],
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--attempts", type=int, default=30)
    parser.add_argument("--concurrency", type=int, default=10)
    args = parser.parse_args()
    if min(args.attempts, args.concurrency) < 1:
        parser.error("attempts and concurrency must be positive")
    print(json.dumps(measure(args.port, args.attempts, args.concurrency), indent=2))
