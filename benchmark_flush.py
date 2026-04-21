import asyncio
import time
import os
import shutil
from pathlib import Path
import pandas as pd
from collections import namedtuple

from score_openrouter import _flush_batch
from composite_formula import load_composite_config, DEFAULT_DIRECT_POINT_MAPS
from score_openrouter import set_direct_point_help

async def ping_loop():
    # Measure event loop responsiveness
    max_delay = 0
    start = time.monotonic()
    for i in range(100):
        t0 = time.monotonic()
        await asyncio.sleep(0.01)
        delay = time.monotonic() - t0 - 0.01
        if delay > max_delay:
            max_delay = delay
    total_time = time.monotonic() - start
    return max_delay, total_time

async def main():
    config = load_composite_config("scoring_rubric.md")
    set_direct_point_help(config.direct_point_maps)

    # Create fake data
    batch_out = []
    meta_by_index = {}
    for i in range(500):
        batch_out.append({
            "_seq": i,
            "Match Key": f"mk_{i}",
            "fo_persona": 1,
            "ft_persona": 1,
            "allocator": 1,
            "access": 1,
            "company_fit": 1,
        })
        meta_by_index[i] = {
            "URN": f"urn_{i}",
            "Raw ID": f"raw_{i}",
            "Best Email": f"email_{i}",
            "Full Name": f"Name {i}",
            "Current Company": f"Company {i}",
        }

    out_dir = Path("bench_out")
    out_dir.mkdir(exist_ok=True)
    results_csv = out_dir / "results.csv"
    progress_jsonl = out_dir / "progress.jsonl"

    if results_csv.exists(): results_csv.unlink()
    if progress_jsonl.exists(): progress_jsonl.unlink()

    io_lock = asyncio.Lock()
    counter_ref = [0]
    scoring_mode = "legacy_raw_weighted"

    # We will run _flush_batch many times concurrently
    async def flush_many():
        for i in range(20):
            await _flush_batch(
                batch_out,
                meta_by_index=meta_by_index,
                results_csv=results_csv,
                progress_jsonl=progress_jsonl,
                io_lock=io_lock,
                counter_ref=counter_ref,
                scoring_mode=scoring_mode,
                composite_config=config,
            )
            await asyncio.sleep(0) # yield

    # Start ping loop and flush loop
    t0 = time.monotonic()
    ping_task = asyncio.create_task(ping_loop())
    flush_task = asyncio.create_task(flush_many())

    max_delay, ping_time = await ping_task
    await flush_task
    total_time = time.monotonic() - t0

    print(f"Max event loop delay: {max_delay:.4f}s")
    print(f"Total time: {total_time:.4f}s")
    print(f"Records written: {counter_ref[0]}")

    shutil.rmtree(out_dir)

if __name__ == "__main__":
    asyncio.run(main())
