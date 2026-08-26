#!/usr/bin/env python3
"""Multiprocess throughput check for HOW proxy (bypass the GIL).

Each worker process keeps one persistent `requests.Session` and fires
sequential GET /hello for a fixed wall time. Workers run in separate
processes, so the aggregate is not capped by the Python GIL the way the
threaded `requests` bench is. Reports direct vs proxied RPS at several
worker counts.
"""
import multiprocessing
import time

import requests

UP = "http://127.0.0.1:18081"
SRV = "http://127.0.0.1:18080"
DUR = 5.0  # seconds per measurement


def worker(args):
    url, dur, seed = args
    s = requests.Session()
    # warmup
    for _ in range(5):
        try:
            s.get(url, timeout=5)
        except Exception:
            return 0
    end = time.perf_counter() + dur
    n = 0
    while time.perf_counter() < end:
        try:
            s.get(url, timeout=5)
            n += 1
        except Exception:
            pass
    return n


def measure(url, workers, dur=DUR):
    pool = multiprocessing.Pool(workers)
    t0 = time.perf_counter()
    counts = pool.map(worker, [(url, dur, i) for i in range(workers)])
    wall = time.perf_counter() - t0
    pool.close()
    pool.join()
    total = sum(counts)
    return total, wall


def main():
    print("Multiprocess throughput (GET /hello, {}s per run, one Session/proc)".format(DUR))
    print("  {:6s}  {:>16s}  {:>16s}  {:>9s}  {:>8s}".format(
        "procs", "direct rps", "proxied rps", "ratio", "loss"))
    for w in (8, 16, 32, 64):
        d, dw = measure(UP + "/hello", w)
        p, pw = measure(SRV + "/hello", w)
        drps = d / dw
        prps = p / pw
        ratio = prps / drps if drps else 0
        loss = (1 - ratio) * 100
        print("  procs={:<3d}  {:>9.1f} rps ({:.2f}s)  {:>9.1f} rps ({:.2f}s)  {:>7.3f}x  {:>6.1f}%".format(
            w, drps, dw, prps, pw, ratio, loss))


if __name__ == "__main__":
    main()
