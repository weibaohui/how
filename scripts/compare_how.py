#!/usr/bin/env python3
"""Controlled A/B: baseline (HEAD) vs #2 (zero-copy) across loads.

Uses two pre-built binary sets under /tmp/howcmp/{base,v2} plus a shared
how-test-api. For each load config, alternates base <-> v2 (2 rounds each)
to cancel machine-load drift, restarting only how-server + how-client
(test_api stays up). Reports the per-config delta and %.

Build the two variants first:
  # v2 (#2):  cp target/release/how-{server,client} /tmp/howcmp/v2/
  # base:     git checkout the two connection.rs, build, cp to /tmp/howcmp/base/
  cp target/release/how-test-api /tmp/howcmp/
"""
import concurrent.futures
import multiprocessing
import os
import statistics
import subprocess
import sys
import time

import requests

WORK = "/media/Admin/数据盘1/projects/wsp"
TESTAPI = "/tmp/howcmp/how-test-api"
VARIANTS = {"base": "/tmp/howcmp/base", "v2": "/tmp/howcmp/v2"}
SRV_CFG = f"{WORK}/bench/config.server.bench.cfg"
CLI_CFG = f"{WORK}/bench/config.client.bench.cfg"
UP = "http://127.0.0.1:18081"
SRV = "http://127.0.0.1:18080"

_cur = []


def _stop():
    for p in _cur:
        try:
            p.terminate()
            p.wait(timeout=5)
        except Exception:
            try:
                p.kill()
            except Exception:
                pass
    _cur.clear()


def _wait(url, timeout=15):
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            r = requests.get(url, timeout=1)
            if r.status_code < 500:
                return True
        except Exception:
            pass
        time.sleep(0.2)
    return False


def start_variant(variant):
    _stop()
    server = os.path.join(VARIANTS[variant], "how-server")
    client = os.path.join(VARIANTS[variant], "how-client")
    _cur.append(subprocess.Popen([server, "-config", SRV_CFG],
                                 stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
    time.sleep(0.4)
    _cur.append(subprocess.Popen([client, "-config", CLI_CFG],
                                 stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
    if not _wait(SRV + "/status"):
        raise RuntimeError("server not ready")
    for _ in range(20):
        try:
            requests.get(SRV + "/hello", timeout=3)
        except Exception:
            pass


# ---- measurement functions (return a single summary number) ----

def m_lat_small(n=300, w=30):
    s = requests.Session()
    for _ in range(w):
        s.get(SRV + "/hello", timeout=5)
    ts = []
    for _ in range(n):
        t0 = time.perf_counter()
        s.get(SRV + "/hello", timeout=5)
        ts.append((time.perf_counter() - t0) * 1000)
    return statistics.mean(ts)


def m_lat_1mb(n=40, w=5):
    s = requests.Session()
    for _ in range(w):
        s.get(SRV + "/bytes?n=1048576", timeout=30)
    ts = []
    for _ in range(n):
        t0 = time.perf_counter()
        s.get(SRV + "/bytes?n=1048576", timeout=30)
        ts.append((time.perf_counter() - t0) * 1000)
    return statistics.mean(ts)


def _worker(args):
    url, dur = args
    s = requests.Session()
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


def m_thr_small(procs=16, dur=5):
    pool = multiprocessing.Pool(procs)
    t0 = time.perf_counter()
    counts = pool.map(_worker, [(SRV + "/hello", dur)] * procs)
    wall = time.perf_counter() - t0
    pool.close()
    pool.join()
    return sum(counts) / wall


def m_thr_1mb(procs=4, dur=5):
    pool = multiprocessing.Pool(procs)
    t0 = time.perf_counter()
    counts = pool.map(_worker, [(SRV + "/bytes?n=1048576", dur)] * procs)
    wall = time.perf_counter() - t0
    pool.close()
    pool.join()
    return (sum(counts) * 1.0) / wall  # MiB/s (1MB payload)


# load configs: (name, measurement_fn)
CONFIGS = [
    ("small GET latency (n=300)", m_lat_small),
    ("1MB downstream latency (n=40)", m_lat_1mb),
    ("small GET rps @16", lambda: m_thr_small(16)),
    ("small GET rps @64", lambda: m_thr_small(64)),
    ("1MB downstream MiB/s @4", lambda: m_thr_1mb(4)),
]

ROUNDS = 2  # alternating rounds per config


def main():
    # shared upstream
    ta = subprocess.Popen([TESTAPI, "-addr", "127.0.0.1:18081"],
                          stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if not _wait(UP + "/hello"):
        print("ERROR: test_api not ready", file=sys.stderr)
        sys.exit(1)
    print("Controlled A/B: baseline (HEAD) vs v2 (#2 zero-copy). Alternating, "
          "{} rounds each.\n".format(ROUNDS))
    try:
        results = {name: {"base": [], "v2": []} for name, _ in CONFIGS}
        for name, fn in CONFIGS:
            order = []
            for r in range(ROUNDS):
                order += ["base", "v2"]  # alternate within the config window
            for variant in order:
                start_variant(variant)
                val = fn()
                results[name][variant].append(val)
                print("  {:32s} {:4s} round -> {:.2f}".format(name, variant, val))
            b = statistics.mean(results[name]["base"])
            v = statistics.mean(results[name]["v2"])
            delta = v - b
            pct = (delta / b * 100) if b else 0
            tag = "faster" if delta < 0 else "slower"
            print("  {:32s}  base={:.2f}  v2={:.2f}  delta={:+.2f} ({:+.1f}%)  {}".format(
                "  >> " + name, b, v, delta, pct, tag))
            print()
    finally:
        _stop()
        ta.terminate()
        try:
            ta.wait(timeout=5)
        except Exception:
            pass


if __name__ == "__main__":
    main()
