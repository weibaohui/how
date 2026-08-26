#!/usr/bin/env python3
"""HOW proxy overhead benchmark.

Compares request latency and throughput through the HOW proxy
(caller -> how-server ->[WebSocket]-> how-client -> upstream) against a
direct connection to the SAME upstream (how-test-api). Because both paths
terminate at the same upstream, the measured delta is the proxy's own
processing overhead: HTTP<->WebSocket framing, the dispatcher hand-off,
header JSON serialize/deserialize, and the extra loopback hop.

All sockets are loopback (127.0.0.1), so this isolates proxy *processing*
overhead. A real WAN hop between server and client would add its RTT on top.

Layout (ports chosen to avoid the example/e2e configs):
    how-test-api   127.0.0.1:18081   (fake upstream API)
    how-server     127.0.0.1:18080   (proxy entry, catch-all)
    how-client     -> 18080/register, routes 127.0.0.1:18080 -> 18081

Requires: Python 3 + `requests`. Run:
    ./scripts/bench_how.py
"""
import concurrent.futures
import statistics
import sys
import time

import requests

UP = "http://127.0.0.1:18081"   # direct / upstream (how-test-api)
SRV = "http://127.0.0.1:18080"  # proxy entry (how-server)


def pct(xs, p):
    if not xs:
        return 0.0
    xs = sorted(xs)
    k = max(0, min(len(xs) - 1, int(round(p / 100.0 * (len(xs) - 1)))))
    return xs[k]


def latency(fn, n=300, warmup=30):
    """Sequential latency in ms. fn() should perform one full request."""
    s = requests.Session()
    for _ in range(warmup):
        r = fn(s)
        if r is None:
            return None
    ts = []
    for _ in range(n):
        t0 = time.perf_counter()
        fn(s)
        ts.append((time.perf_counter() - t0) * 1000.0)
    return dict(mean=statistics.mean(ts), p50=pct(ts, 50),
                p95=pct(ts, 95), p99=pct(ts, 99), n=n)


def throughput(fn, conc, total=2000, warmup=None):
    """Concurrent RPS. fn(session) performs one request."""
    s = requests.Session()
    w = warmup if warmup is not None else conc
    for _ in range(w):
        fn(s)
    barrier = concurrent.futures.ThreadPoolExecutor(max_workers=1)

    def one(_):
        t0 = time.perf_counter()
        fn(s)
        return time.perf_counter() - t0

    t0 = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=conc) as ex:
        list(ex.map(one, range(total)))
    dt = time.perf_counter() - t0
    return dict(rps=total / dt, wall=dt, conc=conc, total=total)


def fmt_lat_row(name, r):
    print("  {:30s} mean={:7.3f}ms  p50={:7.3f}  p95={:7.3f}  p99={:7.3f}  (n={})".format(
        name, r["mean"], r["p50"], r["p95"], r["p99"], r["n"]))


def fmt_overhead(d, p):
    dm = d["mean"] - p["mean"]
    dp = d["p50"] - p["p50"]
    inc = (d["mean"] / p["mean"] - 1) * 100 if p["mean"] else 0.0
    print("  {:30s}  -> overhead: {:+.3f}ms mean ({:+.1f}%)  {:+.3f}ms p50".format(
        "", dm, inc, dp))


def get(session, url):
    return session.get(url)


def post(session, url, data):
    return session.post(url, data=data)


def main():
    # Sanity: both endpoints must respond before we measure.
    try:
        requests.get(UP + "/hello", timeout=5).raise_for_status()
        requests.get(SRV + "/status", timeout=5).raise_for_status()
        # Warm the WS pool (client dials idle connections on startup).
        for _ in range(20):
            requests.get(SRV + "/hello", timeout=5).raise_for_status()
    except Exception as e:
        print("ERROR: endpoints not ready: {}".format(e), file=sys.stderr)
        print("  start: how-test-api -addr 127.0.0.1:18081, "
              "how-server -config bench/config.server.bench.cfg, "
              "how-client -config bench/config.client.bench.cfg", file=sys.stderr)
        sys.exit(1)

    print("=" * 78)
    print("HOW proxy overhead benchmark  (loopback, upstream = how-test-api)")
    print("  direct   : caller -> {}".format(UP))
    print("  proxied  : caller -> {} ->[WS]-> how-client -> {}".format(SRV, UP))
    print("=" * 78)

    # ---- 1. Small GET latency (12-byte body) -------------------------------
    print("\n[1] Small GET /hello latency (12-byte response, sequential)")
    d = latency(lambda s: get(s, UP + "/hello"))
    p = latency(lambda s: get(s, SRV + "/hello"))
    fmt_lat_row("direct (test_api)", d)
    fmt_lat_row("proxied (HOW)", p)
    fmt_overhead(p, d)

    # ---- 2. Small POST 256B echo latency -----------------------------------
    print("\n[2] Small POST /post echo latency (256B body, sequential)")
    small = b"x" * 256
    d = latency(lambda s: post(s, UP + "/post", small))
    p = latency(lambda s: post(s, SRV + "/post", small))
    fmt_lat_row("direct (test_api)", d)
    fmt_lat_row("proxied (HOW)", p)
    fmt_overhead(p, d)

    # ---- 3. Downstream payload latency at various sizes --------------------
    print("\n[3] Downstream GET /bytes?n=N latency (sequential)")
    print("  {:7s}  {:>20s}  {:>20s}  {:>14s}  {:>10s}".format(
        "size", "direct mean/MBps", "proxied mean/MBps", "overhead", "slowdown"))
    for n in (1024, 8 * 1024, 64 * 1024, 256 * 1024, 1024 * 1024):
        d = latency(lambda s, n=n: get(s, UP + "/bytes?n={}".format(n)), n=60, warmup=10)
        p = latency(lambda s, n=n: get(s, SRV + "/bytes?n={}".format(n)), n=60, warmup=10)
        if not d or not p:
            continue
        mb = n / (1024 * 1024)
        d_mbps = mb / (d["mean"] / 1000.0) if d["mean"] else 0
        p_mbps = mb / (p["mean"] / 1000.0) if p["mean"] else 0
        ovh = p["mean"] - d["mean"]
        slow = (p["mean"] / d["mean"]) if d["mean"] else 0
        print("  {:>5s}   {:>8.3f}ms {:6.1f}MB/s  {:>8.3f}ms {:6.1f}MB/s  {:+8.3f}ms  {:6.2f}x".format(
            human(n), d["mean"], d_mbps, p["mean"], p_mbps, ovh, slow))

    # ---- 4. Throughput: small GET at concurrency ---------------------------
    print("\n[4] Throughput: small GET /hello (concurrent)")
    print("  {:6s}  {:>18s}  {:>18s}  {:>12s}  {:>10s}".format(
        "conc", "direct rps", "proxied rps", "rps ratio", "overhead"))
    for conc in (8, 32, 64):
        d = throughput(lambda s: get(s, UP + "/hello"), conc=conc, total=3000)
        p = throughput(lambda s: get(s, SRV + "/hello"), conc=conc, total=3000)
        ratio = p["rps"] / d["rps"] if d["rps"] else 0
        ovh_pct = (1 - ratio) * 100
        print("  conc={:<2d}  {:>9.1f} rps ({:.2f}s)  {:>9.1f} rps ({:.2f}s)  {:>10.3f}x  {:>8.1f}%".format(
            conc, d["rps"], d["wall"], p["rps"], p["wall"], ratio, ovh_pct))

    # ---- 5. Throughput: 1MB downstream at low concurrency -------------------
    print("\n[5] Throughput: 1MB downstream GET /bytes?n=1048576 (concurrent)")
    for conc in (1, 4):
        d = throughput(lambda s: get(s, UP + "/bytes?n=1048576"),
                       conc=conc, total=20 if conc == 1 else 40, warmup=2)
        p = throughput(lambda s: get(s, SRV + "/bytes?n=1048576"),
                       conc=conc, total=20 if conc == 1 else 40, warmup=2)
        d_mbps = (d["total"] * 1.0) / d["wall"]  # MiB/s (1MB payload)
        p_mbps = (p["total"] * 1.0) / p["wall"]
        print("  conc={:<2d}  direct {:.1f} MiB/s ({:.2f}s)   proxied {:.1f} MiB/s ({:.2f}s)   ratio {:.3f}x".format(
            conc, d_mbps, d["wall"], p_mbps, p["wall"], p_mbps / d_mbps if d_mbps else 0))

    print("\nNote: all sockets loopback; the proxied path adds one extra loopback hop")
    print("plus WebSocket framing + header JSON round-trip. A real server<->client")
    print("WAN link would add its RTT to every proxied request on top of this.")


def human(n):
    if n >= 1024 * 1024:
        return "{}MB".format(n // (1024 * 1024))
    if n >= 1024:
        return "{}KB".format(n // 1024)
    return "{}B".format(n)


if __name__ == "__main__":
    main()
