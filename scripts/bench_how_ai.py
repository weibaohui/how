#!/usr/bin/env python3
"""HOW proxy: AI large-context request test.

Simulates an OpenAI-style chat-completions request carrying a very large
"context" (a ~1MB / ~4MB request body, i.e. a ~1M-token-class prompt), sent
both directly to how-test-api and through the HOW proxy. Measures:

  * upload latency + upload throughput (MB/s) for non-streaming requests,
    with an integrity check: the upstream echoes `usage.prompt_bytes` ==
    body length, proving the full body arrived through the proxy.
  * streaming (SSE) time-to-first-token and total time after a large upload,
    proving streaming still works end-to-end through the proxy.

All loopback. The delta = the proxy's own upload-piping + framing overhead.
"""
import json
import statistics
import sys
import time

import requests

UP = "http://127.0.0.1:18081"   # direct / upstream (how-test-api)
SRV = "http://127.0.0.1:18080"  # proxy entry (how-server)
CHAT = "/v1/chat/completions"


def make_body(target_bytes, stream=False):
    """Build a chat-completions JSON body of ~target_bytes by padding content."""
    if stream:
        suffix = '"}],"stream":true}'
    else:
        suffix = '"}]}'
    base = '{"model":"bench","messages":[{"role":"user","content":"'
    pad = max(0, target_bytes - len(base) - len(suffix))
    return (base + "a" * pad + suffix).encode(), pad


def pct(xs, p):
    if not xs:
        return 0.0
    xs = sorted(xs)
    k = max(0, min(len(xs) - 1, int(round(p / 100.0 * (len(xs) - 1)))))
    return xs[k]


def latency_nonstream(url, body, n=20, warmup=5):
    """POST body, measure ms, verify prompt_bytes == len(body)."""
    s = requests.Session()
    for _ in range(warmup):
        r = s.post(url + CHAT, data=body, timeout=60)
        r.raise_for_status()
    ts = []
    got = None
    for _ in range(n):
        t0 = time.perf_counter()
        r = s.post(url + CHAT, data=body, timeout=60)
        dt = (time.perf_counter() - t0) * 1000.0
        r.raise_for_status()
        got = r.json().get("usage", {}).get("prompt_bytes")
        ts.append(dt)
    return dict(mean=statistics.mean(ts), p50=pct(ts, 50), p95=pct(ts, 95),
                n=n, prompt_bytes=got)


def stream_once(url, body):
    """POST with stream:true; return (ttfb_ms, total_ms, n_data_lines, ok)."""
    s = requests.Session()
    t0 = time.perf_counter()
    r = s.post(url + CHAT, data=body, stream=True, timeout=60)
    r.raise_for_status()
    ttfb = None
    ndata = 0
    ok_done = False
    for line in r.iter_lines():
        if not line:
            continue
        if ttfb is None:
            ttfb = (time.perf_counter() - t0) * 1000.0
        if line.startswith(b"data:"):
            ndata += 1
            if line.endswith(b"[DONE]"):
                ok_done = True
    total = (time.perf_counter() - t0) * 1000.0
    return ttfb, total, ndata, ok_done


def main():
    # Sanity.
    try:
        requests.get(UP + "/hello", timeout=5).raise_for_status()
        requests.get(SRV + "/status", timeout=5).raise_for_status()
        for _ in range(20):
            requests.get(SRV + "/hello", timeout=5).raise_for_status()
    except Exception as e:
        print("ERROR: endpoints not ready: {}".format(e), file=sys.stderr)
        sys.exit(1)

    print("=" * 78)
    print("HOW proxy: AI large-context request test (loopback)")
    print("  endpoint: POST {}/v1/chat/completions".format(SRV + " | " + UP))
    print("=" * 78)

    # ---- Non-streaming: large upload latency + throughput + integrity ----
    print("\n[1] Large-context upload (non-streaming chat completion)")
    print("  {:>7s}  {:>22s}  {:>22s}  {:>9s}  {:>9s}  {:>6s}  {:>5s}".format(
        "size", "direct mean / up MB/s", "proxied mean / up MB/s",
        "overhead", "slowdown", "integ", "x"))
    for target in (256 * 1024, 1024 * 1024, 4 * 1024 * 1024):
        body, _pad = make_body(target, stream=False)
        blen = len(body)
        d = latency_nonstream(UP, body, n=15, warmup=3)
        p = latency_nonstream(SRV, body, n=15, warmup=3)
        d_up = blen / (d["mean"] / 1000.0) / (1024 * 1024) if d["mean"] else 0
        p_up = blen / (p["mean"] / 1000.0) / (1024 * 1024) if p["mean"] else 0
        ovh = p["mean"] - d["mean"]
        slow = (p["mean"] / d["mean"]) if d["mean"] else 0
        # integrity: did the upstream see the full body length via the proxy?
        ok = "OK" if p["prompt_bytes"] == blen else "FAIL:%s" % p["prompt_bytes"]
        print("  {:>7s}  {:8.2f}ms {:7.1f}MB/s  {:8.2f}ms {:7.1f}MB/s  {:+7.2f}ms  {:6.2f}x  {:>5s}".format(
            human(blen), d["mean"], d_up, p["mean"], p_up, ovh, slow, ok))

    # ---- Streaming: TTFT + total after a 1MB upload ----
    print("\n[2] Streaming completion (SSE) after large upload")
    for target in (256 * 1024, 1024 * 1024):
        body, _pad = make_body(target, stream=True)
        blen = len(body)
        d_ttfb, d_tot, d_n, d_ok = stream_once(UP, body)
        p_ttfb, p_tot, p_n, p_ok = stream_once(SRV, body)
        print("  {:>7s}  direct  TTFB={:6.1f}ms total={:6.1f}ms lines={} done={}".format(
            human(blen), d_ttfb, d_tot, d_n, d_ok))
        print("  {:>7s}  proxied TTFB={:6.1f}ms total={:6.1f}ms lines={} done={}  (TTFB +{:.1f}ms)".format(
            human(blen), p_ttfb, p_tot, p_n, p_ok, p_ttfb - d_ttfb))

    print("\nIntegrity: 'prompt_bytes' echoed by the upstream must equal the sent")
    print("body length; SSE must deliver all chunks incl. [DONE] end-to-end.")


def human(n):
    if n >= 1024 * 1024:
        return "{}MB".format(round(n / (1024 * 1024), 2))
    if n >= 1024:
        return "{}KB".format(n // 1024)
    return "{}B".format(n)


if __name__ == "__main__":
    main()
