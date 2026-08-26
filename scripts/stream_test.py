#!/usr/bin/env python3
"""Measure SSE streaming behavior: direct vs through the WSP proxy.

Emits a timestamp for every chunk as it arrives. If streaming is preserved,
chunks arrive ~400ms apart. If the proxy buffers, all chunks arrive at once
after the full response completes (~2.4s).
"""
import time, requests, sys

API = "http://127.0.0.1:18081/stream"
SRV = "http://127.0.0.1:18080/request"

def measure(label, url, headers):
    s = requests.Session()
    t0 = time.perf_counter()
    r = s.get(url, headers=headers, stream=True, timeout=30)
    # response headers / first byte
    t_hdr = time.perf_counter() - t0
    ctype = r.headers.get("content-type", "?")
    tenc  = r.headers.get("transfer-encoding", "-")
    clen  = r.headers.get("content-length", "-")
    print(f"  [{label}] first-byte(headers)={t_hdr*1000:.0f}ms  "
          f"content-type={ctype}  transfer-encoding={tenc}  content-length={clen}")
    n = 0
    first_chunk_t = None
    for chunk in r.iter_content(chunk_size=None):
        if not chunk:
            continue
        if first_chunk_t is None:
            first_chunk_t = time.perf_counter() - t0
        n += 1
        elapsed = time.perf_counter() - t0
        txt = chunk.decode("utf-8", "replace").strip().replace("\n", " | ")
        print(f"    chunk#{n} @ {elapsed*1000:6.0f}ms  ({len(chunk)}B)  {txt[:60]}")
    total = time.perf_counter() - t0
    print(f"  [{label}] done: {n} chunks, total={total*1000:.0f}ms, "
          f"first-chunk={first_chunk_t*1000:.0f}ms" if first_chunk_t else f"  [{label}] no chunks")
    print()

def main():
    print("== direct (caller -> test_api /stream) ==")
    measure("direct", API, {})

    print("== through WSP (caller -> wsp_server -> wsp_client -> test_api /stream) ==")
    measure("wsp", SRV, {"X-PROXY-DESTINATION": API})

if __name__ == "__main__":
    main()
