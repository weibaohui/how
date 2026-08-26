#!/usr/bin/env python3
import requests, time, statistics, concurrent.futures, sys

API = "http://127.0.0.1:18081"
SRV = "http://127.0.0.1:18080"
HELLO_API = f"{API}/hello"
WSP_REQ = f"{SRV}/request"
WSP_DST = HELLO_API

def wsp_headers():
    return {"X-PROXY-DESTINATION": WSP_DST}

def pct(xs, p):
    if not xs: return 0
    xs = sorted(xs)
    k = max(0, min(len(xs)-1, int(round((p/100.0)*(len(xs)-1)))))
    return xs[k]

def latency(name, fn, n=200, warmup=20):
    # warmup
    for _ in range(warmup):
        try: fn()
        except Exception as e: print(f"  {name} warmup err: {e}"); return
    ts=[]
    for _ in range(n):
        t0=time.perf_counter()
        fn()
        ts.append((time.perf_counter()-t0)*1000.0)
    print(f"  {name:28s} n={n}  mean={statistics.mean(ts):6.2f}ms  p50={pct(ts,50):6.2f}  p95={pct(ts,95):6.2f}  p99={pct(ts,99):6.2f}")

def rps(name, fn, conc, total=1000):
    sess = requests.Session()
    def one(_):
        t0=time.perf_counter()
        fn(sess)
        return time.perf_counter()-t0
    # warmup
    for _ in range(conc): fn(sess)
    t0=time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=conc) as ex:
        list(ex.map(one, range(total)))
    dt=time.perf_counter()-t0
    print(f"  {name:28s} conc={conc:3d} total={total}  rps={total/dt:7.1f}  wall={dt:.2f}s")

def main():
    s = requests.Session()
    def direct_get(): s.get(HELLO_API).text
    def wsp_get():    s.get(WSP_REQ, headers=wsp_headers()).text

    small = b"x" * 256
    def direct_post(): s.post(f"{API}/post", data=small).text
    def wsp_post():    s.post(WSP_REQ, headers=wsp_headers(), data=small).text

    big = b"y"*65536
    def direct_big(): s.post(f"{API}/post", data=big).content
    def wsp_big():    s.post(WSP_REQ, headers=wsp_headers(), data=big).content

    print("== small GET latency ==")
    latency("direct (test_api)", direct_get)
    latency("wsp (proxy)", wsp_get)

    print("== small POST (256B) latency ==")
    latency("direct (test_api)", direct_post)
    latency("wsp (proxy)", wsp_post)

    print("== 64KB POST latency (shows buffering cost) ==")
    latency("direct (test_api)", direct_big, n=100)
    latency("wsp (proxy)", wsp_big, n=100)

    print("== throughput (small GET) ==")
    sd = requests.Session(); sw = requests.Session()
    def dget(sess): sess.get(HELLO_API).text
    def wget(sess): sess.get(WSP_REQ, headers=wsp_headers()).text
    rps("direct (test_api)", dget, 16)
    rps("wsp (proxy)",      wget, 16)
    rps("direct (test_api)", dget, 64)
    rps("wsp (proxy)",      wget, 64)

if __name__ == "__main__":
    main()
