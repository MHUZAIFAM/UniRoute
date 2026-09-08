#!/usr/bin/env python3
"""
UNIROUTE - validateDomains.py

Checks the domain we hold for every university and classifies what is wrong
with it. Missing domains are only part of the problem; a domain that is dead,
parked, or now redirects to an unrelated site is worse, because nothing in the
app signals that it is untrustworthy.

    python validateDomains.py [--limit N] [--workers 40] [--stage dns|http]

Stage 1 (dns)  - resolve every domain. Cheap, catches dead/typo domains.
Stage 2 (http) - for those that resolve, make a request and follow redirects,
                 recording the final host so silent rebrands and domain
                 takeovers surface.

Statuses:
    ok                 resolves, responds, stays on the same registered domain
    missing            no domain recorded at all
    dns-fail           does not resolve - dead or mistyped
    no-response        resolves but refuses/times out
    http-error         responds with 4xx/5xx
    redirect-offsite   lands on a different registered domain (needs a look)

Results checkpoint to CSV per row and the run resumes, so it can be stopped
and restarted freely.
"""

import argparse
import csv
import os
import socket
import ssl
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

import psycopg2

UA = "UnirouteDomainCheck/1.0 (+educational directory)"
DNS_TIMEOUT = 5
HTTP_TIMEOUT = 12
_lock = threading.Lock()

# Multi-part public suffixes we must not truncate to two labels
MULTI_TLD = {
    "ac.uk", "co.uk", "org.uk", "gov.uk", "ac.jp", "co.jp", "or.jp", "ne.jp",
    "ac.nz", "co.nz", "ac.za", "co.za", "edu.au", "gov.au", "com.au", "org.au",
    "edu.cn", "com.cn", "edu.in", "ac.in", "edu.sg", "com.sg", "edu.my",
    "edu.pk", "edu.br", "com.br", "edu.mx", "com.mx", "edu.tr", "edu.ar",
    "com.ar", "co.kr", "ac.kr", "ac.th", "co.th", "edu.hk", "edu.tw", "com.tw",
    "ac.ir", "edu.eg", "edu.sa", "edu.co", "edu.pe", "edu.ph", "ac.id",
}


def registered_domain(host):
    """Reduce a hostname to its registered domain, honouring multi-part TLDs."""
    host = (host or "").lower().strip().rstrip(".")
    if host.startswith("www."):
        host = host[4:]
    parts = host.split(".")
    if len(parts) <= 2:
        return host
    if ".".join(parts[-2:]) in MULTI_TLD and len(parts) >= 3:
        return ".".join(parts[-3:])
    return ".".join(parts[-2:])


def host_of(domain, web):
    raw = (domain or "").strip() or (web or "").strip()
    if not raw:
        return None
    if not raw.startswith(("http://", "https://")):
        raw = "https://" + raw
    return urllib.parse.urlparse(raw).netloc or None


def load_env(path):
    env = {}
    if os.path.exists(path):
        for line in open(path, encoding="utf-8"):
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def check(row, stage):
    uid, name, country, domain, web = row
    rec = {"id": uid, "name": name, "country": country,
           "domain": domain or "", "status": "", "final_host": "", "detail": ""}

    host = host_of(domain, web)
    if not host:
        rec["status"] = "missing"
        return rec

    # ---- stage 1: DNS -------------------------------------------------
    # Many institutions publish an A record only for www, not the apex
    # (soton.ac.uk, tsinghua.edu.cn, ust.hk ...). Checking the bare domain
    # alone reports live universities as dead, so try both.
    socket.setdefaulttimeout(DNS_TIMEOUT)
    candidates = [host] if host.startswith("www.") else [host, "www." + host]
    resolved = None
    last_err = None
    for cand in candidates:
        try:
            socket.getaddrinfo(cand, None)
            resolved = cand
            break
        except Exception as e:
            last_err = type(e).__name__
    if not resolved:
        rec["status"] = "dns-fail"
        rec["detail"] = last_err or "unresolved"
        return rec
    if resolved != host:
        rec["detail"] = "apex has no record; www does"
    host = resolved
    if stage == "dns":
        rec["status"] = "ok"
        return rec

    # ---- stage 2: HTTP ------------------------------------------------
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    url = f"https://{host}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": UA})
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT, context=ctx) as r:
            r.read(1024)
            final = urllib.parse.urlparse(r.geturl()).netloc
            rec["final_host"] = final
            if registered_domain(final) != registered_domain(host):
                rec["status"] = "redirect-offsite"
                rec["detail"] = f"{registered_domain(host)} -> {registered_domain(final)}"
            else:
                rec["status"] = "ok"
    except urllib.error.HTTPError as e:
        # 401/403 still prove the host is a live server for this institution
        rec["status"] = "ok" if e.code in (401, 403) else "http-error"
        rec["detail"] = f"HTTP {e.code}"
    except Exception as e:
        rec["status"] = "no-response"
        rec["detail"] = type(e).__name__
    return rec


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0, help="0 = all universities")
    ap.add_argument("--workers", type=int, default=40)
    ap.add_argument("--stage", choices=["dns", "http"], default="dns")
    ap.add_argument("--out", default=None)
    args = ap.parse_args()
    out = os.path.abspath(args.out or f"../data/imports/domain_check_{args.stage}.csv")

    env = load_env(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))
    conn = psycopg2.connect(host=env.get("DB_HOST"), port=env.get("DB_PORT"),
                            dbname=env.get("DB_NAME"), user=env.get("DB_USER"),
                            password=env.get("DB_PASSWORD"))
    cur = conn.cursor()
    q = "SELECT id, name, country, domain, web FROM universities ORDER BY rank_num NULLS LAST, id"
    if args.limit:
        q += f" LIMIT {args.limit}"
    cur.execute(q)
    rows = cur.fetchall()
    cur.close(); conn.close()

    fields = ["id", "name", "country", "domain", "status", "final_host", "detail"]
    done_ids = set()
    os.makedirs(os.path.dirname(out), exist_ok=True)
    if os.path.exists(out):
        with open(out, newline="", encoding="utf-8") as fh:
            for r in csv.DictReader(fh):
                done_ids.add(int(r["id"]))
    todo = [r for r in rows if r[0] not in done_ids]
    if done_ids:
        print(f"Resuming: {len(done_ids)} done, {len(todo)} to go")
    if not todo:
        print("Nothing left to check.")
    else:
        print(f"Checking {len(todo)} universities [{args.stage}] with {args.workers} workers...")
        started, done = time.time(), 0
        new = not os.path.exists(out) or os.path.getsize(out) == 0
        with open(out, "a", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=fields)
            if new:
                w.writeheader(); fh.flush()
            with ThreadPoolExecutor(max_workers=args.workers) as ex:
                futs = [ex.submit(check, r, args.stage) for r in todo]
                for f in as_completed(futs):
                    w.writerow(f.result()); done += 1
                    if done % 500 == 0:
                        fh.flush()
                        with _lock:
                            print(f"  {done}/{len(todo)}  ({time.time()-started:.0f}s)", flush=True)

    with open(out, newline="", encoding="utf-8") as fh:
        allr = list(csv.DictReader(fh))
    from collections import Counter
    c = Counter(r["status"] for r in allr)
    print(f"\n{len(allr)} checked -> {out}\n")
    for k, v in c.most_common():
        print(f"  {k:18} {v:>6}  ({v*100//len(allr)}%)")


if __name__ == "__main__":
    main()
