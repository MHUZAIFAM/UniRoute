#!/usr/bin/env python3
"""
UNIROUTE - surveyCatalogues.py

Surveys university websites to find out which ones can actually be read by an
automated program-catalogue extractor, and records exactly what blocks the rest.

    python surveyCatalogues.py [--limit 1000] [--workers 10] [--out report.csv]

For each of the top-N ranked universities it:
  1. reads robots.txt and honours it (a Disallow is recorded, never bypassed)
  2. checks the homepage is reachable
  3. probes a short list of common catalogue paths for a 200

Outputs a CSV with one row per university and a status of:
  no-website          - nothing to fetch; our own data is missing the domain
  robots-disallowed   - site asks automated clients not to fetch this path
  blocked-403         - server refuses automated access
  unreachable         - DNS failure, connection refused, TLS error
  timeout             - no response within the timeout
  no-catalogue-found  - site is readable, but no obvious catalogue path
  ok                  - readable catalogue page found (url recorded)

Nothing is scraped here beyond HTTP status checks; this only establishes what
is fetchable. Extraction is a separate, slower step.
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
import urllib.robotparser
from concurrent.futures import ThreadPoolExecutor, as_completed

import psycopg2

UA = "UnirouteCatalogueSurvey/1.0 (+educational directory; contact via repo)"
TIMEOUT = 12

# Ordered by how commonly they hold a full program listing
CANDIDATE_PATHS = [
    "/academics/programs", "/programs", "/academics", "/courses", "/study",
]
# Whole-university budget. Without this a dead host with slow DNS can hold a
# worker for minutes while every candidate path times out in turn.
PER_UNI_BUDGET = 30

_print_lock = threading.Lock()


def load_env(path):
    env = {}
    if not os.path.exists(path):
        return env
    for line in open(path, encoding="utf-8"):
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def base_url(domain, web):
    raw = (web or "").strip() or (domain or "").strip()
    if not raw:
        return None
    if not raw.startswith(("http://", "https://")):
        raw = "https://" + raw
    p = urllib.parse.urlparse(raw)
    if not p.netloc:
        return None
    return f"{p.scheme}://{p.netloc}"


def fetch_status(url):
    """Return (status_code, final_url) or ('ERR:<kind>', None)."""
    req = urllib.request.Request(url, headers={"User-Agent": UA}, method="GET")
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE  # many .edu hosts have chain issues
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT, context=ctx) as r:
            r.read(2048)  # touch the body so we know it really responds
            return r.status, r.geturl()
    except urllib.error.HTTPError as e:
        return e.code, url
    except socket.timeout:
        return "ERR:timeout", None
    except urllib.error.URLError as e:
        kind = "timeout" if isinstance(e.reason, socket.timeout) else "unreachable"
        return f"ERR:{kind}", None
    except Exception:
        return "ERR:unreachable", None


def load_robots(base):
    """Fetch robots.txt once per host and return a checker callable."""
    rp = urllib.robotparser.RobotFileParser()
    rp.set_url(base + "/robots.txt")
    try:
        rp.read()
    except Exception:
        return lambda path: True  # unreadable robots.txt: default to allowed

    def allows(path):
        try:
            return rp.can_fetch(UA, base + path)
        except Exception:
            return True
    return allows


def survey_one(row):
    uid, name, country, rank, domain, web = row
    base = base_url(domain, web)
    rec = {"id": uid, "name": name, "country": country, "rank": rank,
           "base_url": base or "", "status": "", "catalogue_url": "", "detail": ""}

    if not base:
        rec["status"] = "no-website"
        rec["detail"] = "no domain or web field in our dataset"
        return rec

    code, _ = fetch_status(base)
    if code == 403:
        rec["status"] = "blocked-403"; rec["detail"] = "homepage refuses automated access"
        return rec
    if isinstance(code, str) and code.startswith("ERR:"):
        rec["status"] = code.split(":")[1]; rec["detail"] = "homepage " + code
        return rec

    allows = load_robots(base)          # one robots.txt read per host
    deadline = time.time() + PER_UNI_BUDGET
    disallowed = []

    for path in CANDIDATE_PATHS:
        if time.time() > deadline:
            rec["status"] = "timeout"
            rec["detail"] = "exceeded per-university time budget while probing paths"
            return rec
        if not allows(path):
            disallowed.append(path)
            continue                    # respect it, try the next path
        c, final = fetch_status(base + path)
        if c == 200:
            rec["status"] = "ok"; rec["catalogue_url"] = final or (base + path)
            return rec
        if c == 403:
            rec["status"] = "blocked-403"; rec["detail"] = f"403 on {path}"
            return rec

    if disallowed and len(disallowed) == len(CANDIDATE_PATHS):
        rec["status"] = "robots-disallowed"
        rec["detail"] = "robots.txt disallows every candidate path"
        return rec

    rec["status"] = "no-catalogue-found"
    rec["detail"] = "homepage reachable; none of the common catalogue paths returned 200"
    return rec


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=1000)
    ap.add_argument("--workers", type=int, default=10)
    ap.add_argument("--out", default="../data/imports/catalogue_survey.csv")
    args = ap.parse_args()

    env = load_env(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))
    conn = psycopg2.connect(host=env.get("DB_HOST"), port=env.get("DB_PORT"),
                            dbname=env.get("DB_NAME"), user=env.get("DB_USER"),
                            password=env.get("DB_PASSWORD"))
    cur = conn.cursor()
    cur.execute("""
        SELECT id, name, country, rank, domain, web
        FROM universities
        WHERE rank_num IS NOT NULL
        ORDER BY rank_num
        LIMIT %s
    """, (args.limit,))
    rows = cur.fetchall()
    cur.close(); conn.close()

    out = os.path.abspath(args.out)
    os.makedirs(os.path.dirname(out), exist_ok=True)
    fields = ["id", "name", "country", "rank", "base_url", "status", "catalogue_url", "detail"]

    # Resume: skip anything already recorded, so an interrupted run picks up
    # where it stopped instead of starting over.
    done_ids = set()
    if os.path.exists(out):
        with open(out, newline="", encoding="utf-8") as fh:
            for r in csv.DictReader(fh):
                done_ids.add(int(r["id"]))
    todo = [r for r in rows if r[0] not in done_ids]
    if done_ids:
        print(f"Resuming: {len(done_ids)} already surveyed, {len(todo)} to go")
    if not todo:
        print("Nothing left to survey.")
        return

    print(f"Surveying {len(todo)} universities with {args.workers} workers...")
    started = time.time()
    done = 0
    # Append as results arrive so an interruption never loses completed work
    new_file = not os.path.exists(out) or os.path.getsize(out) == 0
    with open(out, "a", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fields)
        if new_file:
            w.writeheader(); fh.flush()
        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            futs = {ex.submit(survey_one, r): r for r in todo}
            for f in as_completed(futs):
                w.writerow(f.result()); fh.flush()
                done += 1
                if done % 25 == 0:
                    with _print_lock:
                        print(f"  {done}/{len(todo)}  ({time.time()-started:.0f}s)", flush=True)

    counts = {}
    with open(out, newline="", encoding="utf-8") as fh:
        rowsr = list(csv.DictReader(fh))
    for r in rowsr:
        counts[r["status"]] = counts.get(r["status"], 0) + 1
    print(f"\nDone in {time.time()-started:.0f}s. {len(rowsr)} total surveyed -> {out}\n")
    for k, v in sorted(counts.items(), key=lambda kv: -kv[1]):
        print(f"  {k:20} {v:>5}  ({v*100//len(rowsr)}%)")


if __name__ == "__main__":
    main()
