#!/usr/bin/env python3
"""
UNIROUTE - fetchWikidataSites.py

Harvests official university websites from Wikidata and reconciles them against
our own domain column.

    python fetchWikidataSites.py --harvest          # pull from Wikidata -> cache
    python fetchWikidataSites.py --reconcile        # compare against our data
    python fetchWikidataSites.py --reconcile --apply

Why Wikidata: it publishes an official-website statement (P856) for most of the
world's universities, with country and alternate names, under a free licence.
That gives an independent source both for the universities we have no domain
for, and for checking the ones we do.

Reconciliation produces three buckets:
    fill      we have no domain, Wikidata does        -> safe to apply
    conflict  ours differs from Wikidata's            -> needs review, NOT applied
    agree     same registered domain                  -> confirms what we hold

Only 'fill' is written by --apply. A conflict means one of the two is wrong and
guessing which would risk replacing a correct domain with a worse one.
"""

import argparse
import csv
import json
import os
import re
import ssl
import sys
import time
import unicodedata
import urllib.parse
import urllib.request

import psycopg2

ENDPOINT = "https://query.wikidata.org/sparql"
UA = "UnirouteDomainFill/1.0 (educational university directory)"
CACHE = "../data/imports/wikidata_universities.json"

MULTI_TLD = {
    "ac.uk", "co.uk", "org.uk", "ac.jp", "co.jp", "ac.nz", "ac.za", "co.za",
    "edu.au", "com.au", "org.au", "edu.cn", "com.cn", "edu.in", "ac.in",
    "edu.sg", "edu.my", "edu.pk", "edu.br", "com.br", "edu.mx", "edu.tr",
    "edu.ar", "co.kr", "ac.kr", "ac.th", "edu.hk", "edu.tw", "ac.ir",
    "edu.eg", "edu.sa", "edu.co", "edu.pe", "edu.ph", "ac.id", "ac.be",
    "ac.at", "ac.il", "edu.pl", "edu.gr", "edu.ua", "edu.ru",
}

# Wikidata classes that are universities / higher-education institutions
CLASSES = ["wd:Q3918", "wd:Q38723", "wd:Q875538", "wd:Q189004"]


def strip_accents(s):
    s = unicodedata.normalize("NFKD", str(s or ""))
    return "".join(c for c in s if not unicodedata.combining(c))


def norm_name(s):
    s = strip_accents(s).lower()
    s = re.sub(r"\(.*?\)", " ", s)
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return " ".join(s.split())


def registered_domain(url_or_host):
    h = str(url_or_host or "").strip().lower()
    if h.startswith(("http://", "https://")):
        h = urllib.parse.urlparse(h).netloc
    h = h.split("/")[0].split(":")[0].rstrip(".")
    if h.startswith("www."):
        h = h[4:]
    parts = [p for p in h.split(".") if p]
    if len(parts) <= 2:
        return ".".join(parts)
    if ".".join(parts[-2:]) in MULTI_TLD and len(parts) >= 3:
        return ".".join(parts[-3:])
    return ".".join(parts[-2:])


def sparql(query, retries=3):
    url = ENDPOINT + "?" + urllib.parse.urlencode({"query": query, "format": "json"})
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    req = urllib.request.Request(url, headers={
        "User-Agent": UA, "Accept": "application/sparql-results+json"})
    last = None
    for a in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=90, context=ctx) as r:
                return json.load(r)["results"]["bindings"]
        except Exception as e:
            last = e
            time.sleep(3 * (a + 1))
    raise RuntimeError(f"SPARQL failed: {last}")


def harvest(out_path):
    """Page through Wikidata in chunks; one big query times out."""
    all_rows, seen = [], set()
    for cls in CLASSES:
        offset, page = 0, 5000
        while True:
            q = f"""
            SELECT ?uni ?uniLabel ?website ?countryLabel WHERE {{
              ?uni wdt:P31/wdt:P279* {cls} ;
                   wdt:P856 ?website .
              OPTIONAL {{ ?uni wdt:P17 ?country }}
              SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en,mul". }}
            }} LIMIT {page} OFFSET {offset}
            """
            rows = sparql(q)
            if not rows:
                break
            for b in rows:
                qid = b["uni"]["value"].rsplit("/", 1)[-1]
                key = (qid, b["website"]["value"])
                if key in seen:
                    continue
                seen.add(key)
                all_rows.append({
                    "qid": qid,
                    "name": b.get("uniLabel", {}).get("value", ""),
                    "website": b["website"]["value"],
                    "country": b.get("countryLabel", {}).get("value", ""),
                })
            print(f"  {cls} offset {offset}: +{len(rows)} (total {len(all_rows)})", flush=True)
            if len(rows) < page:
                break
            offset += page
            time.sleep(1)

    out = os.path.abspath(out_path)
    os.makedirs(os.path.dirname(out), exist_ok=True)
    json.dump(all_rows, open(out, "w", encoding="utf-8"), ensure_ascii=False)
    print(f"\nharvested {len(all_rows)} university websites -> {out}")


def load_env(path):
    env = {}
    if os.path.exists(path):
        for line in open(path, encoding="utf-8"):
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def reconcile(cache_path, apply_fills, out_path):
    wd = json.load(open(os.path.abspath(cache_path), encoding="utf-8"))
    print(f"Wikidata entries: {len(wd)}")

    idx = {}
    for r in wd:
        n = norm_name(r["name"])
        if n:
            idx.setdefault(n, []).append(r)

    here = os.path.dirname(os.path.abspath(__file__))
    env = load_env(os.path.join(here, ".env"))
    conn = psycopg2.connect(host=env.get("DB_HOST"), port=env.get("DB_PORT"),
                            dbname=env.get("DB_NAME"), user=env.get("DB_USER"),
                            password=env.get("DB_PASSWORD"))
    cur = conn.cursor()
    cur.execute("SELECT id, name, country, domain, web, rank FROM universities")
    ours = cur.fetchall()

    results = []
    fills, conflicts, agrees = [], 0, 0
    for uid, name, country, domain, web, rank in ours:
        cands = idx.get(norm_name(name), [])
        if not cands:
            continue
        # prefer a candidate whose country matches ours
        pick = next((c for c in cands
                     if norm_name(c["country"]) and norm_name(c["country"]) == norm_name(country)), None)
        if pick is None:
            if len(cands) > 1:
                continue          # ambiguous across countries: skip
            pick = cands[0]

        wd_dom = registered_domain(pick["website"])
        our_dom = registered_domain(domain or web) if (domain or web) else ""
        if not wd_dom:
            continue

        if not our_dom:
            kind = "fill"; fills.append((pick["website"], wd_dom, uid))
        elif our_dom == wd_dom:
            kind = "agree"; agrees += 1
        else:
            kind = "conflict"; conflicts += 1

        results.append({"id": uid, "name": name, "country": country, "rank": rank or "",
                        "our_domain": our_dom, "wikidata_domain": wd_dom,
                        "wikidata_url": pick["website"], "kind": kind, "qid": pick["qid"]})

    out = os.path.abspath(out_path)
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=["id", "name", "country", "rank", "our_domain",
                                           "wikidata_domain", "wikidata_url", "kind", "qid"])
        w.writeheader(); w.writerows(results)

    print(f"\nmatched {len(results)} of our universities to Wikidata")
    print(f"  agree    {agrees:>5}  our domain confirmed")
    print(f"  fill     {len(fills):>5}  we had none, Wikidata has one")
    print(f"  conflict {conflicts:>5}  differs - needs review, not auto-applied")
    print(f"\nreport -> {out}")

    if apply_fills and fills:
        cur.executemany(
            "UPDATE universities SET web = %s, domain = %s WHERE id = %s", fills)
        conn.commit()
        print(f"\napplied {len(fills)} fills")
    elif apply_fills:
        print("\nnothing to fill")
    cur.close(); conn.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--harvest", action="store_true")
    ap.add_argument("--reconcile", action="store_true")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--cache", default=CACHE)
    ap.add_argument("--out", default="../data/imports/wikidata_reconcile.csv")
    a = ap.parse_args()
    if a.harvest:
        harvest(a.cache)
    if a.reconcile:
        reconcile(a.cache, a.apply, a.out)
    if not (a.harvest or a.reconcile):
        ap.print_help()
