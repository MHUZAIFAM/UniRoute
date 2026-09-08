#!/usr/bin/env python3
"""
UNIROUTE - matchDomains.py

Backfills the website/domain of universities that have none.

Those rows (all id >= 10225) came from the QS rankings side when data.js was
built; the merge failed to match them to the world-universities domain dataset,
so they were appended as new rows. Their domains are therefore already present
in the database, attached to a duplicate row of the same institution:

    624   Massachusetts Institute of Technology         (no rank)  mit.edu
    10225 Massachusetts Institute of Technology (MIT)   rank 1     (no domain)

This finds those twins and copies the domain/web across.

    python matchDomains.py [--apply] [--min-score 0.90] [--out report.csv]

Runs as a dry run by default and writes a CSV of every proposed match with its
score, so matches can be reviewed before anything is changed. Rows are only
ever UPDATEd with a domain -- nothing is deleted or renumbered, so existing
university_programs links stay valid.
"""

import argparse
import csv
import os
import re
import sys
import unicodedata
from difflib import SequenceMatcher

import psycopg2

# Wording that differs between the two datasets for the same institution
NOISE = [
    r"\(.*?\)",                      # trailing "(MIT)", "(UCB)", ...
    r"\b(the|of|and|at|in|for|de|di|del|da|du|des|der|und)\b",
    r"\b(university|universite|universiteit|universidad|universidade|universita|universitat|universitaet|univ)\b",
    r"\b(college|institute|institution|school|academy|faculty)\b",
    r"\b(technology|technological|technical)\b",
    r"\b(national|state|federal|international)\b",
]


def strip_accents(s):
    s = unicodedata.normalize("NFKD", s)
    return "".join(c for c in s if not unicodedata.combining(c))


def norm_full(s):
    """Light normalisation: accents, case, punctuation, parentheticals."""
    s = strip_accents(str(s or "")).lower()
    s = re.sub(r"\(.*?\)", " ", s)
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return " ".join(s.split())


def norm_core(s):
    """Aggressive: also drop generic institution words, leaving the distinctive part."""
    s = strip_accents(str(s or "")).lower()
    for pat in NOISE:
        s = re.sub(pat, " ", s)
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return " ".join(s.split())


def load_env(path):
    env = {}
    if os.path.exists(path):
        for line in open(path, encoding="utf-8"):
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="write the matches (default: dry run)")
    ap.add_argument("--include-fuzzy", action="store_true",
                    help="also apply fuzzy matches. Off by default: at 0.90 roughly half "
                         "are wrong institutions (MGIMO->ITMO, Wroclaw->AGH, Tokai->Tokiwa), "
                         "and a wrong domain is worse than none.")
    ap.add_argument("--min-score", type=float, default=0.90)
    ap.add_argument("--out", default="../data/imports/domain_matches.csv")
    args = ap.parse_args()

    env = load_env(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))
    conn = psycopg2.connect(host=env.get("DB_HOST"), port=env.get("DB_PORT"),
                            dbname=env.get("DB_NAME"), user=env.get("DB_USER"),
                            password=env.get("DB_PASSWORD"))
    cur = conn.cursor()

    cur.execute("""SELECT id, name, country, rank FROM universities
                   WHERE COALESCE(NULLIF(domain,''),NULLIF(web,'')) IS NULL""")
    missing = cur.fetchall()

    cur.execute("""SELECT id, name, country, domain, web FROM universities
                   WHERE COALESCE(NULLIF(domain,''),NULLIF(web,'')) IS NOT NULL""")
    haves = cur.fetchall()
    print(f"{len(missing)} universities without a website; {len(haves)} with one\n")

    # Index candidates by country - the same institution name can exist in
    # several countries, so never match across borders.
    by_country = {}
    for hid, hname, hcountry, hdomain, hweb in haves:
        by_country.setdefault(hcountry, []).append(
            (hid, hname, hdomain, hweb, norm_full(hname), norm_core(hname))
        )

    results, matched = [], 0
    for mid, mname, mcountry, mrank in missing:
        mf, mc = norm_full(mname), norm_core(mname)
        best = None
        for hid, hname, hdomain, hweb, hf, hc in by_country.get(mcountry, []):
            if mf == hf:
                score, how = 1.00, "exact"
            elif mc and mc == hc:
                score, how = 0.97, "core-exact"
            else:
                score = SequenceMatcher(None, mf, hf).ratio()
                how = "fuzzy"
                if score < args.min_score and mc and hc:
                    cs = SequenceMatcher(None, mc, hc).ratio()
                    if cs > score:
                        score, how = cs * 0.98, "fuzzy-core"
            if best is None or score > best[0]:
                best = (score, hid, hname, hdomain, hweb, how)

        if best and best[0] >= args.min_score:
            matched += 1
            results.append({
                "id": mid, "name": mname, "country": mcountry, "rank": mrank,
                "match_id": best[1], "match_name": best[2],
                "domain": best[3] or "", "web": best[4] or "",
                "score": round(best[0], 3), "how": best[5],
            })
        else:
            results.append({
                "id": mid, "name": mname, "country": mcountry, "rank": mrank,
                "match_id": "", "match_name": best[2] if best else "",
                "domain": "", "web": "",
                "score": round(best[0], 3) if best else 0, "how": "NO MATCH",
            })

    out = os.path.abspath(args.out)
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=["id", "name", "country", "rank", "match_id",
                                           "match_name", "domain", "web", "score", "how"])
        w.writeheader(); w.writerows(results)

    print(f"matched  : {matched}/{len(missing)}  ({matched*100//max(len(missing),1)}%)")
    print(f"unmatched: {len(missing)-matched}")
    from collections import Counter
    for how, n in Counter(r["how"] for r in results).most_common():
        print(f"   {how:12} {n}")
    print(f"\nreport -> {out}")

    print("\n-- sample matches --")
    for r in [r for r in results if r["match_id"]][:8]:
        print(f"  {r['score']:.2f} {r['how']:11} {r['name'][:38]:40} -> {r['match_name'][:30]:32} {r['domain']}")

    SAFE = {"exact", "core-exact"}
    safe_rows  = [r for r in results if r["match_id"] and r["how"] in SAFE]
    fuzzy_rows = [r for r in results if r["match_id"] and r["how"] not in SAFE]

    # Park fuzzy candidates in their own file for a human to confirm
    if fuzzy_rows:
        rev = os.path.join(os.path.dirname(out), "domain_matches_needs_review.csv")
        with open(rev, "w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=list(fuzzy_rows[0].keys()))
            w.writeheader(); w.writerows(fuzzy_rows)
        print(f"\n{len(fuzzy_rows)} fuzzy matches need review -> {rev}")
        print("   (not applied by default - about half point at the wrong institution)")

    if not args.apply:
        print("\nDry run - nothing written. Re-run with --apply to save.")
        return

    chosen = safe_rows + (fuzzy_rows if args.include_fuzzy else [])
    print(f"\nApplying {len(chosen)} matches "
          f"({len(safe_rows)} exact/core-exact"
          f"{f', {len(fuzzy_rows)} fuzzy' if args.include_fuzzy else ''}).")
    updates = [(r["domain"], r["web"], r["id"]) for r in chosen]
    cur.executemany(
        "UPDATE universities SET domain = NULLIF(%s,''), web = NULLIF(%s,'') WHERE id = %s",
        updates)
    conn.commit()
    cur.execute("""SELECT COUNT(*) FROM universities
                   WHERE COALESCE(NULLIF(domain,''),NULLIF(web,'')) IS NULL""")
    print(f"\nApplied {len(updates)} updates. Still without a website: {cur.fetchone()[0]}")
    cur.close(); conn.close()


if __name__ == "__main__":
    main()
