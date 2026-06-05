#!/usr/bin/env python3
"""
generate_report.py — Fetch Close CRM data for a date range and write report_data.json.
Usage: python3 generate_report.py --start YYYY-MM-DD --end YYYY-MM-DD [--goals goals.json]
"""

import os, re, sys, time, json, argparse, calendar
from datetime import datetime, date, timedelta, timezone
from zoneinfo import ZoneInfo
import requests

PACIFIC = ZoneInfo("America/Los_Angeles")

# ── Close field IDs ────────────────────────────────────────────────────────────
CF_FIRST_SALES = "cf_LFdYEQ6bsgp49YjZzefypDmdVx8iwuakWDSLPLpVrBq"
CF_FUNNEL_NAME = "cf_xqDQE8fkPsWa0RNEve7hcaxKblCe6489XeZGRDzyPdX"
CF_SHOW_UP     = "cf_OPyvpU45RdvjLqfm8V1VWwNxrGKogEH2IBJmfCj0Uhq"
CF_QUALIFIED   = "cf_ZDx7NBQaDzV1yYrFcBMzt6cIYj81dAcswpNN0CQzCPS"
PIPE_SALES     = "pipe_78hyBUVS7IKikGEmstObu1"
STAT_WON       = "stat_WnFc0uhjcjV0cc3bVzdFVqDz7av6rbsOmOvHUsO6s03"

EXCLUDED_LEAD_STATUS_IDS = {
    "stat_hWIGHjzyNpl4YjIFSFz3VK4fp2ny10SFJLKAihmo4KT",  # Canceled by Lead
    "stat_YV4ZngDB4IGjLjlOf0YTFEWuKZJ6fhNxVkzQkvKYfdB",  # Outside the US
}
EXCLUDED_WON_USER_IDS = {
    "user_3mOVGlSt7OC8FTOk4lsF6EGqPiTBRPrFqEdaqcfj8Pw",  # Ahmad Bukhari
    "user_5KQyMhFRJxMf4OilHxLr4I2HbdXFvTBaqHoXkzW7PqW",  # Stephen Olivas
    "user_w7DG4aSzvFCOPrbJXODJIimGmq4Tqn0nSVDXn2FtZuQ",  # Spencer Reynolds
    "user_MKOQR5gHgClObwmBNdLwVJUE2FgJM9DAtXGHxJ1KFjN",  # Mallory Kent
}
EXCLUDED_FROM_TOTALS_FUNNELS = {"LTF - Quiz Funnel"}

FUNNEL_GROUPS = [
    ("EXTERNAL", ["Low Ticket Funnel", "Instagram", "X", "Linkedin", "LTF - Quiz Funnel"]),
    ("IN-HOUSE",  ["YouTube", "Meta Ads", "VSL", "Website", "Internal Webinar",
                   "Mike Newsletter", "Side Hustle Nation", "WWWS", "Tik Tok",
                   "Anthony IG", "Passivepreneurs", "Reactivation Email",
                   "Reactivation Scrapers", "Referred", "LinkedIn Ads",
                   "Google Ads", "YouTube Ads"]),
]
FUNNEL_ORDER = [f for _, funnels in FUNNEL_GROUPS for f in funnels]

# ── API ────────────────────────────────────────────────────────────────────────
session = requests.Session()
session.auth = (os.environ["CLOSE_API_KEY"], "")
session.headers.update({"Content-Type": "application/json"})

def close_get(endpoint, params=None):
    time.sleep(0.5)
    url = f"https://api.close.com/api/v1/{endpoint}"
    for attempt in range(5):
        resp = session.get(url, params=params or {}, timeout=60)
        if resp.status_code == 429:
            wait = float(resp.headers.get("Retry-After", 5))
            print(f"  Rate limited — waiting {wait}s...", flush=True)
            time.sleep(wait); continue
        resp.raise_for_status()
        return resp.json()
    resp.raise_for_status()

def _is_yes(val):
    if val is None or val is False: return False
    if val is True: return True
    return str(val).strip().lower() in ("yes", "true", "1")

# ── Helpers ────────────────────────────────────────────────────────────────────
def get_funnel_name(lead):
    raw = lead.get(f"custom.{CF_FUNNEL_NAME}") or lead.get(f"custom_{CF_FUNNEL_NAME}")
    if not raw:
        return "No Attribution"
    if isinstance(raw, list):
        raw = raw[0] if raw else ""
    return str(raw).strip() or "No Attribution"

def pct(num, den):
    if not den: return "—"
    return f"{num / den * 100:.1f}%"

def fmt_currency(val):
    if not val: return "$0"
    return f"${val:,.0f}"

def fmt_ordinal(d):
    """e.g. date(2026,5,14) → 'May 14th, 2026'"""
    suffix = {1:"st",2:"nd",3:"rd"}.get(d.day % 10 if d.day not in (11,12,13) else 0, "th")
    return d.strftime(f"%B {d.day}{suffix}, %Y")

# ── Fetchers ──────────────────────────────────────────────────────────────────
def fetch_booked_leads(start_date, end_date):
    start_str = start_date.strftime("%Y-%m-%d")
    end_str   = end_date.strftime("%Y-%m-%d")
    query     = (f'custom.{CF_FIRST_SALES} >= "{start_str}" '
                 f'AND custom.{CF_FIRST_SALES} <= "{end_str}"')
    print(f"Fetching booked leads ({start_str} → {end_str})...", flush=True)
    leads, skip = [], 0
    while True:
        data = close_get("lead/", {
            "query":   query,
            "_fields": f"id,status_id,custom.{CF_FUNNEL_NAME},custom.{CF_SHOW_UP},custom.{CF_QUALIFIED}",
            "_limit":  200, "_skip": skip,
        })
        batch = data.get("data", [])
        leads.extend(batch)
        if not data.get("has_more"): break
        skip += 200
    print(f"  Total booked leads: {len(leads)}", flush=True)
    return leads

def fetch_won_opps(start_date, end_date):
    start_str = start_date.strftime("%Y-%m-%d")
    end_str   = end_date.strftime("%Y-%m-%d")
    print(f"Fetching won opps ({start_str} → {end_str})...", flush=True)
    opps, skip = [], 0
    while True:
        data = close_get("opportunity/", {
            "pipeline_id": PIPE_SALES, "status_id": STAT_WON,
            "date_won__gte": start_str, "date_won__lte": end_str,
            "_fields": "id,lead_id,value,date_won,user_id",
            "_limit": 100, "_skip": skip,
        })
        batch = data.get("data", [])
        opps.extend(batch)
        if not data.get("has_more"): break
        skip += 100
    print(f"  Won opps: {len(opps)}", flush=True)
    return opps

# ── Aggregation ────────────────────────────────────────────────────────────────
def aggregate(start_date, end_date, goals):
    booked_leads = fetch_booked_leads(start_date, end_date)
    won_opps     = fetch_won_opps(start_date, end_date)

    # Per-funnel meeting rows
    funnel_data = {}  # funnel → {booked, showed, qualified}
    for lead in booked_leads:
        if lead.get("status_id") in EXCLUDED_LEAD_STATUS_IDS: continue
        funnel = get_funnel_name(lead)
        if funnel not in funnel_data:
            funnel_data[funnel] = {"booked": 0, "showed": 0, "qualified": 0,
                                   "closed": 0, "revenue": 0.0}
        funnel_data[funnel]["booked"]    += 1
        if _is_yes(lead.get(f"custom.{CF_SHOW_UP}")):    funnel_data[funnel]["showed"]    += 1
        if _is_yes(lead.get(f"custom.{CF_QUALIFIED}")): funnel_data[funnel]["qualified"] += 1

    # Per-funnel won opps — need lead funnel for each opp
    seen_deals = set()
    for opp in won_opps:
        if opp.get("user_id") in EXCLUDED_WON_USER_IDS: continue
        lid = opp.get("lead_id")
        key = f"opp:{opp['id']}"
        if key in seen_deals: continue
        seen_deals.add(key)
        # Fetch lead funnel if not already cached
        try:
            lead_resp = close_get(f"lead/{lid}/", {"_fields": f"id,custom.{CF_FUNNEL_NAME}"})
            funnel = get_funnel_name(lead_resp)
        except Exception:
            funnel = "No Attribution"
        if funnel not in funnel_data:
            funnel_data[funnel] = {"booked": 0, "showed": 0, "qualified": 0,
                                   "closed": 0, "revenue": 0.0}
        funnel_data[funnel]["closed"]  += 1
        funnel_data[funnel]["revenue"] += (opp.get("value") or 0) / 100

    # Build ordered funnel rows
    all_funnels = list(FUNNEL_ORDER) + [f for f in funnel_data if f not in FUNNEL_ORDER]

    # Goals and on-pace calculation (based on days elapsed in the period)
    period_days = (end_date - start_date).days + 1
    now = datetime.now(PACIFIC).date()
    days_elapsed = min((now - start_date).days + 1, period_days)

    rows = []
    grand = {"booked": 0, "showed": 0, "qualified": 0, "closed": 0, "revenue": 0.0}
    group_totals = {g: {"booked": 0, "showed": 0, "qualified": 0, "closed": 0, "revenue": 0.0}
                    for g, _ in FUNNEL_GROUPS}
    group_totals["UNCATEGORIZED"] = {"booked": 0, "showed": 0, "qualified": 0, "closed": 0, "revenue": 0.0}
    group_map = {f: g for g, funnels in FUNNEL_GROUPS for f in funnels}

    for funnel in all_funnels:
        t = funnel_data.get(funnel)
        if not t: continue
        if t["booked"] == 0 and t["closed"] == 0: continue

        goal = goals.get(funnel)
        on_pace = round((t["booked"] / days_elapsed) * period_days) if days_elapsed and t["booked"] else None
        goal_pct = f"{round(t['booked'] / goal * 100)}% ({goal})" if goal else "—"

        row = {
            "funnel":      funnel,
            "group":       group_map.get(funnel, "UNCATEGORIZED"),
            "excluded":    funnel in EXCLUDED_FROM_TOTALS_FUNNELS,
            "booked":      t["booked"],
            "on_pace":     on_pace,
            "goal_pct":    goal_pct,
            "showed":      t["showed"],
            "show_pct":    pct(t["showed"], t["booked"]),
            "qualified":   t["qualified"],
            "qual_pct":    pct(t["qualified"], t["booked"]),
            "closed":      t["closed"] if t["closed"] else None,
            "cw_pct":      pct(t["closed"], t["booked"]),
            "revenue":     t["revenue"],
            "rev_per_close": fmt_currency(t["revenue"] / t["closed"]) if t["closed"] else "—",
        }
        rows.append(row)

        if not row["excluded"]:
            for k in grand: grand[k] += t.get(k, 0)
            grp = group_map.get(funnel, "UNCATEGORIZED")
            for k in group_totals[grp]: group_totals[grp][k] += t.get(k, 0)

    return {
        "start_date":    start_date.strftime("%Y-%m-%d"),
        "end_date":      end_date.strftime("%Y-%m-%d"),
        "week_ending":   fmt_ordinal(end_date),
        "date_range_label": f"{start_date.strftime('%B %-d')} – {end_date.strftime('%B %-d, %Y')}",
        "grand":         grand,
        "group_totals":  group_totals,
        "funnel_rows":   rows,
    }

# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True, help="Start date YYYY-MM-DD (Friday)")
    parser.add_argument("--end",   required=True, help="End date YYYY-MM-DD (Thursday)")
    parser.add_argument("--goals", default="goals.json")
    args = parser.parse_args()

    start_date = date.fromisoformat(args.start)
    end_date   = date.fromisoformat(args.end)

    goals = {}
    try:
        with open(args.goals) as f:
            goals = json.load(f)
    except Exception:
        pass

    print(f"\n=== Generating report: {args.start} → {args.end} ===\n", flush=True)
    data = aggregate(start_date, end_date, goals)

    with open("report_data.json", "w") as f:
        json.dump(data, f, indent=2)
    print(f"\nWritten: report_data.json", flush=True)

if __name__ == "__main__":
    main()
