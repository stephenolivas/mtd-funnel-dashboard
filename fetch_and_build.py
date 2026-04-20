#!/usr/bin/env python3
"""
MTD Funnel Performance Dashboard
Pulls meeting bookings, show-up, qualified, closed-won, and UTM campaign data
from Close CRM and builds a static HTML dashboard.
"""

import os
import re
import sys
import time
import json
import argparse
import calendar
from datetime import datetime, date, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

# ── Config ─────────────────────────────────────────────────────────────────────

PACIFIC = ZoneInfo("America/Los_Angeles")
CLOSE_API_KEY = os.environ["CLOSE_API_KEY"]

session = requests.Session()
session.auth = (CLOSE_API_KEY, "")
session.headers.update({"Content-Type": "application/json"})

# ── Custom Field IDs ───────────────────────────────────────────────────────────

CF_FUNNEL_NAME  = "cf_xqDQE8fkPsWa0RNEve7hcaxKblCe6489XeZGRDzyPdX"  # Funnel Name DEAL (lead)
CF_SHOW_UP      = "cf_OPyvpU45RdvjLqfm8V1VWwNxrGKogEH2IBJmfCj0Uhq"  # First Call Show Up (opp)
CF_QUALIFIED    = "cf_ZDx7NBQaDzV1yYrFcBMzt6cIYj81dAcswpNN0CQzCPS"  # Qualified (opp)
CF_UTM_CAMPAIGN = "cf_jnbd0xzUY3tuxzxiGxBs2hONuExeXMvAoTUM2R64Lq3"  # utm_campaign (contact)
CF_UTM_CONTENT    = "cf_R7o66i0XPycLQHlxOLbIqk6c6j3oB8CzxF3e3apI1hn"   # utm_content (contact)
CF_FIRST_SALES    = "cf_LFdYEQ6bsgp49YjZzefypDmdVx8iwuakWDSLPLpVrBq"            # First Sales Call Booked Date (lead)

# Funnels that use utm_content instead of utm_campaign for sub-breakdown
UTM_CONTENT_FUNNELS = {"Internal Webinar"}

CLOSED_WON_STATUS_ID    = "stat_0oW3iRpVp9z5DJq0cuwI1HgR0XhHAhykEPPIq4TFsxd"
WEEKLY_FEATURE_START    = "2026-04"  # Weeks only available for this month and later

# ── Filter Constants (identical to Capacity Dashboard) ────────────────────────

EXCLUDED_LEAD_STATUS_IDS = {
    "stat_hWIGHjzyNpl4YjIFSFz3VK4fp2ny10SFJLKAihmo4KT",  # Canceled (by Lead)
    "stat_YV4ZngDB4IGjLjlOf0YTFEWuKZJ6fhNxVkzQkvKYfdB",  # Outside the US
}

EXCLUDED_USER_IDS = {
    "user_5cZRqXu8kb4O1IeBVA98UMcMEhYZUhx1fnCHfSL0YMV",  # Stephen Olivas
    "user_yRF070m26JE67J6CJqzkAB3IqY7btNm1K5RisCglKa6",  # Ahmad Bukhari
    "user_EmhqCmaHERTfgfWnPADiLGEqQw3ENvRYd3u1VEmblIp",  # Kristin Nelson
    "user_4sfuKGMbv0LQZ4hpS8ipASv406kKTSNP5Xx79jOwSqM",  # Spencer Reynolds
}

# Excluded from closed-won revenue — matches rep dashboard user exclusions
EXCLUDED_CLOSER_USER_IDS = {
    "user_yRF070m26JE67J6CJqzkAB3IqY7btNm1K5RisCglKa6",  # Ahmad Bukhari
    "user_5cZRqXu8kb4O1IeBVA98UMcMEhYZUhx1fnCHfSL0YMV",  # Stephen Olivas
    "user_4sfuKGMbv0LQZ4hpS8ipASv406kKTSNP5Xx79jOwSqM",  # Spencer Reynolds
    "user_SGISGe3kE7zhSm7LQgZ0Vrt7DKz5RVZ0JzFkI4S8llS",  # Mallory Kent
}

# ── Title Classification Regexes (first-match-wins, same order as Capacity Dashboard) ──

INCLUDE_SCRAPER_RE = re.compile(
    r"vendingpren[eu]+rs?\s*-?\s*next\s+steps"
    r"|vendingpreneur\s+next\s+steps",
    re.IGNORECASE,
)
EXCLUDE_DISCOVERY_RE = re.compile(r"vending\s+quick\s+discovery", re.IGNORECASE)
EXCLUDE_FOLLOWUP_RE  = re.compile(
    r"follow[-\s]?up|fallow\s+up|\bf/u\b|next\s+steps|rescheduled?",
    re.IGNORECASE,
)
EXCLUDE_ANTHONY_QA_RE = re.compile(r"anthony", re.IGNORECASE)
EXCLUDE_QA_RE         = re.compile(r"q\s*[&/]\s*a", re.IGNORECASE)
EXCLUDE_ENROLLMENT_RE = re.compile(
    r"enrollment|silver\s+start\s*up|bronze\s+enrollment|questions\s+on\s+enrollment",
    re.IGNORECASE,
)
INCLUDE_STANDARD_RE = re.compile(
    r"vending\s+strategy\s+call"
    r"|vendingpren[eu]+rs?\s+consultation"
    r"|vendingpren[eu]+rs?\s+strategy\s+call"
    r"|new\s+vendingpreneur\s+strategy\s+call"
    r"|post\s+masterclass\s+strategy\s+call"
    r"|vending\s+consult",
    re.IGNORECASE,
)

# ── Known Funnel Display Order (grouped) ──────────────────────────────────────

FUNNEL_GROUPS = [
    ("EXTERNAL", [
        "Low Ticket Funnel",
        "Instagram",
        "X",
        "Linkedin",
    ]),
    ("IN-HOUSE", [
        "YouTube",
        "Meta Ads",
        "VSL",
        "Website",
        "Internal Webinar",
        "Mike Newsletter",
        "Side Hustle Nation",
        "WWWS",
        "Tik Tok",
        "Anthony IG",
        "Passivepreneurs",
        "Reactivation Email",
        "Reactivation Scrapers",
        "Referred",
    ]),
    ("UNCATEGORIZED", [
        "Unknown (Needs Review)",
        "No Attribution",
    ]),
]

# Flat ordered list for membership checks
FUNNEL_ORDER = [f for _, funnels in FUNNEL_GROUPS for f in funnels]

# ── API Helpers ────────────────────────────────────────────────────────────────

def close_get(endpoint, params=None):
    """GET from Close API with 0.5s throttle and 429 retry logic."""
    time.sleep(0.5)
    url = f"https://api.close.com/api/v1/{endpoint}"
    for attempt in range(5):
        resp = session.get(url, params=params or {}, timeout=60)
        if resp.status_code == 429:
            wait = float(resp.headers.get("Retry-After", 5))
            print(f"  Rate limited — waiting {wait}s...", flush=True)
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return resp.json()
    resp.raise_for_status()


# ── Meeting Classification ─────────────────────────────────────────────────────

def classify_title(title):
    """Returns True if title represents a qualifying first sales call."""
    t = (title or "").strip()

    # 1. Starts with "canceled"
    if re.match(r"canceled", t, re.IGNORECASE):
        return False
    # 2. Discovery
    if EXCLUDE_DISCOVERY_RE.search(t):
        return False
    # 3. Scraper Next Steps — MUST come before generic follow-up check
    if INCLUDE_SCRAPER_RE.search(t):
        return True
    # 4. Follow-up / reschedule / generic Next Steps
    if EXCLUDE_FOLLOWUP_RE.search(t):
        return False
    # 5. Anthony Q&A
    if EXCLUDE_ANTHONY_QA_RE.search(t) and EXCLUDE_QA_RE.search(t):
        return False
    # 6. Enrollment
    if EXCLUDE_ENROLLMENT_RE.search(t):
        return False
    # 7. Standard first-call patterns
    if INCLUDE_STANDARD_RE.search(t):
        return True
    # Default: exclude
    return False


def is_valid_meeting(meeting):
    """Apply all meeting-level filters."""
    status = (meeting.get("status") or "").lower()
    if status.startswith("canceled") or status.startswith("declined"):
        return False
    if meeting.get("user_id") in EXCLUDED_USER_IDS:
        return False
    if not classify_title(meeting.get("title", "")):
        return False
    return True


# ── Step 1: Fetch & Filter Meetings ───────────────────────────────────────────

def fetch_all_meetings():
    """Paginate ALL meetings (~120 API calls for ~12k meetings)."""
    print("Fetching all meetings from Close API...", flush=True)
    meetings, skip = [], 0
    while True:
        data = close_get("activity/meeting/", {"_skip": skip, "_limit": 100})
        batch = data.get("data", [])
        meetings.extend(batch)
        print(f"  Fetched {len(meetings)} meetings so far...", flush=True)
        if not data.get("has_more"):
            break
        skip += 100
    print(f"Total raw meetings: {len(meetings)}", flush=True)
    return meetings


def filter_meetings_by_range(meetings, start_date, end_date):
    """
    Keep qualifying meetings within start_date..end_date (Pacific, inclusive).
    start_date, end_date: date objects.
    """
    start_dt = datetime(start_date.year, start_date.month, start_date.day, tzinfo=PACIFIC)
    valid = []
    for m in meetings:
        starts_at = m.get("starts_at")
        if not starts_at:
            continue
        try:
            dt_utc = datetime.fromisoformat(starts_at.replace("Z", "+00:00"))
            dt_pac = dt_utc.astimezone(PACIFIC)
        except Exception:
            continue
        if dt_pac < start_dt or dt_pac.date() > end_date:
            continue
        if not is_valid_meeting(m):
            continue
        m["_date_pac"] = dt_pac.date()
        valid.append(m)
    return valid


# ── Week helpers ───────────────────────────────────────────────────────────────

def current_week_monday():
    """Return the Monday of the current week (Pacific)."""
    today = datetime.now(PACIFIC).date()
    return today - timedelta(days=today.weekday())


def week_bounds(monday):
    """Return (start, end) dates for the week starting on monday."""
    sunday = monday + timedelta(days=6)
    today  = datetime.now(PACIFIC).date()
    return monday, min(sunday, today)


def week_display_label(monday, end_date=None):
    """e.g. 'Apr 6–12' or 'Apr 27–May 3'"""
    if end_date is None:
        end_date = monday + timedelta(days=6)
    if monday.month == end_date.month:
        return f"{monday.strftime('%b %-d')}–{end_date.day}"
    return f"{monday.strftime('%b %-d')}–{end_date.strftime('%b %-d')}"


def deduplicate_meetings(meetings):
    """One count per lead — most recent qualifying meeting in the window wins."""
    lead_best = {}
    for m in meetings:
        lid = m.get("lead_id")
        if not lid:
            continue
        if lid not in lead_best or m["_date_pac"] > lead_best[lid]["_date_pac"]:
            lead_best[lid] = m
    return list(lead_best.values())


# ── Step 2: Lead Data ──────────────────────────────────────────────────────────

def fetch_lead(lead_id):
    """Fetch minimal lead fields needed for dashboard."""
    return close_get(f"lead/{lead_id}", {
        "_fields": f"id,display_name,status_id,"
                   f"custom.{CF_FUNNEL_NAME},"
                   f"custom.{CF_SHOW_UP},"
                   f"custom.{CF_QUALIFIED}"
    })


def get_funnel_name(lead):
    raw = lead.get(f"custom.{CF_FUNNEL_NAME}")
    val = (raw or "").strip()
    return val if val else "Unknown (Needs Review)"


# ── Step 3: Opportunity Data ───────────────────────────────────────────────────

def fetch_latest_opportunity(lead_id):
    """Return the most recently created opportunity on a lead, or None."""
    data = close_get("opportunity/", {
        "lead_id": lead_id,
        "_fields": f"id,lead_id,created_at,value,status_type,date_won,"
                   f"custom.{CF_SHOW_UP},custom.{CF_QUALIFIED}",
        "_limit": 20,
    })
    opps = data.get("data", [])
    if not opps:
        return None
    # Sort by created_at descending — most recent first
    opps.sort(key=lambda o: o.get("created_at") or "", reverse=True)
    return opps[0]


def fetch_won_opps_by_range(start_date, end_date):
    """Fetch all won opportunities with date_won in [start_date, end_date]."""
    start_str = start_date.strftime("%Y-%m-%d")
    end_str   = end_date.strftime("%Y-%m-%d")
    print(f"Fetching won opportunities ({start_str} → {end_str})...", flush=True)
    opps, skip = [], 0
    while True:
        data = close_get("opportunity/", {
            "status_type":   "won",
            "date_won__gte": start_str,
            "date_won__lte": end_str,
            "_fields":       "id,lead_id,value,date_won,user_id",
            "_skip":         skip,
            "_limit":        100,
        })
        batch = data.get("data", [])
        opps.extend(batch)
        if not data.get("has_more"):
            break
        skip += 100
    print(f"  Won opportunities: {len(opps)}", flush=True)
    return opps


def parse_value(raw):
    """
    Parse Close opportunity value.
    Close stores value in CENTS (integer), so divide by 100 to get dollars.
    e.g. raw=84970000 => $849,700.00
    """
    if raw is None:
        return 0.0
    try:
        cents = float(str(raw).split()[0].replace(",", "").replace("$", ""))
        return cents / 100.0
    except Exception:
        return 0.0


# ── Step 4: UTM Campaign Data ──────────────────────────────────────────────────

def fetch_utm_data(lead_id):
    """
    Return (utm_campaign, utm_content) from the contact with the most UTM data.
    If multiple contacts, prefer the one with utm_campaign set.
    """
    data = close_get("contact/", {
        "lead_id": lead_id,
        "_fields": f"id,custom.{CF_UTM_CAMPAIGN},custom.{CF_UTM_CONTENT}",
        "_limit":  10,
    })
    contacts = data.get("data", [])
    # Prefer contact that has utm_campaign; fall back to first with any UTM data
    best_campaign = None
    best_content  = None
    for c in contacts:
        campaign = c.get(f"custom.{CF_UTM_CAMPAIGN}")
        content  = c.get(f"custom.{CF_UTM_CONTENT}")
        if campaign and not best_campaign:
            best_campaign = str(campaign).strip()
        if content and not best_content:
            best_content = str(content).strip()
    return best_campaign, best_content


# ── Field-Based Booked Leads Fetch ────────────────────────────────────────────

def fetch_leads_by_first_sales_call(start_date, end_date):
    start_str = start_date.strftime("%Y-%m-%d")
    end_str   = end_date.strftime("%Y-%m-%d")
    query     = f'custom.{CF_FIRST_SALES} >= "{start_str}" AND custom.{CF_FIRST_SALES} <= "{end_str}"'
    print(f"Fetching booked leads ({start_str} → {end_str})...", flush=True)
    leads, skip = [], 0
    while True:
        data = close_get("lead/", {
            "query":   query,
            "_fields": (f"id,status_id,custom.{CF_FUNNEL_NAME},"
                        f"custom.{CF_SHOW_UP},custom.{CF_QUALIFIED}"),
            "_limit":  200, "_skip": skip,
        })
        batch = data.get("data", [])
        leads.extend(batch)
        print(f"  Fetched {len(leads)} leads so far...", flush=True)
        if not data.get("has_more"):
            break
        skip += 200
    print(f"  Total booked leads: {len(leads)}", flush=True)
    return leads


# ── Main Aggregation ───────────────────────────────────────────────────────────

def _is_yes(val):
    """Robust truthy check for Close checkbox fields (bool or 'Yes'/'No' string)."""
    if val is None or val is False: return False
    if val is True: return True
    return str(val).strip().lower() in ("yes", "true", "1")


def aggregate_data(start_date, end_date, month_label,
                   won_opps,
                   lead_cache=None, utm_cache=None):
    lead_cache = lead_cache if lead_cache is not None else {}
    utm_cache  = utm_cache  if utm_cache  is not None else {}

    booked_leads = fetch_leads_by_first_sales_call(start_date, end_date)
    meeting_rows = []
    for lead in booked_leads:
        lid = lead.get("id")
        if not lid: continue
        lead_cache[lid] = lead
        if lead.get("status_id") in EXCLUDED_LEAD_STATUS_IDS: continue
        funnel    = get_funnel_name(lead)
        show_up   = _is_yes(lead.get(f"custom.{CF_SHOW_UP}"))
        qualified = _is_yes(lead.get(f"custom.{CF_QUALIFIED}"))
        if lid not in utm_cache:
            utm_cache[lid] = fetch_utm_data(lid)
        utm_campaign, utm_content = utm_cache[lid]
        utm = (utm_content or "Unattributed") if funnel in UTM_CONTENT_FUNNELS               else (utm_campaign or "Unattributed")
        meeting_rows.append({"funnel": funnel, "show_up": show_up,
                              "qualified": qualified, "utm_campaign": utm})
    print(f"  Booked rows after status filter: {len(meeting_rows)}", flush=True)

    closed_rows = []
    for opp in won_opps:
        lid = opp["lead_id"]
        if lid not in lead_cache:
            lead_cache[lid] = fetch_lead(lid)
        lead = lead_cache[lid]
        if lead.get("status_id") in EXCLUDED_LEAD_STATUS_IDS:
            continue
        if opp.get("user_id") in EXCLUDED_CLOSER_USER_IDS:
            continue
        funnel = get_funnel_name(lead)
        value  = parse_value(opp.get("value"))
        if lid not in utm_cache:
            utm_cache[lid] = fetch_utm_data(lid)
        utm_campaign, utm_content = utm_cache[lid]
        utm = (utm_content or "Unattributed") if funnel in UTM_CONTENT_FUNNELS               else (utm_campaign or "Unattributed")
        closed_rows.append({"funnel": funnel, "value": value, "utm_campaign": utm})

    print(f"  Closed-won rows: {len(closed_rows)}", flush=True)

    # Aggregate
    funnel_data = {}
    def slot(funnel, utm):
        funnel_data.setdefault(funnel, {})
        funnel_data[funnel].setdefault(utm, {
            "booked": 0, "showed": 0, "qualified": 0, "closed": 0, "revenue": 0.0})
        return funnel_data[funnel][utm]

    for row in meeting_rows:
        s = slot(row["funnel"], row["utm_campaign"])
        s["booked"]    += 1
        s["showed"]    += 1 if row["show_up"]   else 0
        s["qualified"] += 1 if row["qualified"] else 0
    for row in closed_rows:
        s = slot(row["funnel"], row["utm_campaign"])
        s["closed"]  += 1
        s["revenue"] += row["value"]

    funnel_totals = {}
    for funnel, utms in funnel_data.items():
        t = {"booked": 0, "showed": 0, "qualified": 0, "closed": 0, "revenue": 0.0}
        for v in utms.values():
            for k in t: t[k] += v[k]
        funnel_totals[funnel] = t

    grand = {"booked": 0, "showed": 0, "qualified": 0, "closed": 0, "revenue": 0.0}
    for t in funnel_totals.values():
        for k in grand: grand[k] += t[k]

    group_totals = {}
    for group_label, group_funnels in FUNNEL_GROUPS:
        t = {"booked": 0, "showed": 0, "qualified": 0, "closed": 0, "revenue": 0.0}
        for funnel in group_funnels:
            ft = funnel_totals.get(funnel, {})
            for k in t: t[k] += ft.get(k, 0)
        group_totals[group_label] = t

    now_pac      = datetime.now(PACIFIC)
    _goals       = load_goals()
    _days_in_mon = calendar.monthrange(start_date.year, start_date.month)[1]
    _day_elapsed = end_date.day if end_date.month == start_date.month else _days_in_mon
    data = {
        "funnel_data":   funnel_data,
        "funnel_totals": funnel_totals,
        "grand":         grand,
        "group_totals":  group_totals,
        "generated_at":  now_pac.strftime("%B %d, %Y at %I:%M %p PT"),
        "month_label":   month_label,
        "start_date":    start_date,
        "end_date":      end_date,
        "goals":         _goals,
        "day_of_month":  _day_elapsed,
        "days_in_month": _days_in_mon,
    }
    return data, lead_cache, utm_cache


# ── HTML Helpers ───────────────────────────────────────────────────────────────

def pct(num, denom):
    if not denom:
        return "—"
    return f"{num / denom * 100:.1f}%"

def pct_class(num, denom, high=0.70, low=0.50):
    """CSS class for a percentage — green if good, red if bad."""
    if not denom:
        return ""
    r = num / denom
    if r >= high:
        return "good"
    if r < low:
        return "bad"
    return "mid"

def fmt_currency(val):
    if not val:
        return "$0"
    return f"${val:,.0f}"

def rev_per_close(revenue, closed):
    if not closed:
        return "—"
    return f"${revenue / closed:,.0f}"

def funnel_slug(name):
    return re.sub(r"[^a-z0-9]", "_", name.lower())


# ── Goals ─────────────────────────────────────────────────────────────────────

def load_goals():
    try:
        with open("goals.json", "r") as f:
            return json.load(f)
    except Exception:
        return {}

def calc_on_pace(goal, day_of_month, days_in_month):
    if not goal or not days_in_month: return None
    return round(goal * day_of_month / days_in_month)

def pace_class(booked, on_pace, goal):
    if goal and booked >= goal: return "pace-goal"
    if on_pace and booked >= on_pace: return "pace-on"
    return "pace-behind"

def pace_label(booked, on_pace, goal):
    return "—" if on_pace is None else str(on_pace)

def goal_pct_label(booked, goal):
    if not goal: return "—"
    return f"{round(booked / goal * 100)}% ({goal})"


# ── HTML Generation ────────────────────────────────────────────────────────────

def build_funnel_rows(funnel_data, funnel_totals, goals=None, day_of_month=1, days_in_month=30):
    """Build <tr> HTML for each funnel and its UTM sub-rows, grouped by section."""
    all_funnels = set(funnel_data.keys())
    claimed     = set()
    rows        = []

    def funnel_row_html(funnel):
        t   = funnel_totals.get(funnel, {})
        bo  = t.get("booked", 0)
        # Zero suppression — hide rows with no activity this month
        if bo == 0 and t.get("closed", 0) == 0:
            return []
        sh  = t.get("showed", 0)
        qu  = t.get("qualified", 0)
        cl  = t.get("closed", 0)
        rev = t.get("revenue", 0.0)
        fid = funnel_slug(funnel)

        _goals   = goals or {}
        _goal    = _goals.get(funnel)
        _on_pace = calc_on_pace(_goal, day_of_month, days_in_month)
        _pc      = pace_class(bo, _on_pace, _goal)

        html = [f"""
    <tr class="funnel-row" onclick="toggleUTM('{fid}')" data-fid="{fid}">
      <td class="col-name">
        <span class="chevron" id="chev-{fid}">›</span>{funnel}
      </td>
      <td class="col-num">{bo if bo else "—"}</td>
      <td class="col-pace {_pc}">{pace_label(bo, _on_pace, _goal)}</td>
      <td class="col-goal">{goal_pct_label(bo, _goal)}</td>
      <td class="col-num">{sh if sh else "—"}</td>
      <td class="col-pct {pct_class(sh, bo)}">{pct(sh, bo)}</td>
      <td class="col-num">{qu if qu else "—"}</td>
      <td class="col-pct {pct_class(qu, bo)}">{pct(qu, bo)}</td>
      <td class="col-num">{cl if cl else "—"}</td>
      <td class="col-pct {pct_class(cl, bo, high=0.15, low=0.07)}">{pct(cl, bo)}</td>
      <td class="col-rev">{fmt_currency(rev)}</td>
      <td class="col-num">{rev_per_close(rev, cl)}</td>
    </tr>"""]

        utms = funnel_data.get(funnel, {})
        for utm_label, vals in sorted(utms.items(), key=lambda x: -x[1]["booked"]):
            b  = vals["booked"]
            s  = vals["showed"]
            q  = vals["qualified"]
            c  = vals["closed"]
            r  = vals["revenue"]
            html.append(f"""
    <tr class="utm-row" data-parent="{fid}">
      <td class="col-name col-utm">↳ {utm_label}</td>
      <td class="col-num">{b if b else "—"}</td>
      <td class="col-pace"></td>
      <td class="col-goal"></td>
      <td class="col-num">{s if s else "—"}</td>
      <td class="col-pct {pct_class(s, b)}">{pct(s, b)}</td>
      <td class="col-num">{q if q else "—"}</td>
      <td class="col-pct {pct_class(q, b)}">{pct(q, b)}</td>
      <td class="col-num">{c if c else "—"}</td>
      <td class="col-pct {pct_class(c, b, high=0.15, low=0.07)}">{pct(c, b)}</td>
      <td class="col-rev">{fmt_currency(r)}</td>
      <td class="col-num">{rev_per_close(r, c)}</td>
    </tr>""")
        return html

    # ── Grouped sections ──────────────────────────────────────────────────────
    for group_label, group_funnels in FUNNEL_GROUPS:
        # Only emit a section header if at least one funnel in this group has data
        # (or is in the defined list — always show defined funnels for consistency)
        grp_id = group_label.lower().replace(" ", "_").replace("-", "_")
        rows.append(f"""
    <tr class="section-header-row" onclick="toggleSection('{grp_id}')">
      <td colspan="12">
        <span class="section-chevron open" id="secchev-{grp_id}">›</span>FUNNEL BREAKDOWN — {group_label}
      </td>
    </tr>""")

        for funnel in group_funnels:
            claimed.add(funnel)
            # Always render the row even if no data (shows — across the board)
            # Tag each row with the section group so we can collapse the whole section
            section_rows = funnel_row_html(funnel)
            # Inject data-section attribute into first <tr> of each funnel block
            section_rows = [r.replace('<tr class="funnel-row"', f'<tr class="funnel-row" data-section="{grp_id}"', 1) for r in section_rows]
            rows.extend(section_rows)

    # ── Any funnels not in any group (safety net) ─────────────────────────────
    extras = sorted(all_funnels - claimed)
    if extras:
        rows.append(f"""
    <tr class="section-header-row" onclick="toggleSection('other')">
      <td colspan="12">
        <span class="section-chevron open" id="secchev-other">›</span>FUNNEL BREAKDOWN — OTHER
      </td>
    </tr>""")
        for funnel in extras:
            section_rows = funnel_row_html(funnel)
            section_rows = [r.replace('<tr class="funnel-row"', '<tr class="funnel-row" data-section="other"', 1) for r in section_rows]
            rows.extend(section_rows)

    return "\n".join(rows)


def generate_html(data, month_picker_html="", week_picker_html=""):
    grand       = data["grand"]
    gt          = data["group_totals"]
    ext         = gt.get("EXTERNAL",     {"booked":0,"showed":0,"qualified":0,"closed":0,"revenue":0.0})
    inh         = gt.get("IN-HOUSE",     {"booked":0,"showed":0,"qualified":0,"closed":0,"revenue":0.0})
    goals        = data.get("goals", {})
    day_of_month = data.get("day_of_month", 1)
    days_in_month= data.get("days_in_month", 30)
    funnel_rows  = build_funnel_rows(data["funnel_data"], data["funnel_totals"],
                                     goals, day_of_month, days_in_month)

    g_bo  = grand["booked"]
    g_sh  = grand["showed"]
    g_qu  = grand["qualified"]
    g_cl  = grand["closed"]
    g_rev = grand["revenue"]

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>MTD Funnel Performance — {data['month_label']}</title>
<style>
  :root {{
    --bg:        #f4f6f9;
    --surface:   #ffffff;
    --surface2:  #f0f2f7;
    --border:    #dde1ea;
    --border2:   #e8eaf0;
    --text:      #1a1f36;
    --muted:     #8792a2;
    --muted2:    #5c6680;
    --green:     #0e9f6e;
    --green-dim: #0e9f6e20;
    --red:       #e02424;
    --red-dim:   #e0242420;
    --amber:     #d97706;
    --blue:      #2563eb;
    --purple:    #7c3aed;
    --accent:    #4f46e5;
  }}

  *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}

  body {{
    background: var(--bg);
    color: var(--text);
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Inter, sans-serif;
    font-size: 13px;
    min-height: 100vh;
  }}

  /* Light mode card shadow */
  .kpi {{
    box-shadow: 0 1px 3px rgba(0,0,0,0.07), 0 1px 2px rgba(0,0,0,0.04);
  }}
  table {{
    box-shadow: 0 1px 3px rgba(0,0,0,0.06);
    border-radius: 8px;
    overflow: hidden;
  }}

  /* ── Header ── */
  .header {{
    display: flex;
    justify-content: space-between;
    align-items: flex-start;
    padding: 28px 36px 0;
  }}
  .header-left h1 {{
    font-size: 20px;
    font-weight: 700;
    color: var(--text);
    letter-spacing: -0.01em;
  }}
  .header-left .sub {{
    font-size: 11.5px;
    color: var(--muted2);
    margin-top: 3px;
  }}
  .header-right {{
    text-align: right;
    font-size: 11px;
    color: var(--muted2);
    line-height: 1.6;
  }}
  .header-right .snapshot-label {{
    font-weight: 600;
    color: var(--muted2);
    display: block;
  }}

  /* ── KPI Cards ── */
  .kpis {{
    display: grid;
    grid-template-columns: repeat(5, 1fr);
    gap: 14px;
    padding: 24px 36px;
  }}
  .kpi {{
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 18px 20px;
    position: relative;
    overflow: hidden;
  }}
  .kpi::before {{
    content: "";
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 2px;
    background: var(--kpi-accent, var(--accent));
    opacity: 0.6;
  }}
  .kpi .label {{
    font-size: 10.5px;
    text-transform: uppercase;
    letter-spacing: 0.07em;
    color: var(--muted2);
    margin-bottom: 8px;
  }}
  .kpi .value {{
    font-size: 34px;
    font-weight: 700;
    line-height: 1;
    color: var(--kpi-color, var(--text));
  }}
  .kpi .kpi-sub {{
    font-size: 11px;
    color: var(--muted2);
    margin-top: 5px;
  }}
  .kpi-split {{
    display: flex;
    gap: 6px;
    margin-top: 10px;
    padding-top: 9px;
    border-top: 1px solid var(--border);
  }}
  .kpi-split-item {{
    flex: 1;
    background: var(--surface2);
    border-radius: 6px;
    padding: 6px 8px;
  }}
  .kpi-split-item .split-label {{
    font-size: 9.5px;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    color: var(--muted);
    margin-bottom: 2px;
  }}
  .kpi-split-item .split-value {{
    font-size: 14px;
    font-weight: 700;
    color: var(--text);
    line-height: 1.1;
  }}
  .kpi-split-item .split-rate {{
    font-size: 10px;
    color: var(--muted2);
    margin-top: 1px;
  }}

  /* ── Section label ── */
  .section-label {{
    display: flex;
    align-items: center;
    gap: 10px;
    padding: 6px 36px 10px;
    font-size: 10.5px;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: var(--muted);
  }}
  .section-label::after {{
    content: "";
    flex: 1;
    height: 1px;
    background: var(--border);
  }}

  /* ── Table ── */
  .table-wrap {{
    padding: 0 36px 40px;
    overflow-x: auto;
  }}
  table {{
    width: 100%;
    border-collapse: collapse;
  }}

  thead th {{
    font-size: 10.5px;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    color: var(--muted);
    padding: 8px 12px;
    border-bottom: 1px solid var(--border);
    white-space: nowrap;
    font-weight: 500;
  }}
  thead th.col-num,
  thead th.col-pct,
  thead th.col-rev {{ text-align: right; }}

  /* Funnel parent rows */
  .funnel-row {{
    cursor: pointer;
    border-top: 1px solid var(--border2);
    transition: background 0.1s;
  }}
  .funnel-row:hover {{ background: rgba(79,70,229,0.04); }}
  .funnel-row td {{ padding: 11px 12px; }}

  /* UTM sub-rows */
  .utm-row {{
    display: none;
    background: rgba(79,70,229,0.025);
  }}
  .utm-row.open {{ display: table-row; }}
  .utm-row td {{ padding: 7px 12px; }}
  .utm-row + .utm-row td {{ border-top: 1px solid var(--border2); }}

  /* Total row */
  .total-row {{
    border-top: 2px solid var(--border);
    font-weight: 700;
    background: var(--surface2);
    color: var(--text);
  }}
  .total-row td {{ padding: 12px 12px; }}

  /* Cell types */
  .col-name   {{ min-width: 190px; font-weight: 500; white-space: nowrap; }}
  .col-utm    {{ color: var(--muted2); padding-left: 32px !important; font-weight: 400; }}
  .col-num    {{ text-align: right; color: var(--text); }}
  .col-pct    {{ text-align: right; font-weight: 500; }}
  .col-rev    {{ text-align: right; color: var(--green); font-weight: 500; }}

  .col-pct.good {{ color: var(--green); }}
  .col-pct.bad  {{ color: var(--red); }}
  .col-pct.mid  {{ color: var(--amber); }}

  .col-pace  {{ text-align: right; font-size: 12px; color: var(--muted); }}
  .col-goal  {{ text-align: right; font-size: 12px; color: var(--muted); }}
  .col-pace.pace-goal   {{ color: var(--green); font-weight: 600; }}
  .col-pace.pace-on     {{ color: var(--blue);  font-weight: 500; }}
  .col-pace.pace-behind {{ color: var(--amber); }}

  /* Chevron toggle */
  .chevron {{
    display: inline-block;
    width: 16px;
    color: var(--muted);
    font-size: 14px;
    transition: transform 0.15s ease;
    transform: rotate(0deg);
    line-height: 1;
  }}
  .chevron.open {{ transform: rotate(90deg); color: var(--accent); }}

  /* Section header rows */
  .section-header-row td {{
    padding: 16px 12px 6px;
    font-size: 10.5px;
    text-transform: uppercase;
    letter-spacing: 0.1em;
    color: var(--accent);
    font-weight: 700;
    border-top: 2px solid var(--border);
    background: transparent;
    cursor: pointer;
    user-select: none;
  }}
  .section-header-row td:hover {{ color: #a5b4fc; }}
  .section-header-row:first-child td {{ border-top: none; }}
  .section-chevron {{
    display: inline-block;
    width: 14px;
    margin-right: 4px;
    transition: transform 0.15s ease;
    opacity: 0.7;
  }}
  .section-chevron.open {{ transform: rotate(90deg); }}

  /* Progress bar mini (optional decoration on booked column) */
  /* Pickers row */
  .pickers-row {{
    display: flex;
    align-items: center;
    gap: 8px;
    margin-bottom: 6px;
    flex-wrap: wrap;
    justify-content: flex-end;
  }}
  /* Month picker */
  .month-picker {{
    display: flex;
    align-items: center;
    gap: 7px;
  }}
  .month-picker select, .week-picker select {{
    background: var(--surface);
    color: var(--text);
    border: 1px solid var(--border);
    border-radius: 6px;
    padding: 5px 10px;
    font-size: 12px;
    cursor: pointer;
    outline: none;
  }}
  .month-picker select:hover, .week-picker select:hover {{
    border-color: var(--accent);
  }}
  /* Week picker */
  .week-picker {{
    display: flex;
    align-items: center;
    gap: 7px;
  }}
  .picker-divider {{
    color: var(--border);
    font-size: 16px;
    line-height: 1;
    margin: 0 2px;
  }}
  .archive-badge {{
    display: inline-block;
    background: #fef3c7;
    color: #92400e;
    border: 1px solid #fcd34d;
    border-radius: 4px;
    font-size: 10px;
    font-weight: 600;
    padding: 2px 7px;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    white-space: nowrap;
  }}

  @media (max-width: 960px) {{
    .kpis {{ grid-template-columns: repeat(2, 1fr); }}
    .header {{ flex-direction: column; gap: 12px; }}
    .header-right {{ text-align: left; }}
  }}
</style>
</head>
<body>

<!-- Header -->
<div class="header">
  <div class="header-left">
    <h1>MTD Funnel Performance</h1>
    <p class="sub">Vendingpreneurs · All Sales Calls · {data['month_label']}{data.get('week_range_label','')}</p>
  </div>
  <div class="header-right">
    <div class="pickers-row">
      {month_picker_html}{week_picker_html}
    </div>
    <span class="snapshot-label">{data.get("badge_html","") or "Snapshot"}</span>
    {data['generated_at']}<br>
    Source · Close CRM
  </div>
</div>

<!-- KPI Cards -->
<div class="kpis">
  <div class="kpi" style="--kpi-accent:#4f46e5; --kpi-color:var(--text);">
    <div class="label">Total Booked</div>
    <div class="value">{g_bo}</div>
    <div class="kpi-sub">new first calls MTD</div>
    <div class="kpi-split">
      <div class="kpi-split-item">
        <div class="split-label">External</div>
        <div class="split-value">{ext["booked"]}</div>
        <div class="split-rate">{pct(ext["booked"], g_bo)} of total</div>
      </div>
      <div class="kpi-split-item">
        <div class="split-label">In-House</div>
        <div class="split-value">{inh["booked"]}</div>
        <div class="split-rate">{pct(inh["booked"], g_bo)} of total</div>
      </div>
    </div>
  </div>
  <div class="kpi" style="--kpi-accent:#2563eb; --kpi-color:#2563eb;">
    <div class="label">Showed</div>
    <div class="value">{g_sh}</div>
    <div class="kpi-sub">{pct(g_sh, g_bo)} show rate</div>
    <div class="kpi-split">
      <div class="kpi-split-item">
        <div class="split-label">External</div>
        <div class="split-value">{ext["showed"]}</div>
        <div class="split-rate">{pct(ext["showed"], ext["booked"])} show</div>
      </div>
      <div class="kpi-split-item">
        <div class="split-label">In-House</div>
        <div class="split-value">{inh["showed"]}</div>
        <div class="split-rate">{pct(inh["showed"], inh["booked"])} show</div>
      </div>
    </div>
  </div>
  <div class="kpi" style="--kpi-accent:#7c3aed; --kpi-color:#7c3aed;">
    <div class="label">Qualified</div>
    <div class="value">{g_qu}</div>
    <div class="kpi-sub">{pct(g_qu, g_bo)} qual rate</div>
    <div class="kpi-split">
      <div class="kpi-split-item">
        <div class="split-label">External</div>
        <div class="split-value">{ext["qualified"]}</div>
        <div class="split-rate">{pct(ext["qualified"], ext["booked"])} qual</div>
      </div>
      <div class="kpi-split-item">
        <div class="split-label">In-House</div>
        <div class="split-value">{inh["qualified"]}</div>
        <div class="split-rate">{pct(inh["qualified"], inh["booked"])} qual</div>
      </div>
    </div>
  </div>
  <div class="kpi" style="--kpi-accent:#d97706; --kpi-color:#d97706;">
    <div class="label">Closed Won</div>
    <div class="value">{g_cl}</div>
    <div class="kpi-sub">{pct(g_cl, g_bo)} booked→close · {pct(g_cl, g_qu)} qual→close</div>
    <div class="kpi-split">
      <div class="kpi-split-item">
        <div class="split-label">External</div>
        <div class="split-value">{ext["closed"]}</div>
        <div class="split-rate">{pct(ext["closed"], ext["booked"])} b→c</div>
      </div>
      <div class="kpi-split-item">
        <div class="split-label">In-House</div>
        <div class="split-value">{inh["closed"]}</div>
        <div class="split-rate">{pct(inh["closed"], inh["booked"])} b→c</div>
      </div>
    </div>
  </div>
  <div class="kpi" style="--kpi-accent:#0e9f6e; --kpi-color:#0e9f6e;">
    <div class="label">Closed Revenue</div>
    <div class="value">{fmt_currency(g_rev)}</div>
    <div class="kpi-sub">{rev_per_close(g_rev, g_cl)} avg deal</div>
    <div class="kpi-split">
      <div class="kpi-split-item">
        <div class="split-label">External</div>
        <div class="split-value">{fmt_currency(ext["revenue"])}</div>
        <div class="split-rate">{rev_per_close(ext["revenue"], ext["closed"])} avg</div>
      </div>
      <div class="kpi-split-item">
        <div class="split-label">In-House</div>
        <div class="split-value">{fmt_currency(inh["revenue"])}</div>
        <div class="split-rate">{rev_per_close(inh["revenue"], inh["closed"])} avg</div>
      </div>
    </div>
  </div>
</div>

<!-- Table -->
<div class="section-label">Funnel Breakdown — Booked → Showed → Qualified → Closed Won → Revenue</div>

<div class="table-wrap">
  <table>
    <thead>
      <tr>
        <th class="col-name">Funnel</th>
        <th class="col-num">Booked</th>
        <th class="col-pace">On Pace</th>
        <th class="col-goal">Goal %</th>
        <th class="col-num">Showed</th>
        <th class="col-pct">Show %</th>
        <th class="col-num">Qualified</th>
        <th class="col-pct">Qual %</th>
        <th class="col-num">Closed</th>
        <th class="col-pct">CW %</th>
        <th class="col-rev">Revenue</th>
        <th class="col-num">Rev / Close</th>
      </tr>
    </thead>
    <tbody>
{funnel_rows}

    <tr class="total-row">
      <td class="col-name">TOTAL</td>
      <td class="col-num">{g_bo}</td>
      <td class="col-pace">—</td>
      <td class="col-goal">—</td>
      <td class="col-num">{g_sh}</td>
      <td class="col-pct {pct_class(g_sh, g_bo)}">{pct(g_sh, g_bo)}</td>
      <td class="col-num">{g_qu}</td>
      <td class="col-pct {pct_class(g_qu, g_bo)}">{pct(g_qu, g_bo)}</td>
      <td class="col-num">{g_cl}</td>
      <td class="col-pct {pct_class(g_cl, g_bo, high=0.15, low=0.07)}">{pct(g_cl, g_bo)}</td>
      <td class="col-rev">{fmt_currency(g_rev)}</td>
      <td class="col-num">{rev_per_close(g_rev, g_cl)}</td>
    </tr>
    </tbody>
  </table>
</div>

<script>
  function toggleUTM(fid) {{
    const utmRows = document.querySelectorAll(`.utm-row[data-parent="${{fid}}"]`);
    const chevron = document.getElementById("chev-" + fid);
    const isOpen  = chevron.classList.contains("open");
    utmRows.forEach(r => r.classList.toggle("open", !isOpen));
    chevron.classList.toggle("open", !isOpen);
  }}

  function toggleSection(grpId) {{
    const chevron  = document.getElementById("secchev-" + grpId);
    const isOpen   = chevron.classList.contains("open");
    // All funnel rows and their utm sub-rows in this section
    const funnelRows = document.querySelectorAll(`.funnel-row[data-section="${{grpId}}"]`);
    funnelRows.forEach(row => {{
      row.style.display = isOpen ? "none" : "";
      // Also collapse any open UTM sub-rows within this section
      const fid = row.dataset.fid;
      if (fid) {{
        const utmRows = document.querySelectorAll(`.utm-row[data-parent="${{fid}}"]`);
        if (isOpen) {{
          utmRows.forEach(r => r.classList.remove("open"));
          const utmChev = document.getElementById("chev-" + fid);
          if (utmChev) utmChev.classList.remove("open");
        }}
      }}
    }});
    chevron.classList.toggle("open", !isOpen);
  }}
</script>

</body>
</html>"""


# ── Archive Helpers ────────────────────────────────────────────────────────────

ARCHIVES_DIR = Path("archives")


def scan_monthly_archives():
    """Return sorted list of (YYYY-MM, display_label) for existing monthly archive files."""
    ARCHIVES_DIR.mkdir(exist_ok=True)
    months = []
    for p in sorted(ARCHIVES_DIR.glob("*.html"), reverse=True):
        key = p.stem
        try:
            d = datetime.strptime(key, "%Y-%m")
            months.append((key, d.strftime("%B %Y")))
        except ValueError:
            continue
    return months


def scan_weekly_archives(month_key):
    """
    Return frozen weekly archives whose Monday falls in month_key (YYYY-MM).
    Sorted newest first. Returns list of (file_key, label, monday_date).
    Excludes week-current.html (that's always added separately).
    """
    ARCHIVES_DIR.mkdir(exist_ok=True)
    weeks = []
    for p in sorted(ARCHIVES_DIR.glob("week-20*.html"), reverse=True):
        key = p.stem  # e.g. "week-2026-04-06"
        try:
            monday = datetime.strptime(key, "week-%Y-%m-%d").date()
        except ValueError:
            continue
        if monday.strftime("%Y-%m") == month_key:
            sunday = monday + timedelta(days=6)
            label  = week_display_label(monday, sunday)
            weeks.append((key, label, monday))
    return weeks


def build_month_picker(current_month_key, archive_months, is_in_archives):
    """Build the month <select> HTML."""
    now_pac    = datetime.now(PACIFIC)
    live_key   = now_pac.strftime("%Y-%m")
    live_label = now_pac.strftime("%B %Y")

    live_href = "../index.html" if is_in_archives else "index.html"
    options = [(live_key, live_label, live_href)]
    for key, label in archive_months:
        if key == live_key:
            continue
        href = f"{key}.html" if is_in_archives else f"archives/{key}.html"
        options.append((key, label, href))

    select_opts = ""
    for key, label, href in options:
        sel = "selected" if key == current_month_key else ""
        select_opts += f'<option value="{href}" {sel}>{label}</option>\n      '

    return (
        '<div class="month-picker">'
        + '<select onchange="window.location.href=this.value">'
        + select_opts
        + "</select></div>"
    )


def build_week_picker(current_week_key, month_key, weekly_archives,
                      is_in_archives, is_current_month):
    """
    Build the week <select> HTML. Only shown for months >= WEEKLY_FEATURE_START.
    current_week_key: stem of the current file if it's a week page, else None.
    monthly_href: href for the "Full Month" option.
    """
    if month_key < WEEKLY_FEATURE_START:
        return ""

    now_pac = datetime.now(PACIFIC)
    monday  = current_week_monday()
    sunday  = monday + timedelta(days=6)

    # "Full Month" links back to the monthly page
    if is_in_archives:
        full_month_href = f"{month_key}.html" if not is_current_month else "../index.html"
    else:
        full_month_href = "index.html"

    options = []
    # Full Month always first
    sel = "selected" if current_week_key is None else ""
    options.append(f'<option value="{full_month_href}" {sel}>Full Month</option>')

    # Frozen week archives (newest first)
    for key, label, wmonday in weekly_archives:
        href = f"{key}.html" if is_in_archives else f"archives/{key}.html"
        sel  = "selected" if current_week_key == key else ""
        options.append(f'<option value="{href}" {sel}>{label}</option>')

    # Current week (live, always last) — only for current month
    if is_current_month:
        cur_label = week_display_label(monday, min(sunday, now_pac.date())) + " ▶"
        href = "week-current.html" if is_in_archives else "archives/week-current.html"
        sel  = "selected" if current_week_key == "week-current" else ""
        options.append(f'<option value="{href}" {sel}>{cur_label}</option>')

    select_opts = "\n      ".join(options)
    return (
        '<span class="picker-divider">|</span>'
        '<div class="week-picker">'
        '<select onchange="window.location.href=this.value">'
        + select_opts
        + "</select></div>"
    )


def write_dashboard(data, out_path, month_picker_html, week_picker_html,
                    is_archive_page, is_week_page):
    """Generate HTML and write to out_path."""
    # Add archive/week badges to data for template
    badge = ""
    if is_week_page:
        badge = '<span class="archive-badge">Week View</span>'
    elif is_archive_page:
        badge = '<span class="archive-badge">Archive</span>'
    data["badge_html"] = badge
    html = generate_html(data, month_picker_html=month_picker_html,
                         week_picker_html=week_picker_html)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"Written: {out_path}", flush=True)


# ── Entry Point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MTD Funnel Performance Dashboard")
    parser.add_argument("--month", "-m",
        help="Archive month YYYY-MM", default=None)
    parser.add_argument("--week", "-w",
        help="Archive week YYYY-MM-DD (Monday of the week)", default=None)
    args = parser.parse_args()

    now_pac     = datetime.now(PACIFIC)
    live_month  = now_pac.strftime("%Y-%m")

    print("MTD Funnel Performance Dashboard — Build Start", flush=True)

    lead_cache, utm_cache = {}, {}

    ARCHIVES_DIR.mkdir(exist_ok=True)
    archive_months = scan_monthly_archives()

    # ── MODE: Monthly archive ─────────────────────────────────────────────────
    if args.month:
        try:
            parsed = datetime.strptime(args.month, "%Y-%m")
            m_start = date(parsed.year, parsed.month, 1)
            last_d  = calendar.monthrange(parsed.year, parsed.month)[1]
            m_end   = date(parsed.year, parsed.month, last_d)
        except ValueError:
            print(f"ERROR: --month must be YYYY-MM, got: {args.month}", flush=True)
            sys.exit(1)

        print(f"\n=== Building monthly archive: {args.month} ===", flush=True)
        won_opps = fetch_won_opps_by_range(m_start, m_end)
        data, lead_cache, utm_cache = aggregate_data(
            m_start, m_end, parsed.strftime("%B %Y"),
            won_opps, lead_cache, utm_cache)

        out_path    = ARCHIVES_DIR / f"{args.month}.html"
        weekly_arcs = scan_weekly_archives(args.month)
        month_picker = build_month_picker(args.month, archive_months, is_in_archives=True)
        week_picker  = build_week_picker(None, args.month, weekly_arcs,
                                         is_in_archives=True,
                                         is_current_month=(args.month == live_month))
        write_dashboard(data, out_path, month_picker, week_picker,
                        is_archive_page=True, is_week_page=False)

    # ── MODE: Weekly archive ──────────────────────────────────────────────────
    elif args.week:
        try:
            w_monday = datetime.strptime(args.week, "%Y-%m-%d").date()
        except ValueError:
            print(f"ERROR: --week must be YYYY-MM-DD, got: {args.week}", flush=True)
            sys.exit(1)
        w_sunday = w_monday + timedelta(days=6)
        w_end    = min(w_sunday, now_pac.date())
        month_key = w_monday.strftime("%Y-%m")
        label     = f"{w_monday.strftime('%B %Y')} · {week_display_label(w_monday, w_sunday)}"

        print(f"\n=== Building weekly archive: {args.week} ===", flush=True)
        won_opps = fetch_won_opps_by_range(w_monday, w_end)
        data, lead_cache, utm_cache = aggregate_data(
            w_monday, w_end, label,
            won_opps, lead_cache, utm_cache)
        data["week_range_label"] = ""  # already in month_label for week pages

        out_path     = ARCHIVES_DIR / f"week-{args.week}.html"
        weekly_arcs  = scan_weekly_archives(month_key)
        week_key     = f"week-{args.week}"
        month_picker = build_month_picker(month_key, archive_months, is_in_archives=True)
        week_picker  = build_week_picker(week_key, month_key, weekly_arcs,
                                         is_in_archives=True,
                                         is_current_month=(month_key == live_month))
        write_dashboard(data, out_path, month_picker, week_picker,
                        is_archive_page=True, is_week_page=True)

    # ── MODE: Regular live run — build index.html + week-current.html ─────────
    else:
        m_start   = date(now_pac.year, now_pac.month, 1)
        m_end     = now_pac.date()
        m_label   = now_pac.strftime("%B %Y")
        w_monday  = current_week_monday()
        w_end     = now_pac.date()
        w_sunday  = w_monday + timedelta(days=6)

        # ── Build full month (index.html) ─────────────────────────────────────
        print(f"\n=== Building live month: {m_label} ===", flush=True)
        won_month = fetch_won_opps_by_range(m_start, m_end)
        data_month, lead_cache, utm_cache = aggregate_data(
            m_start, m_end, m_label,
            won_month, lead_cache, utm_cache)
        data_month["week_range_label"] = ""

        weekly_arcs  = scan_weekly_archives(live_month)
        month_picker = build_month_picker(live_month, archive_months, is_in_archives=False)
        week_picker  = build_week_picker(None, live_month, weekly_arcs,
                                         is_in_archives=False, is_current_month=True)
        write_dashboard(data_month, Path("index.html"), month_picker, week_picker,
                        is_archive_page=False, is_week_page=False)

        # ── Build current week (archives/week-current.html) ───────────────────
        print(f"\n=== Building current week: {week_display_label(w_monday, w_end)} ===", flush=True)
        won_week = fetch_won_opps_by_range(w_monday, w_end)
        w_label  = f"{m_label} · {week_display_label(w_monday, w_sunday)}"
        data_week, lead_cache, utm_cache = aggregate_data(
            w_monday, w_end, w_label,
            won_week, lead_cache, utm_cache)
        data_week["week_range_label"] = ""

        week_picker_cur = build_week_picker("week-current", live_month, weekly_arcs,
                                            is_in_archives=True, is_current_month=True)
        month_picker_cur = build_month_picker(live_month, archive_months, is_in_archives=True)
        write_dashboard(data_week, ARCHIVES_DIR / "week-current.html",
                        month_picker_cur, week_picker_cur,
                        is_archive_page=False, is_week_page=True)

    # ── Summary ───────────────────────────────────────────────────────────────
    final_data = data_month if not (args.month or args.week) else data
    g = final_data["grand"]
    print(f"\n=== Build Summary ===", flush=True)
    print(f"  Month:     {final_data['month_label']}", flush=True)
    print(f"  Booked:    {g['booked']}", flush=True)
    print(f"  Showed:    {g['showed']}  ({pct(g['showed'], g['booked'])})", flush=True)
    print(f"  Qualified: {g['qualified']}  ({pct(g['qualified'], g['booked'])})", flush=True)
    print(f"  Closed:    {g['closed']}  ({pct(g['closed'], g['booked'])})", flush=True)
    print(f"  Revenue:   {fmt_currency(g['revenue'])}", flush=True)
