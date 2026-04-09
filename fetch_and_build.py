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
from datetime import datetime, date
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
CF_UTM_CONTENT  = "cf_R7o66i0XPycLQHlxOLbIqk6c6j3oB8CzxF3e3apI1hn"   # utm_content (contact)

# Funnels that use utm_content instead of utm_campaign for sub-breakdown
UTM_CONTENT_FUNNELS = {"Internal Webinar"}

CLOSED_WON_STATUS_ID = "stat_0oW3iRpVp9z5DJq0cuwI1HgR0XhHAhykEPPIq4TFsxd"

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


def filter_meetings_mtd(meetings, target_month=None):
    """
    Keep qualifying meetings for the target month (Pacific time).
    target_month: date(YYYY, MM, 1) or None for current month.
    For the current month, cap at today. For past months, include the full month.
    """
    now_pac = datetime.now(PACIFIC)
    if target_month is None:
        target_month = now_pac.replace(day=1).date()
        end_date = now_pac.date()
    else:
        last_day = calendar.monthrange(target_month.year, target_month.month)[1]
        end_date = date(target_month.year, target_month.month, last_day)

    month_start = datetime(target_month.year, target_month.month, 1,
                           tzinfo=PACIFIC)

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

        if dt_pac < month_start or dt_pac.date() > end_date:
            continue

        if not is_valid_meeting(m):
            continue

        m["_date_pac"] = dt_pac.date()
        valid.append(m)

    return valid


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


def fetch_won_opps_mtd(target_month=None):
    """
    Fetch all won opportunities with date_won in the target month.
    target_month: date(YYYY, MM, 1) or None for current month.
    """
    now_pac = datetime.now(PACIFIC)
    if target_month is None:
        month_start = now_pac.replace(day=1).strftime("%Y-%m-%d")
        end_date    = now_pac.strftime("%Y-%m-%d")
    else:
        last_day    = calendar.monthrange(target_month.year, target_month.month)[1]
        month_start = target_month.strftime("%Y-%m-%d")
        end_date    = date(target_month.year, target_month.month, last_day).strftime("%Y-%m-%d")

    print(f"Fetching won opportunities ({month_start} → {end_date})...", flush=True)

    opps, skip = [], 0
    while True:
        data = close_get("opportunity/", {
            "status_type":    "won",
            "date_won__gte":  month_start,
            "date_won__lte":  end_date,
            "_fields":        f"id,lead_id,value,date_won",
            "_skip":          skip,
            "_limit":         100,
        })
        batch = data.get("data", [])
        opps.extend(batch)
        if not data.get("has_more"):
            break
        skip += 100

    print(f"  Won opportunities MTD: {len(opps)}", flush=True)
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


# ── Main Aggregation ───────────────────────────────────────────────────────────

def build_dashboard_data(target_month=None):
    """
    target_month: date(YYYY, MM, 1) or None for current month.
    """
    print("\n=== MTD Funnel Performance — Starting Build ===\n", flush=True)
    if target_month:
        print(f"Archive mode: {target_month.strftime('%B %Y')}", flush=True)

    # ── Meetings ──────────────────────────────────────────────────────────────
    all_meetings  = fetch_all_meetings()
    mtd_meetings  = filter_meetings_mtd(all_meetings, target_month)
    print(f"MTD qualifying meetings (pre-dedup):  {len(mtd_meetings)}", flush=True)

    deduped = deduplicate_meetings(mtd_meetings)
    print(f"MTD qualifying meetings (post-dedup): {len(deduped)}", flush=True)

    # ── Per-meeting lead / opp / UTM lookups ──────────────────────────────────
    lead_cache = {}
    utm_cache  = {}
    meeting_rows = []

    for i, m in enumerate(deduped):
        lid = m["lead_id"]
        print(f"  Meeting {i+1}/{len(deduped)} — lead {lid}", flush=True)

        # Lead
        if lid not in lead_cache:
            lead_cache[lid] = fetch_lead(lid)
        lead = lead_cache[lid]

        # Skip excluded lead statuses
        if lead.get("status_id") in EXCLUDED_LEAD_STATUS_IDS:
            print(f"    → Skipped (excluded lead status)", flush=True)
            continue

        funnel = get_funnel_name(lead)

        # Show Up and Qualified are lead-level custom fields — read directly
        # Close can return booleans (True/False) or strings ("Yes"/"No") depending
        # on how the field was configured, so check both.
        def _is_yes(val):
            if val is None or val is False:
                return False
            if val is True:
                return True
            return str(val).strip().lower() in ("yes", "true", "1")

        show_up   = _is_yes(lead.get(f"custom.{CF_SHOW_UP}"))
        qualified = _is_yes(lead.get(f"custom.{CF_QUALIFIED}"))

        # UTM data — fetch both campaign and content in one call
        if lid not in utm_cache:
            utm_cache[lid] = fetch_utm_data(lid)
        utm_campaign, utm_content = utm_cache[lid]

        # Internal Webinar uses utm_content for sub-breakdown; all others use utm_campaign
        utm = (utm_content or "Unattributed") if funnel in UTM_CONTENT_FUNNELS               else (utm_campaign or "Unattributed")

        meeting_rows.append({
            "lead_id":      lid,
            "funnel":       funnel,
            "show_up":      show_up,
            "qualified":    qualified,
            "utm_campaign": utm,
        })

    print(f"\nMeeting rows after all filters: {len(meeting_rows)}", flush=True)

    # ── Closed-Won Opportunities ───────────────────────────────────────────────
    won_opps   = fetch_won_opps_mtd(target_month)
    closed_rows = []

    for i, opp in enumerate(won_opps):
        lid = opp["lead_id"]
        print(f"  Won opp {i+1}/{len(won_opps)} — lead {lid}", flush=True)

        if lid not in lead_cache:
            lead_cache[lid] = fetch_lead(lid)
        lead = lead_cache[lid]

        funnel = get_funnel_name(lead)
        value  = parse_value(opp.get("value"))

        if lid not in utm_cache:
            utm_cache[lid] = fetch_utm_data(lid)
        utm_campaign, utm_content = utm_cache[lid]
        utm = (utm_content or "Unattributed") if funnel in UTM_CONTENT_FUNNELS               else (utm_campaign or "Unattributed")

        closed_rows.append({
            "lead_id":      lid,
            "funnel":       funnel,
            "value":        value,
            "utm_campaign": utm,
        })

    print(f"\nClosed-won rows: {len(closed_rows)}", flush=True)

    # ── Aggregate into funnel × utm structure ─────────────────────────────────
    # funnel_data[funnel][utm] = {booked, showed, qualified, closed, revenue}

    funnel_data = {}

    def slot(funnel, utm):
        funnel_data.setdefault(funnel, {})
        funnel_data[funnel].setdefault(utm, {
            "booked": 0, "showed": 0, "qualified": 0,
            "closed": 0, "revenue": 0.0
        })
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

    # Roll up per-funnel totals
    funnel_totals = {}
    for funnel, utms in funnel_data.items():
        t = {"booked": 0, "showed": 0, "qualified": 0, "closed": 0, "revenue": 0.0}
        for v in utms.values():
            for k in t:
                t[k] += v[k]
        funnel_totals[funnel] = t

    # Grand totals
    grand = {"booked": 0, "showed": 0, "qualified": 0, "closed": 0, "revenue": 0.0}
    for t in funnel_totals.values():
        for k in grand:
            grand[k] += t[k]

    # Group totals (External / In-House) for KPI sub-breakdown
    group_totals = {}
    for group_label, group_funnels in FUNNEL_GROUPS:
        t = {"booked": 0, "showed": 0, "qualified": 0, "closed": 0, "revenue": 0.0}
        for funnel in group_funnels:
            ft = funnel_totals.get(funnel, {})
            for k in t:
                t[k] += ft.get(k, 0)
        group_totals[group_label] = t

    now_pac      = datetime.now(PACIFIC)
    display_date = datetime(target_month.year, target_month.month, 1, tzinfo=PACIFIC)                    if target_month else now_pac
    return {
        "funnel_data":   funnel_data,
        "funnel_totals": funnel_totals,
        "grand":         grand,
        "group_totals":  group_totals,
        "generated_at":  now_pac.strftime("%B %d, %Y at %I:%M %p PT"),
        "month_label":   display_date.strftime("%B %Y"),
    }


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


# ── HTML Generation ────────────────────────────────────────────────────────────

def build_funnel_rows(funnel_data, funnel_totals):
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

        html = [f"""
    <tr class="funnel-row" onclick="toggleUTM('{fid}')" data-fid="{fid}">
      <td class="col-name">
        <span class="chevron" id="chev-{fid}">›</span>{funnel}
      </td>
      <td class="col-num">{bo if bo else "—"}</td>
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
      <td colspan="10">
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
      <td colspan="10">
        <span class="section-chevron open" id="secchev-other">›</span>FUNNEL BREAKDOWN — OTHER
      </td>
    </tr>""")
        for funnel in extras:
            section_rows = funnel_row_html(funnel)
            section_rows = [r.replace('<tr class="funnel-row"', '<tr class="funnel-row" data-section="other"', 1) for r in section_rows]
            rows.extend(section_rows)

    return "\n".join(rows)


def generate_html(data, month_picker_html=""):
    grand       = data["grand"]
    gt          = data["group_totals"]
    ext         = gt.get("EXTERNAL",     {"booked":0,"showed":0,"qualified":0,"closed":0,"revenue":0.0})
    inh         = gt.get("IN-HOUSE",     {"booked":0,"showed":0,"qualified":0,"closed":0,"revenue":0.0})
    funnel_rows = build_funnel_rows(data["funnel_data"], data["funnel_totals"])

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
  /* Month picker */
  .month-picker {{
    margin-bottom: 8px;
  }}
  .month-picker select {{
    background: var(--surface);
    color: var(--text);
    border: 1px solid var(--border);
    border-radius: 6px;
    padding: 5px 10px;
    font-size: 12px;
    cursor: pointer;
    outline: none;
  }}
  .month-picker select:hover {{
    border-color: var(--accent);
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
    margin-bottom: 6px;
    text-transform: uppercase;
    letter-spacing: 0.05em;
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
    <p class="sub">Vendingpreneurs · All Sales Calls · {data['month_label']}</p>
  </div>
  <div class="header-right">
    {month_picker_html}
    <span class="snapshot-label">Snapshot</span>
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

def scan_archives():
    """Return sorted list of (YYYY-MM, display_label) for existing archive files."""
    ARCHIVES_DIR.mkdir(exist_ok=True)
    months = []
    for p in sorted(ARCHIVES_DIR.glob("*.html"), reverse=True):
        key = p.stem  # e.g. "2026-03"
        try:
            d = datetime.strptime(key, "%Y-%m")
            months.append((key, d.strftime("%B %Y")))
        except ValueError:
            continue
    return months

def build_month_picker(current_key, archive_months, is_archive):
    """
    Build the <select> HTML for switching months.
    current_key: YYYY-MM string for the page being rendered.
    archive_months: list of (key, label) from scan_archives().
    is_archive: True if this page is itself an archive page.
    """
    now_pac = datetime.now(PACIFIC)
    live_key   = now_pac.strftime("%Y-%m")
    live_label = now_pac.strftime("%B %Y")

    # All options: current live month first, then archives newest→oldest
    # Paths differ depending on whether the page being rendered is index.html or archives/YYYY-MM.html
    live_href = "../index.html" if is_archive else "index.html"
    options = [(live_key, live_label, live_href)]
    for key, label in archive_months:
        if key == live_key:
            continue  # don't double-list current month if somehow archived
        # From index.html → archives/YYYY-MM.html (subdirectory prefix needed)
        # From archives/YYYY-MM.html → YYYY-MM.html (same directory, no prefix)
        href = f"{key}.html" if is_archive else f"archives/{key}.html"
        options.append((key, label, href))

    badge = '<span class="archive-badge">Archive</span><br>' if is_archive else ""

    select_opts = ""
    for key, label, href in options:
        sel = "selected" if key == current_key else ""
        select_opts += f"<option value=\"{href}\" {sel}>{label}</option>\n      "

    return (
        badge +
        '<div class="month-picker">'
        '<select onchange="window.location.href=this.value">'
        + select_opts +
        "</select></div>"
    )


# ── Entry Point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MTD Funnel Performance Dashboard")
    parser.add_argument(
        "--month", "-m",
        help="Archive month to build, format YYYY-MM (omit for current month)",
        default=None,
    )
    args = parser.parse_args()

    # Determine target month
    target_month = None
    is_archive   = False
    now_pac      = datetime.now(PACIFIC)

    if args.month:
        try:
            parsed = datetime.strptime(args.month, "%Y-%m")
            target_month = date(parsed.year, parsed.month, 1)
            is_archive   = True
        except ValueError:
            print(f"ERROR: --month must be YYYY-MM format, got: {args.month}", flush=True)
            sys.exit(1)

    print(f"MTD Funnel Performance Dashboard — Build Start", flush=True)
    data = build_dashboard_data(target_month)

    # Determine output path
    ARCHIVES_DIR.mkdir(exist_ok=True)
    if is_archive:
        out_path    = ARCHIVES_DIR / f"{args.month}.html"
        current_key = args.month
    else:
        out_path    = Path("index.html")
        current_key = now_pac.strftime("%Y-%m")

    # Build month picker from existing archives
    archive_months  = scan_archives()
    month_picker    = build_month_picker(current_key, archive_months, is_archive)

    print("\nGenerating HTML...", flush=True)
    html = generate_html(data, month_picker_html=month_picker)

    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"Done — {out_path} written.", flush=True)

    # Summary
    g = data["grand"]
    print(f"\n=== Build Summary ===", flush=True)
    print(f"  Month:     {data['month_label']}", flush=True)
    print(f"  Booked:    {g['booked']}", flush=True)
    print(f"  Showed:    {g['showed']}  ({pct(g['showed'], g['booked'])})", flush=True)
    print(f"  Qualified: {g['qualified']}  ({pct(g['qualified'], g['booked'])})", flush=True)
    print(f"  Closed:    {g['closed']}  ({pct(g['closed'], g['booked'])})", flush=True)
    print(f"  Revenue:   {fmt_currency(g['revenue'])}", flush=True)
