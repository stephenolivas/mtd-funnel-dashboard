#!/usr/bin/env python3
"""
MTD Funnel Performance Dashboard
Pulls meeting bookings, show-up, qualified, closed-won, and UTM campaign data
from Close CRM and builds a static HTML dashboard.
"""

import os
import re
import time
import json
from datetime import datetime, date
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


def filter_meetings_mtd(meetings):
    """Keep only current-month qualifying meetings (Pacific time)."""
    now_pac  = datetime.now(PACIFIC)
    mtd_start = now_pac.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

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

        # Must be within current month (past meetings only — future bookings are
        # intentionally excluded here; booked = meeting has occurred or is scheduled
        # within the current month window up to now)
        if dt_pac < mtd_start or dt_pac > now_pac:
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


def fetch_won_opps_mtd():
    """
    Fetch all won opportunities with date_won in the current month (Pacific).
    Uses opportunity endpoint with status_type=won and date_won filters.
    Note: Close API date filters work on /opportunity/ — no Python-side filtering needed.
    """
    now_pac = datetime.now(PACIFIC)
    month_start = now_pac.replace(day=1).strftime("%Y-%m-%d")
    today       = now_pac.strftime("%Y-%m-%d")

    print(f"Fetching won opportunities ({month_start} → {today})...", flush=True)

    opps, skip = [], 0
    while True:
        data = close_get("opportunity/", {
            "status_type":    "won",
            "date_won__gte":  month_start,
            "date_won__lte":  today,
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

def build_dashboard_data():
    print("\n=== MTD Funnel Performance — Starting Build ===\n", flush=True)

    # ── Meetings ──────────────────────────────────────────────────────────────
    all_meetings  = fetch_all_meetings()
    mtd_meetings  = filter_meetings_mtd(all_meetings)
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
    won_opps   = fetch_won_opps_mtd()
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

    now_pac = datetime.now(PACIFIC)
    return {
        "funnel_data":   funnel_data,
        "funnel_totals": funnel_totals,
        "grand":         grand,
        "generated_at":  now_pac.strftime("%B %d, %Y at %I:%M %p PT"),
        "month_label":   now_pac.strftime("%B %Y"),
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


def generate_html(data):
    grand       = data["grand"]
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
    --bg:        #0d0f18;
    --surface:   #161927;
    --surface2:  #1d2035;
    --border:    #272a3d;
    --border2:   #1e2136;
    --text:      #e2e8f4;
    --muted:     #5a607a;
    --muted2:    #7c849e;
    --green:     #22c55e;
    --green-dim: #16a34a40;
    --red:       #ef4444;
    --red-dim:   #b91c1c40;
    --amber:     #f59e0b;
    --blue:      #60a5fa;
    --purple:    #c084fc;
    --accent:    #818cf8;
  }}

  *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}

  body {{
    background: var(--bg);
    color: var(--text);
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Inter, sans-serif;
    font-size: 13px;
    min-height: 100vh;
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
    color: #fff;
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
    color: var(--muted);
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
    color: var(--kpi-color, #fff);
  }}
  .kpi .kpi-sub {{
    font-size: 11px;
    color: var(--muted2);
    margin-top: 5px;
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
  .funnel-row:hover {{ background: rgba(255,255,255,0.025); }}
  .funnel-row td {{ padding: 11px 12px; }}

  /* UTM sub-rows */
  .utm-row {{
    display: none;
    background: rgba(255,255,255,0.012);
  }}
  .utm-row.open {{ display: table-row; }}
  .utm-row td {{ padding: 7px 12px; }}
  .utm-row + .utm-row td {{ border-top: 1px solid var(--border2); }}

  /* Total row */
  .total-row {{
    border-top: 2px solid var(--border);
    font-weight: 700;
    background: var(--surface2);
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
    <span class="snapshot-label">Snapshot</span>
    {data['generated_at']}<br>
    Source · Close CRM
  </div>
</div>

<!-- KPI Cards -->
<div class="kpis">
  <div class="kpi" style="--kpi-accent:#818cf8; --kpi-color:#fff;">
    <div class="label">Total Booked</div>
    <div class="value">{g_bo}</div>
    <div class="kpi-sub">new first calls MTD</div>
  </div>
  <div class="kpi" style="--kpi-accent:#60a5fa; --kpi-color:#60a5fa;">
    <div class="label">Showed</div>
    <div class="value">{g_sh}</div>
    <div class="kpi-sub">{pct(g_sh, g_bo)} show rate</div>
  </div>
  <div class="kpi" style="--kpi-accent:#c084fc; --kpi-color:#c084fc;">
    <div class="label">Qualified</div>
    <div class="value">{g_qu}</div>
    <div class="kpi-sub">{pct(g_qu, g_bo)} qual rate</div>
  </div>
  <div class="kpi" style="--kpi-accent:#f59e0b; --kpi-color:#f59e0b;">
    <div class="label">Closed Won</div>
    <div class="value">{g_cl}</div>
    <div class="kpi-sub">{pct(g_cl, g_bo)} booked→close</div>
  </div>
  <div class="kpi" style="--kpi-accent:#22c55e; --kpi-color:#22c55e;">
    <div class="label">Closed Revenue</div>
    <div class="value">{fmt_currency(g_rev)}</div>
    <div class="kpi-sub">{rev_per_close(g_rev, g_cl)} avg deal</div>
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


# ── Entry Point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("MTD Funnel Performance Dashboard — Build Start", flush=True)
    data = build_dashboard_data()

    print("\nGenerating HTML...", flush=True)
    html = generate_html(data)

    out_path = "index.html"
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
