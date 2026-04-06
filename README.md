# MTD Funnel Performance Dashboard

Live dashboard tracking month-to-date funnel performance across Close CRM — bookings, show-ups, qualified, closed-won, and revenue, broken down by funnel and utm_campaign.

**Live:** `YOUR_USERNAME.github.io/mtd-funnel-dashboard/`

---

## What It Tracks

| Metric | Source |
|--------|--------|
| Booked | Qualifying meeting activities (same classification as Capacity Dashboard) |
| Showed | `First Call Show Up (Opp)` custom field on most-recent opportunity |
| Qualified | `Qualified (Opp)` custom field on most-recent opportunity |
| Closed Won | Opportunities with `status_type=won` and `date_won` in current month |
| Revenue | `value` field on closed-won opportunities |
| UTM Campaign | `utm_campaign` contact custom field (contact with most data wins) |

All rows deduplicated by `lead_id` — one lead = one booking count.  
Booked/Showed/Qualified share the same deduplicated meeting set.  
Closed Won is pulled **independently** by `date_won` to avoid attribution timing issues.

---

## Setup

### 1. Create the GitHub repo
```
mtd-funnel-dashboard
```
Enable **GitHub Pages** → Branch: `main` → Folder: `/ (root)`

### 2. Add repository secret
`Settings → Secrets → Actions → New repository secret`

| Name | Value |
|------|-------|
| `CLOSE_API_KEY` | Your Close CRM API key |

### 3. Configure cron-job.org
Create a job that fires every 15 minutes:

- **URL:** `https://api.github.com/repos/YOUR_USERNAME/mtd-funnel-dashboard/actions/workflows/update.yml/dispatches`
- **Method:** POST
- **Headers:**
  - `Authorization: Bearer YOUR_GITHUB_PAT` *(needs `workflow` scope)*
  - `Accept: application/vnd.github+json`
  - `Content-Type: application/json`
- **Body:** `{"ref":"main"}`

### 4. Generate a GitHub PAT
`GitHub → Settings → Developer Settings → Personal Access Tokens → Fine-grained`
Scope needed: **Actions (write)** on this repo.

---

## Meeting Classification Rules (identical to Capacity Dashboard)

Meetings must survive ALL filters:

1. Calendar status: exclude `canceled` / `declined`
2. User exclusions: Stephen Olivas, Ahmad Bukhari, Kristin Nelson, Spencer Reynolds
3. Title classification (first-match-wins):
   - Starts with "Canceled" → exclude
   - Contains "Vending Quick Discovery" → exclude
   - Matches scraper "Next Steps" patterns → **include** *(checked before follow-up)*
   - Contains follow-up / F/U / Next Steps / reschedule → exclude
   - Contains "Anthony" + "Q&A" → exclude
   - Contains enrollment patterns → exclude
   - Matches standard first-call patterns → **include**
   - Default → exclude
4. Lead status: exclude `Canceled (by Lead)` and `Outside the US`

---

## Custom Field Reference

| Field | ID | Object |
|-------|----|--------|
| Funnel Name DEAL | `cf_xqDQE8fkPsWa0RNEve7hcaxKblCe6489XeZGRDzyPdX` | Lead |
| First Call Show Up | `cf_OPyvpU45RdvjLqfm8V1VWwNxrGKogEH2IBJmfCj0Uhq` | Opportunity |
| Qualified | `cf_ZDx7NBQaDzV1yYrFcBMzt6cIYj81dAcswpNN0CQzCPS` | Opportunity |
| utm_campaign | `cf_jnbd0xzUY3tuxzxiGxBs2hONuExeXMvAoTUM2R64Lq3` | Contact |

---

## Performance

| Step | API Calls | Notes |
|------|-----------|-------|
| Meeting pagination | ~120 | All meetings, filter in Python |
| Lead fetches | ~80–200 | Only for meetings surviving all filters |
| Opportunity fetches | ~80–200 | One per meeting lead |
| Contact/UTM fetches | ~80–200 | One per unique lead |
| Won opp pagination | ~1–5 | Filtered by date_won in Close |
| Lead/UTM for won opps | ~50–150 | Deduplicated with cache |
| **Total** | **~400–700** | ~5–7 min at 0.5s throttle |

Workflow timeout: **20 minutes**.
