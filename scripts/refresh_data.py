"""
PhantomBuster Content Dashboard — Data Refresh Script
Pulls data from AirOps API, GA4 Data API, and Amplitude Export API.
Writes updated dashboard-data.json.
Run manually or via GitHub Actions on a schedule.

Required env vars:
  GA4_CREDENTIALS_JSON   — base64-encoded GA4 service account JSON
  GA4_PROPERTY_ID        — GA4 property ID (e.g. 368863425)
  AMPLITUDE_API_KEY      — Amplitude project API key
  AMPLITUDE_SECRET_KEY   — Amplitude project secret key
  AIROPS_API_KEY         — AirOps API key
  AIROPS_WORKSPACE_ID    — AirOps workspace ID
"""

import os
import sys
import json
import base64
import datetime
import tempfile
from pathlib import Path

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
DATA_FILE = Path(__file__).resolve().parent.parent / "dashboard-data.json"


def load_data():
    with open(DATA_FILE) as f:
        return json.load(f)


def save_data(data):
    data["meta"]["last_updated"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    with open(DATA_FILE, "w") as f:
        json.dump(data, f, indent=2, default=lambda x: None)
    print(f"Saved {DATA_FILE} ({DATA_FILE.stat().st_size:,} bytes)")


# ---------------------------------------------------------------------------
# GA4 Refresh
# ---------------------------------------------------------------------------
def refresh_ga4(data):
    creds_b64 = os.environ.get("GA4_CREDENTIALS_JSON")
    prop_id = os.environ.get("GA4_PROPERTY_ID", "368863425")
    if not creds_b64:
        print("SKIP GA4: GA4_CREDENTIALS_JSON not set")
        return

    import warnings
    warnings.filterwarnings("ignore")

    # Write credentials to temp file
    creds_json = base64.b64decode(creds_b64)
    tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
    tmp.write(creds_json.decode())
    tmp.close()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = tmp.name

    from google.analytics.data_v1beta import BetaAnalyticsDataClient
    from google.analytics.data_v1beta.types import (
        RunReportRequest, DateRange, Dimension, Metric, FilterExpression, Filter,
    )

    client = BetaAnalyticsDataClient()
    prop = f"properties/{prop_id}"

    # --- Blog signups ---
    request = RunReportRequest(
        property=prop,
        dimensions=[Dimension(name="landingPagePlusQueryString")],
        metrics=[Metric(name="sessions"), Metric(name="conversions")],
        date_ranges=[DateRange(start_date="30daysAgo", end_date="yesterday")],
        dimension_filter=FilterExpression(
            filter=Filter(
                field_name="landingPagePlusQueryString",
                string_filter=Filter.StringFilter(
                    value="/blog/", match_type=Filter.StringFilter.MatchType.CONTAINS
                ),
            )
        ),
        limit=500,
    )
    response = client.run_report(request)
    blog_signups = {}
    for r in response.rows:
        page = r.dimension_values[0].value
        sessions = int(r.metric_values[0].value)
        conversions = int(r.metric_values[1].value)
        blog_signups[page] = {"signups": conversions, "sessions": sessions}

    # --- Automations signups ---
    request2 = RunReportRequest(
        property=prop,
        dimensions=[Dimension(name="landingPagePlusQueryString")],
        metrics=[Metric(name="sessions"), Metric(name="conversions"), Metric(name="activeUsers")],
        date_ranges=[DateRange(start_date="30daysAgo", end_date="yesterday")],
        dimension_filter=FilterExpression(
            filter=Filter(
                field_name="landingPagePlusQueryString",
                string_filter=Filter.StringFilter(
                    value="/automations/", match_type=Filter.StringFilter.MatchType.CONTAINS
                ),
            )
        ),
        limit=500,
    )
    response2 = client.run_report(request2)
    auto_signups = {}
    for r in response2.rows:
        page = r.dimension_values[0].value
        sessions = int(r.metric_values[0].value)
        conversions = int(r.metric_values[1].value)
        auto_signups[page] = {"signups": conversions, "sessions": sessions}

    # Merge into pages
    def merge_signups(pages_list, signups_map, url_prefix):
        for p in pages_list:
            url = p.get("url", "")
            path = "/" + url.replace(url_prefix, "").lstrip("/") if url_prefix in url else "/" + url
            if not path.startswith("/"):
                path = "/" + path
            match = signups_map.get(path) or signups_map.get(path.rstrip("/"))
            if match:
                p["signups"] = match["signups"]
                p["signup_rate"] = round(match["signups"] / match["sessions"] * 100, 2) if match["sessions"] > 0 else 0
            else:
                p.setdefault("signups", 0)
                p.setdefault("signup_rate", 0)

    merge_signups(data.get("pages", []), blog_signups, "phantombuster.com")
    for period in data.get("pages_by_period", {}).values():
        merge_signups(period, blog_signups, "phantombuster.com")
    merge_signups(data.get("automations_pages", []), auto_signups, "phantombuster.com")

    # Update automations summary
    auto_pages = data.get("automations_pages", [])
    total_signups = sum(p.get("signups", 0) for p in auto_pages)
    total_sessions = sum(p.get("sessions", 0) or 0 for p in auto_pages)
    if "automations_summary" in data:
        data["automations_summary"]["total_signups"] = total_signups
        data["automations_summary"]["overall_signup_rate"] = round(total_signups / total_sessions * 100, 2) if total_sessions > 0 else 0

    os.unlink(tmp.name)
    print(f"GA4: Merged signups for {len(blog_signups)} blog + {len(auto_signups)} automations pages")


# ---------------------------------------------------------------------------
# Amplitude Refresh
# ---------------------------------------------------------------------------
def refresh_amplitude(data):
    api_key = os.environ.get("AMPLITUDE_API_KEY")
    secret_key = os.environ.get("AMPLITUDE_SECRET_KEY")
    if not api_key or not secret_key:
        print("SKIP Amplitude: AMPLITUDE_API_KEY or AMPLITUDE_SECRET_KEY not set")
        return

    import requests
    from datetime import datetime, timedelta

    end = datetime.now() - timedelta(days=1)
    start = end - timedelta(days=90)

    # Use Amplitude Dashboard REST API (v1) to run a segmentation query
    # Signups from AI referrers grouped by referrer domain
    url = "https://amplitude.com/api/2/events/segmentation"
    params = {
        "e": json.dumps({"event_type": "front passed signup 1st step", "filters": [{"subprop_type": "event", "subprop_key": "Referrer", "subprop_op": "contains", "subprop_value": ["chatgpt.com", "claude.ai", "perplexity.ai", "gemini.google.com"]}], "group_by": [{"type": "event", "value": "Referrer"}]}),
        "m": "uniques",
        "start": start.strftime("%Y%m%d"),
        "end": end.strftime("%Y%m%d"),
        "i": "30",
    }
    resp = requests.get(url, params=params, auth=(api_key, secret_key), timeout=30)

    if resp.status_code == 200:
        result = resp.json()
        series = result.get("data", {}).get("series", [])
        labels = result.get("data", {}).get("seriesLabels", [])

        ai_map = {}
        for label_list, values in zip(labels, series):
            ref = label_list[1] if len(label_list) > 1 else str(label_list[0])
            ref_lower = ref.lower()
            source = None
            if "chatgpt.com" in ref_lower or "chat.openai.com" in ref_lower:
                source = "ChatGPT"
            elif "perplexity.ai" in ref_lower:
                source = "Perplexity"
            elif "claude.ai" in ref_lower:
                source = "Claude"
            elif "gemini.google.com" in ref_lower:
                source = "Gemini"
            if source:
                total = sum(v.get("value", 0) if isinstance(v, dict) else v for v in values)
                ai_map.setdefault(source, {"signups": 0})
                ai_map[source]["signups"] += total

        if "ai_referral_funnel" in data and ai_map:
            for source, vals in ai_map.items():
                if source in data["ai_referral_funnel"].get("by_source", {}):
                    data["ai_referral_funnel"]["by_source"][source]["signups"] = vals["signups"]
            data["ai_referral_funnel"]["aggregate"]["signups"] = sum(
                v.get("signups", 0) for v in data["ai_referral_funnel"]["by_source"].values()
            )
        print(f"Amplitude: Updated AI referral signups — {json.dumps(ai_map)}")
    else:
        print(f"Amplitude API error: {resp.status_code} {resp.text[:200]}")


# ---------------------------------------------------------------------------
# AirOps Refresh
# ---------------------------------------------------------------------------
def refresh_airops(data):
    api_key = os.environ.get("AIROPS_API_KEY")
    workspace_id = os.environ.get("AIROPS_WORKSPACE_ID")
    if not api_key:
        print("SKIP AirOps: AIROPS_API_KEY not set")
        return

    import requests

    base = "https://app.airops.com/api/v1"
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    brand_kit_id = 6716

    # Query weekly AEO analytics
    yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    analytics_url = f"{base}/brand_kits/{brand_kit_id}/analytics"

    payload = {
        "metrics": ["mention_rate", "share_of_voice", "citation_rate", "citation_count",
                     "sentiment_score", "average_position", "answer_count", "first_mention_rate"],
        "dimensions": ["date"],
        "grain": "weekly",
        "start_date": "2026-01-01",
        "end_date": yesterday,
        "countries": ["US"],
    }
    resp = requests.post(analytics_url, json=payload, headers=headers, timeout=30)
    if resp.status_code == 200:
        rows = resp.json().get("data", [])
        if rows:
            date_map = {"01": "Jan", "02": "Feb", "03": "Mar", "04": "Apr", "05": "May",
                        "06": "Jun", "07": "Jul", "08": "Aug", "09": "Sep", "10": "Oct",
                        "11": "Nov", "12": "Dec"}
            weekly = []
            for r in rows:
                d = r["date"]
                month = date_map.get(d[5:7], d[5:7])
                day = str(int(d[8:10]))
                weekly.append({
                    "date": f"{month} {day}",
                    "date_iso": d,
                    "mention_rate": r.get("mention_rate", 0),
                    "share_of_voice": r.get("share_of_voice", 0),
                    "citation_rate": r.get("citation_rate", 0),
                    "citation_count": r.get("citation_count", 0),
                    "sentiment": r.get("sentiment_score", 0),
                    "answer_count": r.get("answer_count", 0),
                    "first_mention": r.get("first_mention_rate", 0),
                })
            data["weekly_trends"] = weekly
            print(f"AirOps: Updated {len(weekly)} weekly trend rows")
    else:
        print(f"AirOps analytics error: {resp.status_code} {resp.text[:200]}")

    # Query pages
    pages_url = f"{base}/brand_kits/{brand_kit_id}/pages"
    params = {
        "sort": "-citations_count",
        "per_page": 50,
        "filters[url][contains]": "blog",
        "fields": "url,primary_keyword,citations_count,citations_count_diff,clicks,impressions,ctr,position,traffic,sessions,engagement",
    }
    resp = requests.get(pages_url, params=params, headers=headers, timeout=30)
    if resp.status_code == 200:
        pages = resp.json().get("data", [])
        if pages:
            fresh = []
            for p in pages:
                fresh.append({
                    "url": p.get("url", "").replace("https://", ""),
                    "keyword": p.get("primary_keyword", ""),
                    "citations_count": p.get("citations_count", 0),
                    "citations_diff": round((p.get("citations_count_diff", 0) or 0) * 100, 1),
                    "clicks": p.get("clicks"),
                    "impressions": p.get("impressions"),
                    "ctr": p.get("ctr"),
                    "position": p.get("position"),
                    "traffic": p.get("traffic"),
                    "sessions": p.get("sessions"),
                    "engagement": p.get("engagement"),
                    "signups": 0,
                    "signup_rate": 0,
                })
            data["pages"] = fresh
            if "pages_by_period" in data:
                data["pages_by_period"]["4w"] = fresh
                data["pages_by_period"]["all"] = fresh
            print(f"AirOps: Updated {len(fresh)} blog pages")
    else:
        print(f"AirOps pages error: {resp.status_code} {resp.text[:200]}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    print(f"=== Dashboard Data Refresh — {datetime.datetime.now(datetime.timezone.utc).isoformat()} ===")
    data = load_data()

    refresh_airops(data)
    refresh_ga4(data)
    refresh_amplitude(data)

    save_data(data)
    print("=== Refresh complete ===")


if __name__ == "__main__":
    main()
