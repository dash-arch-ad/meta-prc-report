import os
import json
import requests
import gspread
from zoneinfo import ZoneInfo
from datetime import datetime, date, timedelta
from oauth2client.service_account import ServiceAccountCredentials

META_API_VERSION = "v25.0"
JST = ZoneInfo("Asia/Tokyo")
DEFAULT_WORKSHEET_NAME = "gitreport"

# デバッグログ
DEBUG_MODE = True


def main():
    print("=== Start Meta Export ===")

    config = load_secret()
    mask_sensitive_values(config)

    resolved = resolve_config(config)
    validate_config(resolved)

    month_ranges, daily_since, daily_until = get_target_date_ranges()
    print_target_ranges(month_ranges, daily_since, daily_until)

    rows = []

    campaign_rows = fetch_campaign_monthly_rows(
        act_id=resolved["meta"]["account_id"],
        token=resolved["meta"]["token"],
        month_ranges=month_ranges,
    )
    rows += campaign_rows
    print(f"campaign rows: {len(campaign_rows)}")

    ad_day_rows = fetch_ad_day_rows(
        act_id=resolved["meta"]["account_id"],
        token=resolved["meta"]["token"],
        since=daily_since,
        until=daily_until,
    )
    rows += ad_day_rows
    print(f"ad_day rows: {len(ad_day_rows)}")

    adset_gen_rows = fetch_adset_breakdown_rows(
        act_id=resolved["meta"]["account_id"],
        token=resolved["meta"]["token"],
        month_ranges=month_ranges,
        breakdown="gender",
        scope_name="adset_gen",
    )
    rows += adset_gen_rows
    print(f"adset_gen rows: {len(adset_gen_rows)}")

    adset_age_rows = fetch_adset_breakdown_rows(
        act_id=resolved["meta"]["account_id"],
        token=resolved["meta"]["token"],
        month_ranges=month_ranges,
        breakdown="age",
        scope_name="adset_age",
    )
    rows += adset_age_rows
    print(f"adset_age rows: {len(adset_age_rows)}")

    adset_pf_rows = fetch_adset_breakdown_rows(
        act_id=resolved["meta"]["account_id"],
        token=resolved["meta"]["token"],
        month_ranges=month_ranges,
        breakdown="publisher_platform",
        scope_name="adset_pf",
    )
    rows += adset_pf_rows
    print(f"adset_pf rows: {len(adset_pf_rows)}")

    rows = sort_rows(rows)

    spreadsheet = connect_spreadsheet(
        sheet_id=resolved["sheet"]["spreadsheet_id"],
        google_creds_dict=resolved["sheet"]["google_service_account"],
    )

    write_to_sheet(
        spreadsheet=spreadsheet,
        sheet_name=resolved["sheet"]["worksheet_name"],
        rows=rows,
    )

    print(f"Total rows written: {len(rows)}")
    print("=== Completed ===")


def load_secret():
    secret_env = os.environ.get("APP_SECRET_JSON")
    if not secret_env:
        raise RuntimeError("APP_SECRET_JSON is not set")

    try:
        return json.loads(secret_env)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"APP_SECRET_JSON is invalid JSON: {e}") from e


def mask_sensitive_values(config):
    candidates = []

    def push(value):
        if value is None:
            return
        value = str(value).strip()
        if not value:
            return
        if "\n" in value:
            return
        candidates.append(value)

    meta = config.get("meta", {})
    push(meta.get("token"))
    push(meta.get("account_id"))
    push(config.get("m_token"))
    push(config.get("m_act_id"))

    for value in sorted(set(candidates)):
        print(f"::add-mask::{value}")


def resolve_config(config):
    meta_conf = config.get("meta", {})
    sheets_conf = config.get("sheets", {})

    spreadsheet_id = sheets_conf.get("spreadsheet_id")
    if not spreadsheet_id:
        legacy_sheet_id = config.get("s_id")
        if isinstance(legacy_sheet_id, list):
            spreadsheet_id = legacy_sheet_id[0] if legacy_sheet_id else None
        else:
            spreadsheet_id = legacy_sheet_id

    worksheet_name = sheets_conf.get("worksheet_name") or DEFAULT_WORKSHEET_NAME

    google_service_account = config.get("gcp_service_account") or config.get("g_creds")
    google_service_account = normalize_google_service_account(google_service_account)

    return {
        "meta": {
            "token": meta_conf.get("token") or config.get("m_token"),
            "account_id": meta_conf.get("account_id") or config.get("m_act_id"),
        },
        "sheet": {
            "spreadsheet_id": spreadsheet_id,
            "worksheet_name": worksheet_name,
            "google_service_account": google_service_account,
        },
    }


def validate_config(resolved):
    required = {
        "meta.token": resolved["meta"]["token"],
        "meta.account_id": resolved["meta"]["account_id"],
        "sheet.spreadsheet_id": resolved["sheet"]["spreadsheet_id"],
        "sheet.google_service_account": resolved["sheet"]["google_service_account"],
    }

    missing = [k for k, v in required.items() if not v]
    if missing:
        raise RuntimeError(f"Missing required config keys: {', '.join(missing)}")


def normalize_google_service_account(creds):
    if not creds:
        return None

    fixed = dict(creds)
    private_key = fixed.get("private_key", "")
    if private_key:
        fixed["private_key"] = private_key.replace("\\n", "\n")
    return fixed


def normalize_meta_act_id(raw_act_id):
    cleaned = (
        str(raw_act_id)
        .replace("act=", "")
        .replace("act_", "")
        .replace("act", "")
        .strip()
    )
    return f"act_{cleaned}"


def get_target_date_ranges():
    today_jst = datetime.now(JST).date()
    yesterday = today_jst - timedelta(days=1)

    # 当月1日
    current_month_start = date(today_jst.year, today_jst.month, 1)

    month_ranges = []

    # 当月（1日〜前日）
    if yesterday >= current_month_start:
        month_ranges.append({
            "label": current_month_start.strftime("%Y-%m"),
            "since": current_month_start,
            "until": yesterday,
        })

    # 過去5ヶ月分
    cursor = current_month_start - timedelta(days=1)
    for _ in range(5):
        month_start = date(cursor.year, cursor.month, 1)
        next_month_start = (
            date(cursor.year + 1, 1, 1)
            if cursor.month == 12
            else date(cursor.year, cursor.month + 1, 1)
        )
        month_end = next_month_start - timedelta(days=1)

        month_ranges.append({
            "label": month_start.strftime("%Y-%m"),
            "since": month_start,
            "until": month_end,
        })

        cursor = month_start - timedelta(days=1)

    month_ranges = sorted(month_ranges, key=lambda x: x["since"])

    daily_since = month_ranges[0]["since"]
    daily_until = yesterday

    return month_ranges, daily_since, daily_until


def print_target_ranges(month_ranges, daily_since, daily_until):
    text = ", ".join(
        [f"{r['label']}({r['since']} to {r['until']})" for r in month_ranges]
    )
    print(f"Target monthly ranges: {text}")
    print(f"Target daily range: {daily_since} to {daily_until}")


def fetch_campaign_monthly_rows(act_id, token, month_ranges):
    rows = []
    normalized_act_id = normalize_meta_act_id(act_id)

    for month_range in month_ranges:
        items = fetch_meta_insights(
            act_id=normalized_act_id,
            token=token,
            since=month_range["since"],
            until=month_range["until"],
            level="campaign",
            fields=[
                "campaign_name",
                "impressions",
                "inline_link_clicks",
                "spend",
                "instagram_profile_visits",
            ],
            time_increment="monthly",
        )

        debug_metric_samples(
            items=items,
            label=f"campaign {month_range['label']}",
            limit=5,
        )

        for item in items:
            metrics = extract_common_metrics(item)

            rows.append(make_output_row(
                media="meta",
                scope="campaign",
                month=month_range["label"],
                day="",
                campaign_name=item.get("campaign_name", ""),
                adset_name="",
                ad_name="",
                gender="",
                age="",
                publisher_platform="",
                impressions=metrics["impressions"],
                link_clicks=metrics["link_clicks"],
                amount_spent=metrics["amount_spent"],
                instagram_profile_visits=metrics["instagram_profile_visits"],
                instagram_follows=metrics["instagram_follows"],
            ))

    return rows


def fetch_ad_day_rows(act_id, token, since, until):
    rows = []
    normalized_act_id = normalize_meta_act_id(act_id)

    items = fetch_meta_insights(
        act_id=normalized_act_id,
        token=token,
        since=since,
        until=until,
        level="ad",
        fields=[
            "campaign_name",
            "adset_name",
            "ad_name",
            "impressions",
            "inline_link_clicks",
            "spend",
            "instagram_profile_visits",
        ],
        time_increment="1",
    )

    debug_metric_samples(
        items=items,
        label=f"ad_day {since} to {until}",
        limit=10,
    )

    for item in items:
        metrics = extract_common_metrics(item)
        day = item.get("date_start", "")

        rows.append(make_output_row(
            media="meta",
            scope="ad_day",
            month=to_month(day),
            day=day,
            campaign_name=item.get("campaign_name", ""),
            adset_name=item.get("adset_name", ""),
            ad_name=item.get("ad_name", ""),
            gender="",
            age="",
            publisher_platform="",
            impressions=metrics["impressions"],
            link_clicks=metrics["link_clicks"],
            amount_spent=metrics["amount_spent"],
            instagram_profile_visits=metrics["instagram_profile_visits"],
            instagram_follows=metrics["instagram_follows"],
        ))

    return rows


def fetch_adset_breakdown_rows(act_id, token, month_ranges, breakdown, scope_name):
    rows = []
    normalized_act_id = normalize_meta_act_id(act_id)

    for month_range in month_ranges:
        items = fetch_meta_insights(
            act_id=normalized_act_id,
            token=token,
            since=month_range["since"],
            until=month_range["until"],
            level="adset",
            fields=[
                "campaign_name",
                "adset_name",
                "impressions",
                "inline_link_clicks",
                "spend",
                "instagram_profile_visits",
            ],
            time_increment="monthly",
            breakdowns=[breakdown],
        )

        debug_metric_samples(
            items=items,
            label=f"{scope_name} {month_range['label']}",
            limit=5,
        )

        for item in items:
            metrics = extract_common_metrics(item)

            gender = item.get("gender", "") if breakdown == "gender" else ""
            age = item.get("age", "") if breakdown == "age" else ""
            publisher_platform = item.get("publisher_platform", "") if breakdown == "publisher_platform" else ""

            rows.append(make_output_row(
                media="meta",
                scope=scope_name,
                month=month_range["label"],
                day="",
                campaign_name=item.get("campaign_name", ""),
                adset_name=item.get("adset_name", ""),
                ad_name="",
                gender=gender,
                age=age,
                publisher_platform=publisher_platform,
                impressions=metrics["impressions"],
                link_clicks=metrics["link_clicks"],
                amount_spent=metrics["amount_spent"],
                instagram_profile_visits=metrics["instagram_profile_visits"],
                instagram_follows=metrics["instagram_follows"],
            ))

    return rows


def fetch_meta_insights(act_id, token, since, until, level, fields, time_increment, breakdowns=None):
    url = f"https://graph.facebook.com/{META_API_VERSION}/{act_id}/insights"

    params = {
        "access_token": token,
        "level": level,
        "time_range": json.dumps({
            "since": since.strftime("%Y-%m-%d"),
            "until": until.strftime("%Y-%m-%d"),
        }),
        "fields": ",".join(fields),
        "time_increment": time_increment,
        "limit": 5000,
    }

    if breakdowns:
        params["breakdowns"] = ",".join(breakdowns)

    all_rows = []

    while True:
        response = requests.get(url, params=params, timeout=180)

        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            raise RuntimeError(
                f"Meta API request failed. status={response.status_code}, body={truncate_text(response.text)}"
            ) from e

        payload = response.json()

        if "error" in payload:
            raise RuntimeError(
                f"Meta API error: {json.dumps(payload['error'], ensure_ascii=False)}"
            )

        batch = payload.get("data", [])
        all_rows.extend(batch)

        next_url = payload.get("paging", {}).get("next")
        if not next_url:
            break

        url = next_url
        params = None

    return all_rows


def extract_common_metrics(item):
    impressions = to_int(item.get("impressions"))
    link_clicks = to_int(item.get("inline_link_clicks"))
    amount_spent = round(to_float(item.get("spend")) * 1.25, 2)
    instagram_profile_visits = to_int(item.get("instagram_profile_visits"))

    # Ads Insights で安定取得できる公式メトリクスが確認できないため空欄運用
    instagram_follows = ""

    return {
        "impressions": impressions,
        "link_clicks": link_clicks,
        "amount_spent": amount_spent,
        "instagram_profile_visits": instagram_profile_visits,
        "instagram_follows": instagram_follows,
    }


def debug_metric_samples(items, label="", limit=5):
    if not DEBUG_MODE:
        return

    print(f"==== DEBUG metrics start: {label} ====")

    for i, item in enumerate(items[:limit], start=1):
        print(f"---- sample {i} ----")
        print(json.dumps({
            "campaign_name": item.get("campaign_name", ""),
            "adset_name": item.get("adset_name", ""),
            "ad_name": item.get("ad_name", ""),
            "date_start": item.get("date_start", ""),
            "date_stop": item.get("date_stop", ""),
            "gender": item.get("gender", ""),
            "age": item.get("age", ""),
            "publisher_platform": item.get("publisher_platform", ""),
            "impressions": item.get("impressions"),
            "inline_link_clicks": item.get("inline_link_clicks"),
            "spend": item.get("spend"),
            "instagram_profile_visits": item.get("instagram_profile_visits"),
        }, ensure_ascii=False))

    print(f"==== DEBUG metrics end: {label} ====")


def make_output_row(
    media,
    scope,
    month,
    day,
    campaign_name,
    adset_name,
    ad_name,
    gender,
    age,
    publisher_platform,
    impressions,
    link_clicks,
    amount_spent,
    instagram_profile_visits,
    instagram_follows,
):
    return [
        media,
        scope,
        month,
        day,
        campaign_name,
        adset_name,
        ad_name,
        gender,
        age,
        publisher_platform,
        impressions,
        link_clicks,
        amount_spent,
        instagram_profile_visits,
        instagram_follows,
    ]


def connect_spreadsheet(sheet_id, google_creds_dict):
    try:
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_dict(
            google_creds_dict, scope
        )
        client = gspread.authorize(creds)
        spreadsheet = client.open_by_key(sheet_id)
        print("Google Sheets connected successfully")
        return spreadsheet
    except Exception as e:
        raise RuntimeError(f"Google Sheets connection error: {repr(e)}") from e


def write_to_sheet(spreadsheet, sheet_name, rows):
    header = [[
        "media",
        "scope",
        "month",
        "day",
        "campaign_name",
        "adset_name",
        "ad_name",
        "gender",
        "age",
        "publisher_platform",
        "impressions",
        "link_clicks",
        "amount_spent",
        "instagram_profile_visits",
        "instagram_follows",
    ]]

    try:
        try:
            worksheet = spreadsheet.worksheet(sheet_name)
        except gspread.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=1000, cols=15)

        worksheet.clear()
        output = header + rows
        worksheet.update("A1", output, value_input_option="USER_ENTERED")
        print(f"Write success: {sheet_name} ({len(rows)} rows)")
    except Exception as e:
        raise RuntimeError(f"Write error ({sheet_name}): {repr(e)}") from e


def sort_rows(rows):
    scope_order = {
        "campaign": 0,
        "ad_day": 1,
        "adset_gen": 2,
        "adset_age": 3,
        "adset_pf": 4,
    }

    def sort_key(row):
        return (
            row[0],   # media
            scope_order.get(row[1], 999),
            row[2],   # month
            row[3],   # day
            row[4],   # campaign_name
            row[5],   # adset_name
            row[6],   # ad_name
            row[7],   # gender
            row[8],   # age
            row[9],   # publisher_platform
        )

    return sorted(rows, key=sort_key)


def to_month(value):
    if not value:
        return ""
    return str(value)[:7]


def to_int(value):
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def truncate_text(value, limit=800):
    value = str(value)
    if len(value) <= limit:
        return value
    return value[:limit] + "...(truncated)"


if __name__ == "__main__":
    main()
