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

# Metaのみ使う
ENABLE_META = True


def main():
    print("=== Start Meta Export ===")
    config = load_secret()
    mask_sensitive_values(config)

    resolved = resolve_config(config)
    validate_config(resolved)

    month_ranges = get_target_month_ranges(month_count=6)
    oldest_since = month_ranges[0]["since"]
    until = month_ranges[-1]["until"]

    print(
        "Target months: "
        + ", ".join(
            [f"{r['label']}({r['since']} to {r['until']})" for r in month_ranges]
        )
    )

    rows = fetch_meta_rows(
        act_id=resolved["meta"]["account_id"],
        token=resolved["meta"]["token"],
        month_ranges=month_ranges,
        daily_since=oldest_since,
        daily_until=until,
    )
    print(f"Meta rows built: {len(rows)}")

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
        "sheet.spreadsheet_id": resolved["sheet"]["spreadsheet_id"],
        "sheet.google_service_account": resolved["sheet"]["google_service_account"],
    }

    if ENABLE_META:
        required.update({
            "meta.token": resolved["meta"]["token"],
            "meta.account_id": resolved["meta"]["account_id"],
        })

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


def get_target_month_ranges(month_count=6):
    """
    起動日の当月1日〜前日を当月とし、過去6か月分を返す
    例: 2026-04-17起動 -> 2025-11, 2025-12, 2026-01, 2026-02, 2026-03, 2026-04(1日〜16日)
    """
    today_jst = datetime.now(JST).date()
    yesterday = today_jst - timedelta(days=1)

    if yesterday.month == today_jst.month:
        current_month_start = date(today_jst.year, today_jst.month, 1)
    else:
        current_month_start = date(yesterday.year, yesterday.month, 1)

    ranges = []
    for i in range(month_count - 1, -1, -1):
        month_start = shift_month_start(current_month_start, -i)
        if month_start.year == yesterday.year and month_start.month == yesterday.month:
            month_end = yesterday
        else:
            month_end = end_of_month(month_start)

        ranges.append({
            "label": month_start.strftime("%Y-%m"),
            "since": month_start,
            "until": month_end,
        })

    return ranges


def shift_month_start(base_month_start, month_delta):
    y = base_month_start.year
    m = base_month_start.month + month_delta

    while m <= 0:
        y -= 1
        m += 12
    while m > 12:
        y += 1
        m -= 12

    return date(y, m, 1)


def end_of_month(d):
    if d.month == 12:
        return date(d.year, 12, 31)
    return date(d.year, d.month + 1, 1) - timedelta(days=1)


def make_output_row(
    media="meta",
    scope="",
    month="",
    day="",
    campaign_name="",
    adset_name="",
    ad_name="",
    gender="",
    age="",
    platform="",
    impressions=0,
    link_clicks=0,
    amount_spent=0,
    instagram_profile_visits=0,
    instagram_follows=0,
):
    return [
        media,
        scope,
        month,
        day,
        campaign_name or "",
        adset_name or "",
        ad_name or "",
        gender or "",
        age or "",
        platform or "",
        to_int(impressions),
        to_int(link_clicks),
        to_float(amount_spent) * 1.25,
        to_int(instagram_profile_visits),
        to_int(instagram_follows),
    ]


def fetch_meta_rows(act_id, token, month_ranges, daily_since, daily_until):
    normalized_act_id = normalize_meta_act_id(act_id)
    rows = []

    common_fields = [
        "campaign_name",
        "adset_name",
        "impressions",
        "inline_link_clicks",
        "spend",
        "instagram_profile_visits",
        "follows",
    ]

    ad_day_fields = [
        "campaign_name",
        "adset_name",
        "ad_name",
        "impressions",
        "inline_link_clicks",
        "spend",
        "instagram_profile_visits",
        "follows",
    ]

    # campaign：キャンペーン別×月別
    for month_range in month_ranges:
        batch = fetch_meta_insights(
            act_id=normalized_act_id,
            token=token,
            since=month_range["since"],
            until=month_range["until"],
            time_increment="monthly",
            level="campaign",
            fields=common_fields,
        )
        for item in batch:
            rows.append(
                make_output_row(
                    media="meta",
                    scope="campaign",
                    month=month_range["label"],
                    day="",
                    campaign_name=item.get("campaign_name", ""),
                    impressions=item.get("impressions"),
                    link_clicks=item.get("inline_link_clicks"),
                    amount_spent=item.get("spend"),
                    instagram_profile_visits=item.get("instagram_profile_visits"),
                    instagram_follows=item.get("follows"),
                )
            )

    # ad_day：広告別×日別
    batch = fetch_meta_insights(
        act_id=normalized_act_id,
        token=token,
        since=daily_since,
        until=daily_until,
        time_increment="1",
        level="ad",
        fields=ad_day_fields,
    )
    for item in batch:
        rows.append(
            make_output_row(
                media="meta",
                scope="ad_day",
                month=to_month(item.get("date_start")),
                day=item.get("date_start", ""),
                campaign_name=item.get("campaign_name", ""),
                adset_name=item.get("adset_name", ""),
                ad_name=item.get("ad_name", ""),
                impressions=item.get("impressions"),
                link_clicks=item.get("inline_link_clicks"),
                amount_spent=item.get("spend"),
                instagram_profile_visits=item.get("instagram_profile_visits"),
                instagram_follows=item.get("follows"),
            )
        )

    # adset_gen：広告セット別×性別
    for month_range in month_ranges:
        batch = fetch_meta_insights(
            act_id=normalized_act_id,
            token=token,
            since=month_range["since"],
            until=month_range["until"],
            time_increment="monthly",
            level="adset",
            fields=common_fields,
            breakdowns=["gender"],
        )
        for item in batch:
            rows.append(
                make_output_row(
                    media="meta",
                    scope="adset_gen",
                    month=month_range["label"],
                    day="",
                    campaign_name=item.get("campaign_name", ""),
                    adset_name=item.get("adset_name", ""),
                    gender=item.get("gender", ""),
                    impressions=item.get("impressions"),
                    link_clicks=item.get("inline_link_clicks"),
                    amount_spent=item.get("spend"),
                    instagram_profile_visits=item.get("instagram_profile_visits"),
                    instagram_follows=item.get("follows"),
                )
            )

    # adset_age：広告セット別×年齢別
    for month_range in month_ranges:
        batch = fetch_meta_insights(
            act_id=normalized_act_id,
            token=token,
            since=month_range["since"],
            until=month_range["until"],
            time_increment="monthly",
            level="adset",
            fields=common_fields,
            breakdowns=["age"],
        )
        for item in batch:
            rows.append(
                make_output_row(
                    media="meta",
                    scope="adset_age",
                    month=month_range["label"],
                    day="",
                    campaign_name=item.get("campaign_name", ""),
                    adset_name=item.get("adset_name", ""),
                    age=item.get("age", ""),
                    impressions=item.get("impressions"),
                    link_clicks=item.get("inline_link_clicks"),
                    amount_spent=item.get("spend"),
                    instagram_profile_visits=item.get("instagram_profile_visits"),
                    instagram_follows=item.get("follows"),
                )
            )

    # adset_pf：広告セット別×プラットフォーム別
    for month_range in month_ranges:
        batch = fetch_meta_insights(
            act_id=normalized_act_id,
            token=token,
            since=month_range["since"],
            until=month_range["until"],
            time_increment="monthly",
            level="adset",
            fields=common_fields,
            breakdowns=["publisher_platform"],
        )
        for item in batch:
            rows.append(
                make_output_row(
                    media="meta",
                    scope="adset_pf",
                    month=month_range["label"],
                    day="",
                    campaign_name=item.get("campaign_name", ""),
                    adset_name=item.get("adset_name", ""),
                    platform=item.get("publisher_platform", ""),
                    impressions=item.get("impressions"),
                    link_clicks=item.get("inline_link_clicks"),
                    amount_spent=item.get("spend"),
                    instagram_profile_visits=item.get("instagram_profile_visits"),
                    instagram_follows=item.get("follows"),
                )
            )

    return rows


def fetch_meta_insights(act_id, token, since, until, time_increment, level, fields, breakdowns=None):
    url = f"https://graph.facebook.com/{META_API_VERSION}/{act_id}/insights"
    params = {
        "access_token": token,
        "level": level,
        "time_range": json.dumps(
            {
                "since": since.strftime("%Y-%m-%d"),
                "until": until.strftime("%Y-%m-%d"),
            }
        ),
        "fields": ",".join(fields),
        "time_increment": time_increment,
        "limit": 5000,
    }

    if breakdowns:
        params["breakdowns"] = ",".join(breakdowns)

    all_rows = []

    while True:
        response = requests.get(url, params=params, timeout=120)
        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            raise RuntimeError(
                f"Meta API request failed. status={response.status_code}, body={truncate_text(response.text)}"
            ) from e

        data = response.json()
        if "error" in data:
            raise RuntimeError(
                f"Meta API error: {json.dumps(data['error'], ensure_ascii=False)}"
            )

        batch = data.get("data", [])
        all_rows.extend(batch)

        next_url = data.get("paging", {}).get("next")
        if not next_url:
            break

        url = next_url
        params = None

    return all_rows


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
        "platform",
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
        worksheet.update("A1", output)
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
        media = row[0]
        scope = row[1]
        month = row[2]
        day = row[3]
        campaign_name = row[4]
        adset_name = row[5]
        ad_name = row[6]
        gender = row[7]
        age = row[8]
        platform = row[9]

        day_sort = int(day.replace("-", "")) if day else 0
        month_sort = int(month.replace("-", "")) if month else 0

        return (
            media,
            scope_order.get(scope, 999),
            month_sort,
            day_sort,
            campaign_name,
            adset_name,
            ad_name,
            gender,
            age,
            platform,
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
