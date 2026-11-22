import hashlib
import logging
import math
import os
import re
import secrets
import sys
import time
import uuid
from urllib.parse import parse_qs, urlsplit
import json
import requests
import gspread


from zoneinfo import ZoneInfo
from datetime import datetime, date, timedelta
from oauth2client.service_account import ServiceAccountCredentials

META_API_VERSION = "v27.0"
JST = ZoneInfo("Asia/Tokyo")
DEFAULT_WORKSHEET_NAME = "gitreport"

class ExportError(Exception):
    """Safe for logs. Messages contain only fixed labels and validated numbers."""


def main():
    print("=== Start Meta Export ===")
    config = load_secret()

    resolved = resolve_config(config)
    validate_config(resolved)

    dest = resolved["sheet"]
    book = connect_spreadsheet(dest["spreadsheet_id"], dest["google_service_account"])
    lease = acquire_lease(book, dest["worksheet_name"])
    if lease is None:
        print("Export skipped: another run owns the destination.")
        return
    try:
        rows = collect_rows(resolved)
        write_to_sheet(book, dest["worksheet_name"], rows, lease)
    except BaseException:
        release_lease(book, lease)
        raise
    print(f"Total rows written: {len(rows)}")
    print("=== Completed ===")


def collect_rows(resolved):
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

    return sort_rows(rows)


def load_secret():
    secret_env = os.environ.get("APP_SECRET_JSON")
    if not secret_env:
        raise ExportError("APP_SECRET_JSON is not set")

    try:
        return json.loads(secret_env)
    except json.JSONDecodeError:
        raise ExportError("APP_SECRET_JSON is invalid JSON.") from None


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
        raise ExportError(f"Missing required config keys: {', '.join(missing)}")


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
    if not re.fullmatch(r"[0-9]+", cleaned):
        raise ExportError("Invalid Meta account ID.")
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


def api_json(service, method, url, **kwargs):
    try:
        response = requests.request(method, url, timeout=180, allow_redirects=False, **kwargs)
    except requests.RequestException:
        raise ExportError(f"{service} connection failed.") from None
    status = response.status_code
    try:
        data = response.json()
    except ValueError:
        raise ExportError(f"{service} invalid JSON (HTTP {status}).") from None
    has_error = isinstance(data, dict) and "error" in data
    if not 200 <= status < 300 or has_error:
        error = data.get("error") if has_error else None
        details = ""
        if isinstance(error, dict):
            for key in ("code", "error_subcode"):
                if type(error.get(key)) is int:
                    details += f", {key}={error[key]}"
        raise ExportError(f"{service} failed (HTTP {status}{details}).")
    return data


def valid_count(value):
    if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        return False
    try:
        number = float(value)
        return math.isfinite(number) and number >= 0
    except (ValueError, OverflowError):
        return False


def validate_meta_row(row, fields, breakdowns, since, until, daily):
    if not isinstance(row, dict):
        raise ExportError("Meta returned an invalid report row.")
    for field in ("impressions", "spend"):
        if not valid_count(row.get(field)):
            raise ExportError("Meta returned an invalid delivery metric.")
    # Meta may omit events with no activity. Keep the original zero on omission.
    for field in ("inline_link_clicks", "instagram_profile_visits"):
        if field in row and not valid_count(row[field]):
            raise ExportError("Meta returned an invalid event metric.")
    for field in (*fields, *breakdowns):
        if field.endswith("_name") or field in breakdowns:
            if not isinstance(row.get(field), str) or (field in breakdowns and not row[field]):
                raise ExportError("Meta returned an invalid report dimension.")
    if daily:
        try:
            day = date.fromisoformat(row["date_start"])
            if not since <= day <= until or day.isoformat() != row["date_start"]:
                raise ValueError
        except (KeyError, TypeError, ValueError):
            raise ExportError("Meta returned an invalid date.") from None


def next_cursor(data, endpoint):
    paging = data.get("paging", {})
    if not isinstance(paging, dict):
        raise ExportError("Meta returned invalid paging.")
    if "next" not in paging:
        return None
    link = paging["next"]
    if not isinstance(link, str):
        raise ExportError("Meta returned an invalid next page.")
    parts = urlsplit(link)
    if (parts.scheme != "https" or parts.netloc != "graph.facebook.com"
            or parts.path != urlsplit(endpoint).path or parts.fragment):
        raise ExportError("Meta returned an unexpected next page.")
    query = parse_qs(parts.query, keep_blank_values=True)
    after = query.get("after", [])
    if len(after) != 1 or not after[0] or any(k in query for k in ("before", "offset", "__paging_token")):
        raise ExportError("Meta returned unsupported pagination.")
    cursors = paging.get("cursors", {})
    if not isinstance(cursors, dict) or cursors.get("after", after[0]) != after[0]:
        raise ExportError("Meta returned inconsistent cursors.")
    return after[0]


def fetch_meta_insights(act_id, token, since, until, level, fields, time_increment, breakdowns=None):
    url = f"https://graph.facebook.com/{META_API_VERSION}/{act_id}/insights"
    params = {
        "level": level,
        "time_range": json.dumps({"since": since.isoformat(), "until": until.isoformat()}),
        "fields": ",".join(fields),
        "time_increment": time_increment,
        "limit": 5000,
    }
    if breakdowns:
        params["breakdowns"] = ",".join(breakdowns)
    headers = {"Authorization": f"Bearer {token}"}
    all_rows = []
    seen = set()
    while True:
        data = api_json("Meta API", "GET", url, headers=headers, params=params)
        if not isinstance(data, dict) or not isinstance(data.get("data"), list):
            raise ExportError("Meta returned an invalid data array.")
        for row in data["data"]:
            validate_meta_row(row, fields, breakdowns or (), since, until, time_increment == "1")
            all_rows.append(row)
        after = next_cursor(data, url)
        if after is None:
            return all_rows
        if after in seen:
            raise ExportError("Meta pagination repeated a cursor.")
        seen.add(after)
        params["after"] = after


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
    except Exception:
        raise ExportError("Google Sheets connection failed.") from None


HEADER = [
    'media', 'scope', 'month', 'day', 'campaign_name', 'adset_name', 'ad_name',
    'gender', 'age', 'publisher_platform', 'impressions', 'link_clicks',
    'amount_spent', 'instagram_profile_visits', 'instagram_follows',
]


LEASE_SECONDS = 3600


def sheet_state(book):
    return book.fetch_sheet_metadata(params={
        "fields": "sheets(properties(sheetId,title,sheetType,gridProperties(rowCount,columnCount))),"
                  "namedRanges(namedRangeId,name)"
    })


def range_names(state):
    return {item["name"]: item["namedRangeId"] for item in state.get("namedRanges", [])}


def delete_range(range_id):
    return {"deleteNamedRange": {"namedRangeId": range_id}}


def add_range(name, range_id, sheet_id):
    return {"addNamedRange": {"namedRange": {
        "name": name, "namedRangeId": range_id,
        "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1,
                  "startColumnIndex": 0, "endColumnIndex": 1},
    }}}


def lease_time(range_id):
    match = re.fullmatch(r"prc_([0-9]+)_[0-9a-f]{32}", range_id)
    if not match:
        raise ExportError("Unexpected Sheets lock. Check named ranges.")
    return int(match.group(1))


def acquire_lease(book, title):
    # All runners compete for one unique name. Each acquisition uses a fresh ID.
    key = hashlib.sha256(title.casefold().encode()).hexdigest()[:24]
    lock = "prc_lock_" + key
    state = sheet_state(book)
    existing = range_names(state).get(lock)
    now = int(time.time())
    if existing and now < lease_time(existing) + LEASE_SECONDS:
        return None
    token = f"prc_{now}_{uuid.uuid4().hex}"
    anchor = next(s["properties"]["sheetId"] for s in state["sheets"]
                  if s["properties"].get("sheetType", "GRID") == "GRID"
                  and not re.fullmatch(r"prc_[0-9]+_[0-9a-f]{32}", s["properties"]["title"]))
    lease = {"id": token, "name": lock, "receipt_prefix": "prc_done_" + key + "_",
             "anchor": anchor}
    # A temporary hidden tab lets Sheets apply its actual USER_ENTERED parser.
    stage_id = secrets.randbelow(2**31 - 1) + 1
    lease["stage_id"] = stage_id
    ops = cleanup_requests(state, existing) if existing else []
    ops.append({"addSheet": {"properties": {"sheetId": stage_id, "title": token,
                "hidden": True, "gridProperties": {"rowCount": 1, "columnCount": 15}}}})
    ops.append(add_range(lock, token, anchor))
    try:
        book.batch_update({"requests": ops})
    except Exception:
        owner = range_names(sheet_state(book)).get(lock)
        if owner == token:
            return lease
        if owner and owner != existing:
            return None
        raise ExportError("Sheets lock acquisition failed.") from None
    return lease


def cleanup_requests(state, token):
    ops = [delete_range(token)]
    for sheet in state["sheets"]:
        if sheet["properties"]["title"] == token:
            ops.append({"deleteSheet": {"sheetId": sheet["properties"]["sheetId"]}})
    return ops


def release_lease(book, lease):
    # IDs are never reused. A delayed cleanup cannot unlock the successor.
    try:
        state = sheet_state(book)
        if range_names(state).get(lease["name"]) == lease["id"]:
            book.batch_update({"requests": cleanup_requests(state, lease["id"])})
    except Exception:
        print("Sheets lock cleanup unconfirmed. Expiry permits recovery.", file=sys.stderr)


def previous_report_height(book, title, new_height):
    escaped = title.replace("'", "''")
    result = book.values_get(f"'{escaped}'!A:O", params={"valueRenderOption": "FORMULA"})
    values = result.get("values", [])
    height = 0
    if values and values[0] == HEADER:
        height = 1
        for row in values[1:]:
            if len(row) < 3 or row[0] != "meta":
                break
            if row[1] not in {"campaign", "ad_day", "adset_gen", "adset_age", "adset_pf"}:
                break
            height += 1
    if any(any(cell != "" for cell in row) for row in values[height:new_height]):
        raise ExportError("Sheets output overlaps existing non-report data.")
    return height


def stage_values(book, lease, output, target):
    stage = lease["stage_id"]
    stage_title = lease["id"]
    width = len(HEADER)
    ops = [{"updateSheetProperties": {"properties": {
        "sheetId": stage, "gridProperties": {"rowCount": len(output)}},
        "fields": "gridProperties.rowCount"}}]
    if target:
        grid = target["gridProperties"]
        bounds = {"startRowIndex": 0, "endRowIndex": min(len(output), grid["rowCount"]),
                  "startColumnIndex": 0, "endColumnIndex": min(width, grid["columnCount"])}
        ops.append({"copyPaste": {"source": dict(bounds, sheetId=target["sheetId"]),
                                 "destination": dict(bounds, sheetId=stage),
                                 "pasteType": "PASTE_FORMAT"}})
    book.batch_update({"requests": ops})
    # Use the same workbook locale, cell formats and A1 coordinates as the final tab.
    area = f"'{stage_title}'!A1:O{len(output)}"
    book.values_update(area, params={"valueInputOption": "USER_ENTERED"}, body={"values": output})
    state = book.fetch_sheet_metadata(params={
        "ranges": area, "fields": "sheets(properties(sheetId),data(rowData(values("
                                 "userEnteredValue,userEnteredFormat(numberFormat)))))"})
    staged = state["sheets"][0]
    cells = staged["data"][0]["rowData"]
    if staged["properties"]["sheetId"] != stage or len(cells) != len(output):
        raise ExportError("Sheets returned incomplete staged values.")
    # The guaranteed first column distinguishes a parsed report row from missing grid data.
    for index, row in enumerate(cells):
        first = row["values"][0].get("userEnteredValue", {})
        if first.get("stringValue") != output[index][0]:
            raise ExportError("Sheets returned an invalid staged row.")
    return cells


def write_to_sheet(book, title, rows, lease):
    state = sheet_state(book)
    names = range_names(state)
    if names.get(lease["name"]) != lease["id"]:
        raise ExportError("Sheets lock ownership changed.")
    # A missing owner ID invalidates this *whole* batch even after the preliminary read.
    receipt_id = "receipt_" + lease["id"]
    receipt = lease["receipt_prefix"] + lease["id"]
    ops = [{"updateNamedRange": {"namedRange": {"namedRangeId": lease["id"], "name": receipt},
                                 "fields": "name"}},
           delete_range(lease["id"]), add_range(receipt, receipt_id, lease["anchor"])]
    # Keep receipts long enough for a timed-out writer to confirm its own commit.
    cutoff = int(time.time()) - 86400
    for name, range_id in names.items():
        saved_token = range_id.removeprefix("receipt_")
        if name == lease["receipt_prefix"] + saved_token and lease_time(saved_token) < cutoff:
            ops.append(delete_range(range_id))
    props = next((s["properties"] for s in state["sheets"]
                  if s["properties"]["title"] == title), None)
    output = [HEADER, *rows]
    if props is None:
        sheet_id = secrets.randbelow(2**31 - 1) + 1
        height = len(output)
        ops.append({"addSheet": {"properties": {
            "sheetId": sheet_id, "title": title,
            "gridProperties": {"rowCount": max(1000, height), "columnCount": len(HEADER)},
        }}})
    else:
        sheet_id = props["sheetId"]
        grid = props["gridProperties"]
        height = max(len(output), previous_report_height(book, title, len(output)))
        for dimension, current, needed in (("ROWS", grid["rowCount"], height),
                                            ("COLUMNS", grid["columnCount"], len(HEADER))):
            if needed > current:
                ops.append({"appendDimension": {"sheetId": sheet_id, "dimension": dimension,
                                                "length": needed - current}})
    cell_rows = stage_values(book, lease, output, props)
    bounds = {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": len(output),
              "startColumnIndex": 0, "endColumnIndex": len(HEADER)}
    ops.append({"updateCells": {"range": bounds, "rows": cell_rows,
                                "fields": "userEnteredValue,userEnteredFormat.numberFormat"}})
    if height > len(output):
        tail = dict(bounds, startRowIndex=len(output), endRowIndex=height)
        ops.append({"updateCells": {"range": tail, "fields": "userEnteredValue"}})
    ops.append({"deleteSheet": {"sheetId": lease["stage_id"]}})

    try:
        book.batch_update({"requests": ops})
    except Exception:
        try:
            confirmed = range_names(sheet_state(book)).get(receipt) == receipt_id
        except Exception:
            raise ExportError("Sheets commit could not be confirmed.") from None
        if not confirmed:
            raise ExportError("Sheets commit was not confirmed.") from None


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


if __name__ == "__main__":
    logging.disable(logging.CRITICAL)
    try:
        main()
    except ExportError as exc:
        sys.exit(str(exc))
    except Exception:
        sys.exit("Export failed: unexpected configuration, service, or response error.")
