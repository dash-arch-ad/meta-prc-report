import os
import json
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials


def load_config():
    secret = os.environ.get("APP_SECRET_JSON")
    if not secret:
        raise ValueError("APP_SECRET_JSON が設定されていません。")
    return json.loads(secret)


def get_instagram_follows(actions):
    instagram_follows = 0

    if not actions:
        return instagram_follows

    for action in actions:
        if action.get("action_type") in ["instagram_follows", "instagram_follow"]:
            instagram_follows += float(action.get("value", 0))

    return instagram_follows


def fetch_meta_insights(config):
    meta = config["meta"]

    access_token = meta["token"]
    account_id = meta["account_id"]

    if not account_id.startswith("act_"):
        account_id = f"act_{account_id}"

    url = f"https://graph.facebook.com/v25.0/{account_id}/insights"

    params = {
        "access_token": access_token,
        "level": "ad",
        "date_preset": "this_month",
        "fields": ",".join([
            "date_start",
            "date_stop",
            "campaign_name",
            "adset_name",
            "ad_name",
            "spend",
            "impressions",
            "clicks",
            "actions"
        ]),
        "limit": 500
    }

    all_rows = []

    while url:
        response = requests.get(url, params=params)

        if response.status_code != 200:
            print("Meta API Error:")
            print(response.text)

        response.raise_for_status()
        data = response.json()

        all_rows.extend(data.get("data", []))

        url = data.get("paging", {}).get("next")
        params = None

    return all_rows


def write_to_sheet(config, rows):
    sheets = config["sheets"]

    spreadsheet_id = sheets["spreadsheet_id"]
    worksheet_name = sheets["test_worksheet_name"]

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]

    credentials_info = config["gcp_service_account"]

    credentials = ServiceAccountCredentials.from_json_keyfile_dict(
        credentials_info,
        scopes
    )

    gc = gspread.authorize(credentials)
    sh = gc.open_by_key(spreadsheet_id)

    try:
        ws = sh.worksheet(worksheet_name)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=worksheet_name, rows=1000, cols=20)

    output = [[
        "date_start",
        "date_stop",
        "campaign_name",
        "adset_name",
        "ad_name",
        "spend",
        "impressions",
        "clicks",
        "instagram_follows",
        "actions_raw"
    ]]

    for row in rows:
        actions = row.get("actions", [])
        instagram_follows = get_instagram_follows(actions)

        output.append([
            row.get("date_start", ""),
            row.get("date_stop", ""),
            row.get("campaign_name", ""),
            row.get("adset_name", ""),
            row.get("ad_name", ""),
            row.get("spend", ""),
            row.get("impressions", ""),
            row.get("clicks", ""),
            instagram_follows,
            json.dumps(actions, ensure_ascii=False)
        ])

    ws.clear()
    ws.update("A1", output)


def main():
    config = load_config()
    rows = fetch_meta_insights(config)
    write_to_sheet(config, rows)

    print(f"完了：{len(rows)}件を git_test に出力しました。")


if __name__ == "__main__":
    main()
