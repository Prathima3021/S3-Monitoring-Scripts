#S3 MONITORING SCRIPT

#!/usr/bin/python3.8

import boto3
import datetime
import requests
import sys
import re
import json
from botocore.exceptions import ClientError

# ==== CONFIG ==== 
# --------------------- Getting The Environment Name ----------------------------
try:
    arn = boto3.client('sts').get_caller_identity()['Arn']
    env_match = re.search(r'us-east-1-(.*?)-HBase', arn)
    ENV = env_match.group(1) if env_match else "Unknown-Env"
except Exception:
    ENV = "Unknown-Env"

# --------------------- S3 Bucket Information ----------------------------
S3_BUCKET_NAME = f"mlife-cdp-{ENV}-backup"
CLUSTER_NAME = f"cod-{ENV}-hdfs"

# ----------------- Determine Region Based on Environment -----------------
AWS_REGION = "eu-central-1" if "eu" in ENV.lower() else "us-east-1"

# ==== Teams Webhook ====
WEBHOOK_URL = "https://defaultd73a39db6eda495d80007579f56d68.b7.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/16c8b86a44e44223bacf436416b1706e/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=9E8Ogx1J14sUEtwXmRG5fjv9qPuNDkdVYDpvL2xDNkc"

# ==== SETTINGS for baseline & missing history (Option D) ====
S3_METADATA_PREFIX = f"{CLUSTER_NAME}/hbase/snapshot_monitor"
BASELINE_KEY = f"{S3_METADATA_PREFIX}/baseline.json"
MISSING_HISTORY_KEY = f"{S3_METADATA_PREFIX}/missing_history.json"

# threshold before removing table from baseline
MISSING_THRESHOLD = 3

IGNORE_TABLES = {".tmp"}

s3 = boto3.client("s3", region_name=AWS_REGION)

# ==== Helper: Send Teams Alert ====
def send_teams_alert(title, text, color="Attention"):
    payload = {
        "type": "message",
        "attachments": [
            {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": {
                    "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                    "type": "AdaptiveCard",
                    "version": "1.5",
                    "body": [
                        {"type": "TextBlock", "text": title, "weight": "Bolder", "size": "Large", "color": color},
                        {"type": "TextBlock", "text": f"**Environment:** {ENV}", "wrap": True},
                        {"type": "TextBlock", "text": text, "wrap": True}
                    ]
                }
            }
        ]
    }
    try:
        r = requests.post(WEBHOOK_URL, json=payload, timeout=10)
        if r.status_code not in (200, 202):
            print(f"Failed to send Teams alert: {r.status_code} - {r.text}")
    except Exception as e:
        print(f"Error sending Teams alert: {e}")

# ==== Helper: S3 JSON read/write ====
def load_json_from_s3(key):
    try:
        resp = s3.get_object(Bucket=S3_BUCKET_NAME, Key=key)
        return json.loads(resp["Body"].read())
    except ClientError as e:
        if e.response["Error"]["Code"] in ("NoSuchKey", "NotFound"):
            return None
        raise

def save_json_to_s3(key, obj):
    body = json.dumps(obj, indent=2).encode("utf-8")
    s3.put_object(Bucket=S3_BUCKET_NAME, Key=key, Body=body)

# ==== Helper: read snapshot tables (yesterday/today bootstrapping) ====
def get_snapshot_tables(date_prefix):
    prefix = f"{CLUSTER_NAME}/hbase/{date_prefix}/.hbase-snapshot/"
    print(f"Scanning S3 path: s3://{S3_BUCKET_NAME}/{prefix}")

    tables = set()
    paginator = s3.get_paginator("list_objects_v2")
    try:
        for page in paginator.paginate(Bucket=S3_BUCKET_NAME, Prefix=prefix):
            for obj in page.get("Contents", []):
                parts = obj["Key"].split("/")
                if len(parts) < 5:
                    continue
                snap = parts[4]
                if "_SNAPSHOT_" not in snap:
                    continue
                if snap.endswith(".snapshotinfo") or snap.endswith("data.manifest"):
                    continue
                tables.add(snap.split("_SNAPSHOT_")[0])
    except ClientError as e:
        print("Error:", e)

    return tables

# ==== Helper: current-hour snapshot tables ====
def get_current_hour_tables(now_utc):
    one_hour_ago = now_utc - datetime.timedelta(hours=1)
    tables = set()
    prefixes = {
        now_utc.strftime("%Y%m%d"),
        one_hour_ago.strftime("%Y%m%d")
    }

    paginator = s3.get_paginator("list_objects_v2")

    for dp in prefixes:
        prefix = f"{CLUSTER_NAME}/hbase/{dp}/.hbase-snapshot/"
        try:
            for page in paginator.paginate(Bucket=S3_BUCKET_NAME, Prefix=prefix):
                for obj in page.get("Contents", []):
                    parts = obj["Key"].split("/")
                    if len(parts) < 5:
                        continue
                    snap = parts[4]
                    if "_SNAPSHOT_" not in snap:
                        continue

                    try:
                        ts_str = snap.split("_SNAPSHOT_")[1]
                        snap_ts = datetime.datetime.strptime(ts_str, "%Y%m%d%H%M")
                        snap_ts = snap_ts.replace(tzinfo=datetime.timezone.utc)
                    except:
                        continue

                    if one_hour_ago <= snap_ts <= now_utc:
                        tables.add(snap.split("_SNAPSHOT_")[0])

        except ClientError as e:
            print("Error:", e)

    return tables

# ==== MAIN LOGIC ====
def check_table_snapshot_consistency():
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    one_hour_ago = now_utc - datetime.timedelta(hours=1)

    timestamp_now = now_utc.strftime("%Y-%m-%d %H:%M:%S UTC")
    timestamp_prev = one_hour_ago.strftime("%Y-%m-%d %H:%M:%S UTC")

    # ---- Load baseline ----
    baseline_obj = load_json_from_s3(BASELINE_KEY)
    if baseline_obj is None:
        print("baseline.json missing → bootstrapping from yesterday & today")
        today = now_utc.strftime("%Y%m%d")
        yesterday = (now_utc - datetime.timedelta(days=1)).strftime("%Y%m%d")

        baseline_set = get_snapshot_tables(today) | get_snapshot_tables(yesterday)
        save_json_to_s3(BASELINE_KEY, sorted(baseline_set))
        print(f"Bootstrapped baseline with {len(baseline_set)} tables.")
    else:
        baseline_set = set(baseline_obj)

    print(f"Baseline tables loaded: {len(baseline_set)}")

    # ---- Load missing history ----
    miss_hist = load_json_from_s3(MISSING_HISTORY_KEY) or {}
    miss_hist = {k: int(v) for k, v in miss_hist.items()}

    # ---- Get current-hour snapshot tables ----
    current_hour_tables = get_current_hour_tables(now_utc)
    print(f"Tables found in last hour: {len(current_hour_tables)}")

    # ---- Detect missing & new tables ----
    missing_tables = sorted([t for t in (baseline_set - current_hour_tables) if t not in IGNORE_TABLES])
    new_tables = sorted([t for t in (current_hour_tables - baseline_set) if t not in IGNORE_TABLES])

    print(f"Missing tables: {missing_tables}")
    print(f"New tables: {new_tables}")

    baseline_before = len(baseline_set)

    # ---- Add new tables to baseline ----
    if new_tables:
        baseline_set.update(new_tables)
        save_json_to_s3(BASELINE_KEY, sorted(baseline_set))
        print(f"Updated baseline count: {len(baseline_set)}")

    # ---- Missing history update ----
    to_remove = set()

    for t in missing_tables:
        miss_hist[t] = miss_hist.get(t, 0) + 1
        print(f"{t} missing count → {miss_hist[t]}")
        if miss_hist[t] >= MISSING_THRESHOLD:
            to_remove.add(t)

    # Reset counters for recovered tables
    for t in list(miss_hist.keys()):
        if t not in missing_tables and t in current_hour_tables:
            print(f"{t} recovered → counter reset")
            miss_hist.pop(t, None)

    # ---- Remove tables hitting threshold ----
    if to_remove:
        print("Tables removed from baseline:", sorted(to_remove))
        baseline_set -= to_remove
        save_json_to_s3(BASELINE_KEY, sorted(baseline_set))

        # Remove from history also
        for t in to_remove:
            miss_hist.pop(t, None)

    # ---- Save updated missing history ----
    save_json_to_s3(MISSING_HISTORY_KEY, miss_hist)

    # ---- Build Teams alert ----
    if missing_tables:

        # Only show baseline preview if threshold reached
        preview_text = ""
        if to_remove:
            preview_after = baseline_before - len(missing_tables)
            preview_text = f"**Baseline preview after removal:** {preview_after}\n\n"

        alert_text = (
            f"**Timestamp:** {timestamp_now}\n\n"
            f"**Period:** `{timestamp_prev}` → `{timestamp_now}`\n\n"
            f"**Baseline Tables (before):** {baseline_before}\n\n"
            f"**Last Hour Tables:** {len(current_hour_tables)}\n\n"
            f"**Missing Tables (count):** {len(missing_tables)}\n\n"
            f"**Tables Missing in This Hour:**\n"
            + "\n".join(f"- `{t}`" for t in missing_tables) + "\n\n"
            + preview_text +
            f"**Tables reaching removal threshold:** "
            f"{sorted(to_remove) if to_remove else 'None'}\n"
        )

        send_teams_alert(
            f"[{ENV}] Snapshot Alert - Missing Tables in Last Hour",
            alert_text,
            color="Attention"
        )

    # ---- New tables info alert ----
    if new_tables:
        new_text = (
            f"**Timestamp:** {timestamp_now}\n\n"
            f"**New tables detected:** {len(new_tables)}\n\n"
            + "\n".join(f"- `{t}`" for t in new_tables) + "\n\n"
            f"**Updated baseline count:** {len(baseline_set)}\n"
        )
        send_teams_alert(
            f"[{ENV}] Snapshot Info - New Tables Detected",
            new_text,
            color="Good"
        )

    print("Run complete.\n")

# ==== LAMBDA HANDLER ====
def lambda_handler(e_vent, context):
    check_table_snapshot_consistency()

# ==== LOCAL RUN ====
if __name__ == "__main__":
    check_table_snapshot_consistency()

