#OLD S3 MONITORING

env_match = re.search(r'us-east-1-(.*?)-HBase', arn)
    ENV = env_match.group(1) if env_match else "Unknown-Env"
except Exception:
    ENV = "Unknown-Env"
#!/usr/bin/python3.8

import boto3
import datetime
import requests
import sys
import re

# ==== CONFIG ====
# --------------------- Getting The Environment Name ----------------------------
try:
    arn = boto3.client('sts').get_caller_identity()['Arn']
# --------------------- S3 Bucket Information ----------------------------
S3_BUCKET_NAME = f"mlife-cdp-{ENV}-backup"
CLUSTER_NAME = f"cod-{ENV}-hdfs"

# ----------------- Determine Region Based on Environment -----------------
if "eu" in ENV.lower():   # Replace Environment
    AWS_REGION = "eu-central-1"
else:
    AWS_REGION = "us-east-1"




# ==== Teams Webhook ====
WEBHOOK_URL = "https://defaultd73a39db6eda495d80007579f56d68.b7.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/16c8b86a44e44223bacf436416b1706e/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=9E8Ogx1J14sUEtwXmRG5fjv9qPuNDkdVYDpvL2xDNkc"
'''
# --------------------- Getting The Teams Webhook From SSM Parameter ---------------------------- 
try:
    ssm_client = boto3.client('ssm', region_name=REGION_NAME)
    webhook_url_parameter_name = "/cdp/msteams_webhook_url_cloudera_manager_alerts"
    TEAMS_WEBHOOK_URL = ssm_client.get_parameter(
        Name=webhook_url_parameter_name,
        WithDecryption=True
    )['Parameter']['Value']
except Exception as e:
    print(f"[ERROR] Unable to fetch Teams webhook URL: {e}")
    sys.exit(1)

'''

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
        response = requests.post(WEBHOOK_URL, json=payload)
        if response.status_code in (200, 202):
            print(f" Teams alert sent: {title}")
        else:
            print(f" Failed to send Teams alert. Status: {response.status_code}")
    except Exception as e:
        print(f" Error sending Teams alert: {e}")


# ==== Helper: Get snapshot table names from S3 ====
def get_snapshot_tables(date_prefix, until_hour=None):
    s3 = boto3.client("s3", region_name=AWS_REGION)
    prefix = f"{CLUSTER_NAME}/hbase/{date_prefix}/.hbase-snapshot/"
    print(f" Scanning S3 path: s3://{S3_BUCKET_NAME}/{prefix}")

    tables = set()
    try:
        response = s3.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=prefix)
        for obj in response.get("Contents", []):
            key = obj["Key"]
            parts = key.split("/")
            if len(parts) >= 6:
                snapshot_name = parts[4]
                if snapshot_name.endswith(".snapshotinfo") or snapshot_name.endswith("data.manifest"):
                    continue

                # Extract table name from snapshot name (e.g., xyz_SNAPSHOT_202510082047 → xyz)
                table_name = snapshot_name.split("_SNAPSHOT_")[0]

                # Optionally, skip snapshots beyond the current hour
                if until_hour and snapshot_name.endswith(until_hour):
                    continue

                tables.add(table_name)
    except Exception as e:
        print(f" Error accessing S3: {e}")

    return tables


# ==== Main Logic ====
def check_table_snapshot_consistency():
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    one_hour_ago_utc = now_utc - datetime.timedelta(hours=1)
    yesterday_utc = now_utc - datetime.timedelta(days=1)

    today_prefix = now_utc.strftime("%Y%m%d")
    yesterday_prefix = yesterday_utc.strftime("%Y%m%d")

    timestamp_now = now_utc.strftime("%Y-%m-%d %H:%M:%S UTC")
    timestamp_one_hour_ago = one_hour_ago_utc.strftime("%Y-%m-%d %H:%M:%S UTC")

    print(f" Checking snapshot consistency for last 1 hour ({timestamp_one_hour_ago} → {timestamp_now})")

    # === Stage 1: Full Baseline (yesterday + today)
    baseline_tables = get_snapshot_tables(yesterday_prefix) | get_snapshot_tables(today_prefix)
    print(f" Baseline total tables (yesterday + today): {len(baseline_tables)}")

    # === Stage 2: Last one hour snapshot detection
    s3 = boto3.client("s3", region_name=AWS_REGION)
    prefix = f"{CLUSTER_NAME}/hbase/{today_prefix}/.hbase-snapshot/"
    print(f" Scanning S3 for snapshots within last hour in: s3://{S3_BUCKET_NAME}/{prefix}")

    recent_tables = set()
    try:
        response = s3.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=prefix)
        for obj in response.get("Contents", []):
            key = obj["Key"]
            parts = key.split("/")
            if len(parts) >= 6:
                snapshot_name = parts[4]
                if "_SNAPSHOT_" not in snapshot_name:
                    continue

                # Extract timestamp suffix
                try:
                    ts_str = snapshot_name.split("_SNAPSHOT_")[-1]
                    snapshot_time = datetime.datetime.strptime(ts_str, "%Y%m%d%H%M").replace(tzinfo=datetime.timezone.utc)
                except Exception:
                    continue

                # If snapshot falls within last hour window
                if one_hour_ago_utc <= snapshot_time <= now_utc:
                    table_name = snapshot_name.split("_SNAPSHOT_")[0]
                    recent_tables.add(table_name)
    except Exception as e:
        print(f" Error while fetching last-hour snapshots: {e}")

    print(f" Tables found in last hour: {len(recent_tables)}")

    # === Compare sets ===
    missing_tables = baseline_tables - recent_tables
    new_tables = recent_tables - baseline_tables
    
    # === Ignore only .tmp table ===
    if ".tmp" in missing_tables:
        missing_tables.remove(".tmp")
        
    print(f" Missing Tables: {missing_tables}")
    print(f" New Tables: {new_tables}")
   
    # === Send Alerts ===
    if missing_tables:
        text = (
            f"**Timestamp:** {timestamp_now}\n\n"
            f"**Period:** From `{timestamp_one_hour_ago}` to `{timestamp_now}`\n\n"
            f"**Baseline Tables:** {len(baseline_tables)}\n\n"
            f"**Last Hour Tables:** {len(recent_tables)}\n\n"
            f"**Missing Tables:** {len(missing_tables)}\n\n"
            f"**Tables Missing in This Hour:**\n" +
            "\n".join(f"- `{t}`" for t in sorted(missing_tables))
        )
        send_teams_alert(f"[{ENV}] Snapshot Alert - Missing Tables in Last Hour", text, color="Attention")

    elif new_tables:
        text = (
            f"**Timestamp:** {timestamp_now}\n"
            f"**Period:** From `{timestamp_one_hour_ago}` to `{timestamp_now}`\n\n"
            f"**Baseline Tables:** {len(baseline_tables)}\n"
            f"**Last Hour Tables:** {len(recent_tables)}\n"
            f"**New Tables:** {len(new_tables)}\n\n"
            f"**Newly Detected Tables:**\n" +
            "\n".join(f"- `{t}`" for t in sorted(new_tables))
        )
        send_teams_alert(f"[{ENV}] Snapshot Alert - New Tables in Last Hour", text, color="Good")

    else:
        print(f" All expected {len(baseline_tables)} tables were found in the last hour.")


# ===== Lambda Handler =====
def lambda_handler(event, context):
    check_table_snapshot_consistency()


# ===== Local Execution =====
if __name__ == "__main__":
    check_table_snapshot_consistency()
