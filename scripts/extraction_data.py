"""
Perp Account Value Comparator
Compares perp account values per address between:
  A) Artemis S3 snapshots (reusing old_script_artemis logic)
  B) Hyperliquid portfolio API
Outputs JSON structured for visualization.
"""

import boto3
from botocore.config import Config
import json
import os
import csv
import requests
from datetime import datetime, timedelta, timezone
from collections import defaultdict

# ─── Artemis S3 constants (from old_script_artemis) ─────────────────────────
BUCKET_NAME = "artemis-hyperliquid-data"
PREFIX = "raw/perp_and_spot_balances/"
TEMP_FILE = "temp_balance_file.jsonl"

# ─── Time window ─────────────────────────────────────────────────────────────
DAYS = 32
END_DATE = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
START_DATE = END_DATE - timedelta(days=DAYS - 1)

OUTPUT_FILE = "comparison_output.json"
HL_API_URL = "https://api.hyperliquid.xyz/info"


# =============================================================================
# ADDRESS LOADING
# =============================================================================

def load_addresses(csv_path: str) -> list[str]:
    """Load addresses from outlier_address.csv and return lower-cased list."""
    addresses: list[str] = []
    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            addr = row["address"].strip().lower()
            if addr:
                addresses.append(addr)
    return addresses


# =============================================================================
# SOURCE A — ARTEMIS S3  (logic lifted from old_script_artemis.py)
# =============================================================================

def get_s3_client():
    config = Config(
        connect_timeout=30,
        read_timeout=300,
        retries={"max_attempts": 3},
    )
    return boto3.client("s3", config=config)


def list_files_for_date(s3_client, date: datetime) -> list[str]:
    prefix = f"{PREFIX}{date.year}/{date.month:02d}/{date.day:02d}/"
    try:
        response = s3_client.list_objects_v2(
            Bucket=BUCKET_NAME,
            Prefix=prefix,
            RequestPayer="requester",
        )
        if "Contents" not in response:
            return []
        return sorted(
            obj["Key"]
            for obj in response["Contents"]
            if obj["Key"].endswith(".jsonl")
        )
    except Exception as e:
        print(f"  S3 list error for {date.date()}: {e}")
        return []


def download_file(s3_client, s3_key: str, local_path: str) -> bool:
    try:
        head = s3_client.head_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            RequestPayer="requester",
        )
        file_size = head["ContentLength"]
        file_size_mb = file_size / (1024 * 1024)
        file_name = os.path.basename(s3_key)
        print(f"{file_name} ({file_size_mb:.1f} MB) … ", end="", flush=True)

        s3_client.download_file(
            BUCKET_NAME,
            s3_key,
            local_path,
            ExtraArgs={"RequestPayer": "requester"},
        )
        print("✓ ", end="", flush=True)
        return True
    except Exception as e:
        print(f"\n  S3 download error {s3_key}: {e}")
        return False


def extract_wallet_data(file_path: str, wallet_set: set[str]) -> list[dict]:
    """Parse a .jsonl snapshot and return records for addresses in wallet_set.

    Each record: {"address", "timestamp_ms", "account_value"}
    """
    results: list[dict] = []
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            try:
                data = json.loads(line.strip())
                if data.get("_metadata", False):
                    continue
                address = data.get("address", "").lower()
                if address not in wallet_set:
                    continue

                response = data.get("response", {})
                perpetual = response.get("perpetual", {})
                margin_summary = perpetual.get("marginSummary", {})
                account_value = float(margin_summary.get("accountValue", 0))

                # Timestamp handling: raw field may be ISO string or epoch-ms
                raw_ts = data.get("timestamp", "")
                if isinstance(raw_ts, (int, float)):
                    ts_ms = int(raw_ts)
                else:
                    try:
                        dt = datetime.fromisoformat(str(raw_ts).replace("Z", "+00:00"))
                        ts_ms = int(dt.timestamp() * 1000)
                    except Exception:
                        ts_ms = 0

                results.append(
                    {
                        "address": address,
                        "timestamp_ms": ts_ms,
                        "account_value": account_value,
                    }
                )
            except (json.JSONDecodeError, Exception):
                continue
    return results


def fetch_artemis_data(
    addresses: list[str],
) -> dict[str, dict[str, list[dict]]]:
    """Download Artemis S3 snapshots for [START_DATE, END_DATE] and return
    nested dict: address → date_str → [records].
    """
    wallet_set = set(addresses)
    s3_client = get_s3_client()

    # address -> date_str -> list of {timestamp_ms, account_value}
    data: dict[str, dict[str, list[dict]]] = defaultdict(lambda: defaultdict(list))

    current = START_DATE
    total_days = (END_DATE - START_DATE).days + 1
    day_num = 0

    while current <= END_DATE:
        day_num += 1
        date_str = current.strftime("%Y-%m-%d")
        print(f"  [Artemis {day_num}/{total_days}] {date_str} … ", end="", flush=True)

        files = list_files_for_date(s3_client, current)
        if not files:
            print("no files")
            current += timedelta(days=1)
            continue

        day_records: list[dict] = []
        print(f"{len(files)} file(s): ", end="", flush=True)

        # Download ALL files for the day so we can pick the latest per address
        for s3_key in files:
            if download_file(s3_client, s3_key, TEMP_FILE):
                recs = extract_wallet_data(TEMP_FILE, wallet_set)
                day_records.extend(recs)
                if os.path.exists(TEMP_FILE):
                    os.remove(TEMP_FILE)

        for rec in day_records:
            data[rec["address"]][date_str].append(
                {
                    "timestamp_ms": rec["timestamp_ms"],
                    "account_value": rec["account_value"],
                }
            )

        print(f"→ {len(day_records)} records")
        current += timedelta(days=1)

    return data


# =============================================================================
# SOURCE B — HYPERLIQUID API
# =============================================================================

def fetch_hyperliquid_data(
    addresses: list[str],
) -> dict[str, dict[str, list[dict]]]:
    """Call the Hyperliquid portfolio endpoint for each address.

    Returns: address → date_str → [{ timestamp_ms, account_value }]
    """
    data: dict[str, dict[str, list[dict]]] = defaultdict(lambda: defaultdict(list))

    for idx, addr in enumerate(addresses, 1):
        print(f"  [HL API {idx}/{len(addresses)}] {addr[:10]}… ", end="", flush=True)
        try:
            resp = requests.post(
                HL_API_URL,
                json={"type": "portfolio", "user": addr},
                timeout=30,
            )
            resp.raise_for_status()
            payload = resp.json()
        except Exception as e:
            print(f"error: {e}")
            continue

        # Response is a list of [key, data] pairs, e.g. ["perpMonth", {"accountValueHistory": [...], ...}]
        try:
            history = []
            for entry in payload:
                if isinstance(entry, list) and len(entry) == 2 and entry[0] == "perpMonth":
                    history = entry[1].get("accountValueHistory", [])
                    break
            if not history:
                print("no perpMonth data")
                continue
        except Exception:
            print("unexpected shape")
            continue

        kept = 0
        for point in history:
            # Each point: [timestamp_ms, account_value_str]
            ts_ms = int(point[0])
            value = float(point[1])
            dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)

            # Filter to our date window (include 1 day before START for
            # the day-shift alignment with Artemis)
            hl_start = (START_DATE - timedelta(days=1)).date()
            if dt.date() < hl_start or dt.date() > END_DATE.date():
                continue

            date_str = dt.strftime("%Y-%m-%d")
            data[addr.lower()][date_str].append(
                {"timestamp_ms": ts_ms, "account_value": value}
            )
            kept += 1

        print(f"{kept} points")

    return data


# =============================================================================
# COMPARISON
# =============================================================================

def pick_latest(records: list[dict]) -> dict | None:
    """From a list of {timestamp_ms, account_value} return the one with the
    largest timestamp (end-of-day proxy)."""
    if not records:
        return None
    return max(records, key=lambda r: r["timestamp_ms"])


def build_comparison(
    addresses: list[str],
    artemis: dict[str, dict[str, list[dict]]],
    hyperliquid: dict[str, dict[str, list[dict]]],
) -> dict:
    """Build the final JSON structure.

    Alignment: Artemis snapshots at ~01:17 UTC (start of day) while
    Hyperliquid's last point is ~22:46 UTC (end of day).  So Artemis's
    value for date D is closest to Hyperliquid's value for date D-1.
    We pair them accordingly: each output row uses the Artemis value
    for that date and the HL value from the previous calendar day.
    """

    # Generate list of date strings in window (plus one day before for HL look-back)
    dates: list[str] = []
    cur = START_DATE
    while cur <= END_DATE:
        dates.append(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)

    address_results = []

    for addr in addresses:
        addr_lower = addr.lower()
        series = []
        for date_str in dates:
            art_latest = pick_latest(artemis.get(addr_lower, {}).get(date_str, []))

            # HL: use the PREVIOUS day's latest value (closest in time to
            # Artemis ~01:17 UTC on this date)
            prev_date_str = (
                datetime.strptime(date_str, "%Y-%m-%d") - timedelta(days=1)
            ).strftime("%Y-%m-%d")
            hl_latest = pick_latest(
                hyperliquid.get(addr_lower, {}).get(prev_date_str, [])
            )

            art_value = art_latest["account_value"] if art_latest else None
            hl_value = hl_latest["account_value"] if hl_latest else None

            # Compute diff only when both values are present
            if art_value is not None and hl_value is not None:
                abs_diff = abs(art_value - hl_value)
                denom = max(abs(art_value), abs(hl_value))
                pct_diff = (abs_diff / denom * 100) if denom != 0 else 0.0
                match = pct_diff < 0.5
            else:
                abs_diff = None
                pct_diff = None
                match = None

            entry: dict = {"date": date_str}

            entry["artemis"] = (
                {
                    "value": art_value,
                    "last_timestamp": art_latest["timestamp_ms"],
                }
                if art_latest
                else {"value": None, "last_timestamp": None}
            )

            entry["hyperliquid"] = (
                {
                    "value": hl_value,
                    "last_timestamp": hl_latest["timestamp_ms"],
                    "source_date": prev_date_str,
                }
                if hl_latest
                else {"value": None, "last_timestamp": None, "source_date": prev_date_str}
            )

            entry["diff"] = {
                "abs": abs_diff,
                "pct": round(pct_diff, 4) if pct_diff is not None else None,
                "match": match,
            }

            series.append(entry)

        address_results.append({"address": addr_lower, "series": series})

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "days": DAYS,
        "addresses": address_results,
    }


# =============================================================================
# MAIN
# =============================================================================

def load_artemis_from_output(output_path: str) -> dict[str, dict[str, list[dict]]]:
    """Reload Artemis data from an existing comparison_output.json so we
    don't need to re-download from S3."""
    with open(output_path, "r") as f:
        existing = json.load(f)

    data: dict[str, dict[str, list[dict]]] = defaultdict(lambda: defaultdict(list))
    for addr_block in existing.get("addresses", []):
        addr = addr_block["address"].lower()
        for day in addr_block.get("series", []):
            art = day.get("artemis", {})
            if art.get("value") is not None and art.get("last_timestamp") is not None:
                data[addr][day["date"]].append(
                    {
                        "timestamp_ms": art["last_timestamp"],
                        "account_value": art["value"],
                    }
                )
    return data


def main():
    print("=" * 60)
    print("PERP ACCOUNT VALUE COMPARATOR")
    print(f"  Window : {START_DATE.date()} → {END_DATE.date()} ({DAYS} days)")
    print("=" * 60)

    # 1. Load addresses
    addresses = load_addresses("outlier_address.csv")
    print(f"\nLoaded {len(addresses)} addresses from outlier_address.csv\n")

    # 2. Reuse Artemis data from existing output (skip S3 re-download)
    if os.path.exists(OUTPUT_FILE):
        print("─── Source A: Loading Artemis from existing output ───")
        artemis_data = load_artemis_from_output(OUTPUT_FILE)
        cached_addrs = len([a for a in artemis_data if artemis_data[a]])
        print(f"  Loaded cached Artemis data for {cached_addrs} addresses\n")
    else:
        print("─── Source A: Artemis S3 (no cached output found) ───")
        artemis_data = fetch_artemis_data(addresses)

    # 3. Fetch Hyperliquid API (Source B)
    print("─── Source B: Hyperliquid API ───")
    hl_data = fetch_hyperliquid_data(addresses)

    # 4. Compare & build output
    print("\n─── Building comparison ───")
    result = build_comparison(addresses, artemis_data, hl_data)

    # 5. Write JSON
    with open(OUTPUT_FILE, "w") as f:
        json.dump(result, f, indent=2)
    print(f"\n✅  Written to {OUTPUT_FILE}")

    # Quick summary
    total_ok = 0
    total_mismatch = 0
    total_missing = 0
    for a in result["addresses"]:
        for s in a["series"]:
            m = s["diff"]["match"]
            if m is True:
                total_ok += 1
            elif m is False:
                total_mismatch += 1
            else:
                total_missing += 1

    print(f"    OK (< 0.5%): {total_ok}")
    print(f"    Mismatch   : {total_mismatch}")
    print(f"    Missing    : {total_missing}")
    print("Done.")


if __name__ == "__main__":
    main()
