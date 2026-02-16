"""
Normalize Hyperliquid values by accounting for deposits, withdrawals,
and perp↔spot transfers that occurred between the HL and Artemis snapshots.

Reads comparison_output.json, fetches ledger events from the HL API,
adjusts HL values, and writes comparison_output_normalized.json.
"""

import json
import time
import requests
from collections import defaultdict
from datetime import datetime, timezone

INPUT_FILE = "comparison_output.json"
OUTPUT_FILE = "comparison_output_normalized.json"
HL_API_URL = "https://api.hyperliquid.xyz/info"


# =============================================================================
# HL LEDGER API
# =============================================================================

def get_ledger_page(address: str, start_time_ms: int, end_time_ms: int) -> list:
    """Single page of userNonFundingLedgerUpdates."""
    resp = requests.post(
        HL_API_URL,
        json={
            "type": "userNonFundingLedgerUpdates",
            "user": address,
            "startTime": start_time_ms,
            "endTime": end_time_ms,
        },
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    return data if isinstance(data, list) else []


def get_all_ledger_events(address: str, start_ms: int, end_ms: int) -> list:
    """Paginate through all ledger events for an address in [start, end]."""
    all_events: list = []
    cursor = 0

    while True:
        events = get_ledger_page(address, cursor, end_ms)
        if not events:
            break
        all_events.extend(events)
        if len(events) < 2000:
            break
        times = [ev.get("time") for ev in events if "time" in ev]
        if times:
            cursor = max(times)
        else:
            break

    # Deduplicate
    unique = list(
        {json.dumps(ev, sort_keys=True): ev for ev in all_events}.values()
    )
    return unique


def extract_flows(events: list) -> list[tuple[int, float]]:
    """Parse ledger events into (timestamp_ms, signed_amount) tuples.

    Sign convention (from the perp account's perspective):
      deposit           → +
      withdraw          → −
      rewardsClaim      → +
      spot → perp       → +
      perp → spot       → −
    """
    flows: list[tuple[int, float]] = []

    for ev in events:
        delta = ev.get("delta", {})
        typ = delta.get("type")
        ts = ev.get("time")
        if ts is None:
            continue

        if typ == "deposit":
            try:
                amt = float(delta["usdc"])
            except (KeyError, ValueError, TypeError):
                continue
            flows.append((int(ts), amt))

        elif typ == "withdraw":
            try:
                amt = float(delta["usdc"])
            except (KeyError, ValueError, TypeError):
                continue
            flows.append((int(ts), -amt))

        elif typ == "rewardsClaim":
            try:
                amt = float(delta.get("amount", 0))
            except (ValueError, TypeError):
                continue
            flows.append((int(ts), amt))

        elif typ == "send":
            try:
                amt = float(delta.get("usdcValue", delta.get("amount", 0)))
            except (ValueError, TypeError):
                continue
            src = delta.get("sourceDex", "")
            dst = delta.get("destinationDex", "")
            if src == "" and dst == "spot":
                # perp → spot  (money leaving perp)
                flows.append((int(ts), -amt))
            elif src == "spot" and dst == "":
                # spot → perp  (money entering perp)
                flows.append((int(ts), amt))

        elif typ == "accountClassTransfer":
            try:
                amt = float(delta.get("usdc", 0))
            except (ValueError, TypeError):
                continue
            to_perp = delta.get("toPerp", False)
            if to_perp:
                flows.append((int(ts), amt))
            else:
                flows.append((int(ts), -amt))

    flows.sort(key=lambda t: t[0])
    return flows


# =============================================================================
# MAIN LOGIC
# =============================================================================

def main():
    # 1. Load existing comparison
    print(f"Loading {INPUT_FILE} …")
    with open(INPUT_FILE) as f:
        data = json.load(f)

    addresses = data["addresses"]
    print(f"  {len(addresses)} addresses, {data['days']} days\n")

    # 2. For each address: fetch ledger events, build flow index, normalize
    for idx, addr_block in enumerate(addresses, 1):
        addr = addr_block["address"]
        series = addr_block["series"]

        # Find the global min/max timestamps across both sources
        all_ts = []
        for day in series:
            art_ts = day["artemis"].get("last_timestamp")
            hl_ts = day["hyperliquid"].get("last_timestamp")
            if art_ts:
                all_ts.append(art_ts)
            if hl_ts:
                all_ts.append(hl_ts)

        if not all_ts:
            print(f"  [{idx}/{len(addresses)}] {addr[:12]}… no timestamps, skipping")
            for day in series:
                day["hyperliquid_normalized"] = {
                    "value": day["hyperliquid"].get("value"),
                    "last_timestamp": day["hyperliquid"].get("last_timestamp"),
                    "source_date": day["hyperliquid"].get("source_date"),
                    "flow_adjustment": 0,
                    "events_in_gap": 0,
                }
                day["diff_normalized"] = day["diff"].copy()
            continue

        global_start = min(all_ts)
        global_end = max(all_ts)

        print(
            f"  [{idx}/{len(addresses)}] {addr[:12]}… ",
            end="", flush=True,
        )

        # Fetch all ledger events for this address
        try:
            raw_events = get_all_ledger_events(addr, global_start, global_end)
        except Exception as e:
            print(f"API error: {e}")
            for day in series:
                day["hyperliquid_normalized"] = {
                    "value": day["hyperliquid"].get("value"),
                    "last_timestamp": day["hyperliquid"].get("last_timestamp"),
                    "source_date": day["hyperliquid"].get("source_date"),
                    "flow_adjustment": 0,
                    "events_in_gap": 0,
                }
                day["diff_normalized"] = day["diff"].copy()
            continue

        flows = extract_flows(raw_events)
        print(f"{len(raw_events)} events → {len(flows)} flows … ", end="", flush=True)

        # 3. For each day, find flows between HL ts and Artemis ts
        adjusted = 0
        for day in series:
            hl_ts = day["hyperliquid"].get("last_timestamp")
            art_ts = day["artemis"].get("last_timestamp")
            hl_val = day["hyperliquid"].get("value")

            if hl_ts is not None and art_ts is not None and hl_val is not None:
                # Window: (hl_ts, art_ts]  — events AFTER HL snapshot, up to Artemis
                gap_start = hl_ts
                gap_end = art_ts

                gap_flows = [
                    (ts, amt)
                    for ts, amt in flows
                    if gap_start < ts <= gap_end
                ]
                net_flow = sum(amt for _, amt in gap_flows)
                normalized_val = hl_val + net_flow

                day["hyperliquid_normalized"] = {
                    "value": round(normalized_val, 6),
                    "last_timestamp": hl_ts,
                    "source_date": day["hyperliquid"].get("source_date"),
                    "flow_adjustment": round(net_flow, 6),
                    "events_in_gap": len(gap_flows),
                }

                # Recompute diff with normalized value
                art_val = day["artemis"].get("value")
                if art_val is not None:
                    abs_diff = abs(art_val - normalized_val)
                    denom = max(abs(art_val), abs(normalized_val))
                    pct_diff = (abs_diff / denom * 100) if denom != 0 else 0.0
                    match = pct_diff < 0.5
                    day["diff_normalized"] = {
                        "abs": round(abs_diff, 6),
                        "pct": round(pct_diff, 4),
                        "match": match,
                    }
                    if match and not day["diff"].get("match"):
                        adjusted += 1
                else:
                    day["diff_normalized"] = {
                        "abs": None,
                        "pct": None,
                        "match": None,
                    }
            else:
                day["hyperliquid_normalized"] = {
                    "value": hl_val,
                    "last_timestamp": day["hyperliquid"].get("last_timestamp"),
                    "source_date": day["hyperliquid"].get("source_date"),
                    "flow_adjustment": 0,
                    "events_in_gap": 0,
                }
                day["diff_normalized"] = day["diff"].copy()

        print(f"fixed {adjusted} days")

    # 4. Summary
    ok_before = ok_after = mismatch_before = mismatch_after = missing = 0
    for addr_block in addresses:
        for day in addr_block["series"]:
            m_before = day["diff"].get("match")
            m_after = day["diff_normalized"].get("match")
            if m_before is None:
                missing += 1
            else:
                if m_before:
                    ok_before += 1
                else:
                    mismatch_before += 1
                if m_after:
                    ok_after += 1
                else:
                    mismatch_after += 1

    print(f"\n{'='*60}")
    print("NORMALIZATION SUMMARY")
    print(f"{'='*60}")
    print(f"  Before normalization:  OK={ok_before:,}  Mismatch={mismatch_before:,}")
    print(f"  After  normalization:  OK={ok_after:,}  Mismatch={mismatch_after:,}")
    print(f"  Improved:              {ok_after - ok_before:,} pairs fixed")
    print(f"  Missing (one side):    {missing:,}")

    # 5. Write output
    data["generated_at"] = datetime.now(timezone.utc).isoformat()
    with open(OUTPUT_FILE, "w") as f:
        json.dump(data, f, indent=2)
    print(f"\n✅ Written to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
