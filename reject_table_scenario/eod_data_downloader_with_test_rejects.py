"""
TEST FIXTURE — NOT used by the production DAG.

This is a copy of dags/lib/eod_data_downloader.py that injects a batch of
synthetic negative-volume rows into the output CSV. It exists purely to
exercise the reject-handling path (reject_table_creation.sql +
4__merge_core_with_rejects.sql) without waiting for real bad data to show up
in the Polygon feed. Do not point the main pipeline's Airflow DAG at this
file.
"""

import csv
import requests
from airflow.exceptions import AirflowFailException
import logging
import pendulum

log = logging.getLogger(__name__)


def download_polygon_eod_data_to_csv(POLYGON_API_KEY, LOOKBACK_DAYS):
    """
    Downloads the Polygon grouped daily (EOD) data, stores it as a CSV file,
    and injects a fixed batch of synthetic negative-volume rows so the
    downstream reject-handling logic has something to catch.

    Arguments:
    - POLYGON_API_KEY: API key for authentication with the Polygon API.
    - LOOKBACK_DAYS: Number of days to look back to find the latest trading day with data.
    """

    if not POLYGON_API_KEY:
        raise AirflowFailException("Missing Polygon API Key in Airflow Variables.")

    POLYGON_BASE_URL = 'https://api.polygon.io'
    EXCHANGE_TZ = 'America/New_York'
    today = pendulum.now(EXCHANGE_TZ).date()

    for i in range(LOOKBACK_DAYS):
        trading_date = today - pendulum.duration(days=i)
        trading_date = trading_date.strftime("%Y-%m-%d")
        url = f"{POLYGON_BASE_URL}/v2/aggs/grouped/locale/us/market/stocks/{trading_date}"
        params = {"adjusted": "true", "include_otc": "false", "apiKey": POLYGON_API_KEY}

        try:
            r = requests.get(url, params=params, timeout=60)
            log.info("[polygon] %s -> %s", r.url, r.status_code)
        except Exception as e:
            log.warning("[polygon] request failed for %s: %s", trading_date, e)
            continue

        if r.status_code == 200 and r.json().get("resultsCount", 0) > 0:
            log.info("Found valid trading data for date: %s", trading_date)

            data = r.json()
            results = data.get("results", [])

            fields = ["T", "o", "h", "l", "c", "v"]
            header = ["symbol", "open", "high", "low", "close", "volume"]

            out_path = f"/tmp/eod_{trading_date}.csv"
            with open(out_path, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(["trade_date"] + header)
                for row in results:
                    w.writerow([trading_date] + [row.get(k, "") for k in fields])

                # --- Inject synthetic negative-volume rows (reject-path test fixture) ---
                dummy_rows = [
                    [trading_date, "AAPL_X", 192.3, 195.6, 191.8, 194.1, -1500000],
                    [trading_date, "GOOGL_X", 138.2, 140.5, 137.6, 139.8, -980000],
                    [trading_date, "MSFT_X", 410.5, 415.2, 409.1, 412.4, -760000],
                    [trading_date, "AMZN_X", 171.8, 175.0, 170.4, 174.2, -620000],
                    [trading_date, "TSLA_X", 252.9, 258.3, 251.7, 257.5, -840000],
                    [trading_date, "META_X", 465.7, 472.2, 463.8, 471.0, -540000],
                    [trading_date, "NFLX_X", 600.1, 610.8, 598.5, 609.2, -430000],
                    [trading_date, "NVDA_X", 1135.6, 1150.3, 1130.1, 1147.9, -890000],
                    [trading_date, "INTC_X", 43.2, 44.0, 42.9, 43.8, -350000],
                    [trading_date, "IBM_TEST", 185.7, 188.9, 184.8, 187.3, -270000],
                ]
                w.writerows(dummy_rows)
                log.warning(f"[Injected] Added {len(dummy_rows)} synthetic negative-volume rows for reject-path testing.")

            return trading_date
        else:
            log.info("No data for date: %s, trying previous day.", trading_date)

    raise AirflowFailException("No grouped-daily data found within lookback window")
