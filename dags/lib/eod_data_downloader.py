import csv
import requests
from airflow.exceptions import AirflowFailException
import logging
import pendulum

# Initialize logger for logging events
log = logging.getLogger(__name__)


def download_polygon_eod_data_to_csv(POLYGON_API_KEY, LOOKBACK_DAYS):
    """
    Downloads the Polygon grouped daily (EOD) data and stores it as a CSV file.

    Arguments:
    - POLYGON_API_KEY: API key for authentication with the Polygon API.
    - LOOKBACK_DAYS: Number of days to look back to find the latest trading day with data.
    """

    # Ensure that the Polygon API Key is provided
    if not POLYGON_API_KEY:
        raise AirflowFailException("Missing Polygon API Key in Airflow Variables.")

    POLYGON_BASE_URL = 'https://api.polygon.io'  # Base URL for Polygon API
    EXCHANGE_TZ = 'America/New_York'  # Timezone for trading day resolution
    today = pendulum.now(EXCHANGE_TZ).date()  # Get today's date in the specified exchange timezone

    # Iterate through the past 'LOOKBACK_DAYS' days to find a valid trading day
    for i in range(LOOKBACK_DAYS):
        # Calculate the target date by subtracting i days from today
        trading_date = today - pendulum.duration(days=i)
        trading_date = trading_date.strftime("%Y-%m-%d")  # Format the date as "YYYY-MM-DD"
        url = f"{POLYGON_BASE_URL}/v2/aggs/grouped/locale/us/market/stocks/{trading_date}"  # URL for the API request
        params = {"adjusted": "true", "include_otc": "false", "apiKey": POLYGON_API_KEY}

        try:
            # Make the API request to fetch the grouped daily data
            r = requests.get(url, params=params, timeout=60)
            log.info("[polygon] %s -> %s", r.url, r.status_code)  # Log the request URL and response status
        except Exception as e:
            log.warning("[polygon] request failed for %s: %s", trading_date, e)
            continue

        # If the response is successful and contains results, write to CSV
        if r.status_code == 200 and r.json().get("resultsCount", 0) > 0:
            log.info("Found valid trading data for date: %s", trading_date)

            data = r.json()
            results = data.get("results", [])

            # Define the CSV structure
            fields = ["T", "o", "h", "l", "c", "v"]
            header = ["symbol", "open", "high", "low", "close", "volume"]

            # Write the data to the CSV file
            out_path = f"/tmp/eod_{trading_date}.csv"  # Define a fixed path for storing the CSV file locally
            with open(out_path, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(["trade_date"] + header)  # Write header row
                for row in results:
                    w.writerow([trading_date] + [row.get(k, "") for k in fields])  # Write the trading day data

            # Return the date for further use (if needed)
            return trading_date
        else:
            log.info("No data for date: %s, trying previous day.", trading_date)

    # If no valid trading day is found within the lookback window, raise an exception
    raise AirflowFailException("No grouped-daily data found within lookback window")
