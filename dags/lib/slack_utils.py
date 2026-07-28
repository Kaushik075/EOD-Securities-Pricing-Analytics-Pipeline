# dags/lib/slack_utils.py


# ------------------------------------------------------------------------
# Slack helpers (Incoming Webhook only) for Airflow
# ------------------------------------------------------------------------
# This module provides three simple functions to integrate Slack alerts
# with Airflow DAGs using the built-in 'Slack Incoming Webhook' connection.
#
# Functions:
#   1. _get_webhook_url()  →  Builds and returns the full Slack webhook URL.
#   2. slack_post(text)    →  Sends a message to Slack.
#   3. on_task_failure()   →  Airflow callback that posts a message on task failure.


from airflow.sdk.bases.hook import BaseHook  # For accessing Airflow connections
import requests  # For sending HTTP POST requests

# Airflow Connection ID used for Slack Incoming Webhook integration
SLACK_CONN_ID = "slack_default"


def _get_webhook_url() -> str | None:
    """Build the Slack Incoming Webhook URL from the Airflow connection.
       The resulting URL will be constructed as:
        https://hooks.slack.com/services/<token>
    """
    try:
        conn = BaseHook.get_connection(SLACK_CONN_ID)
        return f"{conn.schema}://{conn.host}/{conn.password}"
    except Exception:
        return None


def slack_post(text: str) -> bool:
    """Post a simple message to Slack.
       Returns True on success, False otherwise."""
    url = _get_webhook_url()
    if not url:
        print("[slack] ❌ No webhook URL found — check Airflow connection setup.")
        return False

    try:
        r = requests.post(url, json={"text": text}, timeout=10)
        print("[slack] ✅ Message sent successfully.")
        return True
    except Exception as e:  # FIX: bind the exception so failures are actually logged
        print(f"[slack] ❌ Exception while sending message: {e}")
        return False


def on_task_failure(context) -> None:
    """DAG/task failure callback: sends a short alert with a log link."""
    ti = context.get("ti")
    dag_id = context.get("dag").dag_id if context.get("dag") else "unknown_dag"
    task_id = ti.task_id if ti else "unknown_task"
    run_id = context.get("run_id", "unknown_run")
    log_url = getattr(ti, "log_url", "")
    err = str(context.get("exception"))[:300]

    msg = (
            ":x: *Airflow task failed*\n"
            f"• DAG: `{dag_id}`  • Task: `{task_id}`\n"
            f"• Run: `{run_id}`\n"
            f"• Error: `{err}`\n"
            + (f"• <{log_url}|Logs>" if log_url else "")
    )
    slack_post(msg)
