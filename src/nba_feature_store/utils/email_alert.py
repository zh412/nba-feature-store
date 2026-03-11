# ============================================================
# EMAIL ALERT UTILITY
# ============================================================

import os
import smtplib
from email.message import EmailMessage

from nba_feature_store.utils.logging import log


# ------------------------------------------------------------
# LOAD EMAIL CREDENTIALS FROM ENVIRONMENT VARIABLES
# ------------------------------------------------------------
# These environment variables must be configured locally
# for email alerts to be enabled.
#
# Example:
# export PIPELINE_ALERT_EMAIL="your_email@gmail.com"
# export PIPELINE_ALERT_PASSWORD="your_app_password"

SENDER_EMAIL = os.getenv("PIPELINE_ALERT_EMAIL")
SENDER_PASSWORD = os.getenv("PIPELINE_ALERT_PASSWORD")

# By default alerts are sent to the same address
RECIPIENT_EMAIL = SENDER_EMAIL


# ============================================================
# SEND FAILURE EMAIL
# ============================================================

def send_failure_email(error_message):
    """
    Sends an email alert if the pipeline fails.

    Email alerts are optional. If environment variables
    are not configured, the function safely exits without
    interrupting pipeline execution.
    """

    # --------------------------------------------------------
    # ALERTS DISABLED IF CREDENTIALS NOT CONFIGURED
    # --------------------------------------------------------

    if not SENDER_EMAIL or not SENDER_PASSWORD:
        log(
            "WARNING",
            "Email alerts are not configured. "
            "Set PIPELINE_ALERT_EMAIL and PIPELINE_ALERT_PASSWORD "
            "to enable failure notifications."
        )
        return

    try:

        msg = EmailMessage()

        msg["Subject"] = "NBA Feature Store Pipeline Failure"
        msg["From"] = SENDER_EMAIL
        msg["To"] = RECIPIENT_EMAIL

        msg.set_content(
            f"""
NBA Feature Store Pipeline Failure

Error Details:
{error_message}

Please review the pipeline logs for additional information.
"""
        )

        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(SENDER_EMAIL, SENDER_PASSWORD)
            smtp.send_message(msg)

        log("INFO", "Failure alert email sent successfully.")

    except Exception as e:

        log("ERROR", f"Failed to send failure alert email: {e}")