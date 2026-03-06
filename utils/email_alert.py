# ============================================================
# EMAIL ALERT UTILITY
# ============================================================

import os
import smtplib
from email.message import EmailMessage


# ------------------------------------------------------------
# LOAD EMAIL CREDENTIALS FROM ENVIRONMENT VARIABLES
# ------------------------------------------------------------

SENDER_EMAIL = os.getenv("PIPELINE_ALERT_EMAIL")
SENDER_PASSWORD = os.getenv("PIPELINE_ALERT_PASSWORD")

# For now we send alerts to the same address
RECIPIENT_EMAIL = SENDER_EMAIL


# ============================================================
# SEND FAILURE EMAIL
# ============================================================

def send_failure_email(error_message):

    msg = EmailMessage()

    msg["Subject"] = "NBA Feature Store Pipeline Failure"
    msg["From"] = SENDER_EMAIL
    msg["To"] = RECIPIENT_EMAIL

    msg.set_content(
        f"""
NBA Feature Store Pipeline Failure

Error Details:
{error_message}

Please check the pipeline logs.
"""
    )

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
        smtp.login(SENDER_EMAIL, SENDER_PASSWORD)
        smtp.send_message(msg)