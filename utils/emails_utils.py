"""
utils/email_utils.py

PURPOSE
-------
A reusable email sending utility for the Transit Radar 411 pipeline.
Any script in the project can import and call send_email() to send
notifications — download progress, pipeline failures, DAG completions,
model retraining results etc.

Uses Gmail SMTP with an App Password for authentication.
App Passwords are separate from Gmail login password and can
be generated at: https://myaccount.google.com/apppasswords

CREDENTIALS
-----------
Reads from .env file — never hardcoded:
    EMAIL_SENDER       = Gmail address
    EMAIL_APP_PASSWORD = 16-character Gmail app password
    EMAIL_RECIPIENT    = the address to send notifications to
                         (defaults to EMAIL_SENDER if not set)

USAGE
-----
From any script in the project:

    from utils.email_utils import send_email

    send_email(
        subject="Download complete",
        body="flightlist_20200101 downloaded successfully."
    )

    # Send to a different recipient
    send_email(
        subject="Pipeline error",
        body="Silver layer job failed at 23:45 UTC.",
        to="alerts@yourteam.com"
    )
"""

import os
import smtplib
import ssl
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# CONFIGURATION — read from .env
# ─────────────────────────────────────────────────────────────
SMTP_HOST      = "smtp.gmail.com"
SMTP_PORT_TLS  = 587                          # TLS port for Gmail
SMTP_PORT_SSL = 465
EMAIL_SENDER   = os.getenv("EMAIL_SENDER")
EMAIL_PASSWORD = os.getenv("EMAIL_APP_PASSWORD")
EMAIL_RECIPIENT = os.getenv(
    "EMAIL_RECIPIENT",
    EMAIL_SENDER                              # Default: send to yourself
)


def send_email(subject: str, body: str, to: str = None) -> bool:
    """
    Sends a plain text email via Gmail SMTP.

    Parameters
    ----------
    subject : str
        The email subject line.
        Example: "Download complete — flightlist_20200101"

    body : str
        The email body text.
        Example: "File 1 of 48 downloaded successfully at 14:23 UTC."

    to : str, optional
        The recipient email address.
        Defaults to EMAIL_RECIPIENT from .env (which defaults to
        EMAIL_SENDER if EMAIL_RECIPIENT is not set).

    Returns
    -------
    bool
        True if the email was sent successfully.
        False if sending failed — logs the error but does not raise
        an exception so the calling script can continue running.

    Why return bool instead of raising?
    ------------------------------------
    Email sending is a notification — it should never crash the
    pipeline. If Gmail is temporarily unavailable or the app password
    is wrong, the download should still continue. The caller decides
    whether to act on the return value.
    """
    if not EMAIL_SENDER or not EMAIL_PASSWORD:
        log.warning(
            "EMAIL_SENDER or EMAIL_APP_PASSWORD not set in .env. "
            "Email notification skipped."
        )
        return False

    recipient = to or EMAIL_RECIPIENT or EMAIL_SENDER

    # Add a timestamp to every email body automatically
    # so you know exactly when each event happened
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    full_body  = f"{body}\n\n---\nSent by Transit Radar 411 at {timestamp}"

    # Build the email message
    # MIMEMultipart allows us to send both plain text and HTML
    # We use plain text only — clean and simple
    message                    = MIMEMultipart()
    message["From"]            = EMAIL_SENDER
    message["To"]              = recipient
    message["Subject"]         = subject
    message.attach(MIMEText(full_body, "plain"))

    try:
        try:
            # Connect to Gmail SMTP server
            # SMTP_SSL uses port 465, starttls uses port 587
            # We use port 587 with starttls — the recommended approach
            with smtplib.SMTP(SMTP_HOST, SMTP_PORT_TLS) as server:

                # starttls() upgrades the connection to TLS encryption
                # Must be called before login() to protect credentials
                server.starttls()

                # Log in with your Gmail address and App Password
                # App Password is NOT your Gmail login password —
                # it is a 16-character code generated specifically
                # for programmatic access
                server.login(EMAIL_SENDER, EMAIL_PASSWORD)

                # Send the email
                server.sendmail(EMAIL_SENDER,recipient,message.as_string())

            log.info(f"Email sent: '{subject}' → {recipient}")
            return True
        except (smtplib.SMTPException, OSError) as e:
            log.warning(f"STARTTLS failed, trying SSL fallback: {e}")
            # Attempt 2: SSL on port 465
            #context = ssl._create_unverified_context()
            context = ssl.create_default_context()
            with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT_SSL, context=context, timeout=30) as server:
                server.login(EMAIL_SENDER, EMAIL_PASSWORD)
                server.sendmail(EMAIL_SENDER, recipient, message.as_string())
            log.info(f"Email sent via SSL: '{subject}' → {recipient}")
            return True

    except smtplib.SMTPAuthenticationError:
        log.error(
            "Gmail authentication failed. "
            "Check EMAIL_SENDER and EMAIL_APP_PASSWORD in your .env file. "
            "Make sure you are using an App Password, not your Gmail login password."
        )
        return False

    except smtplib.SMTPException as e:
        log.error(f"SMTP error sending email: {e}")
        return False

    except Exception as e:
        log.error(f"Unexpected error sending email: {e}")
        return False


def send_success_email(filename: str, file_number: int, total_files: int,
                       file_size_mb: float) -> bool:
    """
    Convenience function for a successful file download notification.

    Parameters
    ----------
    filename     : The name of the downloaded file
    file_number  : Which file this is in the sequence (e.g. 3)
    total_files  : Total number of files to download (e.g. 48)
    file_size_mb : Size of the downloaded file in megabytes
    """
    subject = (
        f"Transit Radar 411 — Download {file_number}/{total_files} complete"
    )
    body = (
        f"File downloaded successfully.\n\n"
        f"  File     : {filename}\n"
        f"  Progress : {file_number} of {total_files} files\n"
        f"  Size     : {file_size_mb:.1f} MB\n"
        f"  Remaining: {total_files - file_number} files"
    )
    return send_email(subject, body)


def send_error_email(filename: str, file_number: int, total_files: int,
                     error: str, attempt: int, max_attempts: int) -> bool:
    """
    Convenience function for a failed download notification.

    Parameters
    ----------
    filename     : The name of the file that failed
    file_number  : Which file this is in the sequence
    total_files  : Total number of files
    error        : The error message
    attempt      : Which retry attempt this was (1, 2, or 3)
    max_attempts : Maximum number of attempts before giving up
    """
    subject = (
        f"Transit Radar 411 — Download ERROR (file {file_number}/{total_files})"
    )
    body = (
        f"A download failed.\n\n"
        f"  File     : {filename}\n"
        f"  Progress : {file_number} of {total_files} files\n"
        f"  Attempt  : {attempt} of {max_attempts}\n"
        f"  Error    : {error}\n\n"
        f"The script will {'retry' if attempt < max_attempts else 'skip this file and continue'}."
    )
    return send_email(subject, body)


def send_completion_email(total_files: int, successful: int,
                          failed: int, duration_minutes: float) -> bool:
    """
    Convenience function for the final completion notification
    sent after all files have been processed.

    Parameters
    ----------
    total_files      : Total number of files attempted
    successful       : Number of files downloaded successfully
    failed           : Number of files that failed after all retries
    duration_minutes : Total time taken in minutes
    """
    subject = "Transit Radar 411 — All downloads complete"

    if failed == 0:
        status = "All files downloaded successfully."
    else:
        status = f"WARNING: {failed} file(s) failed after all retry attempts."

    body = (
        f"{status}\n\n"
        f"  Total files : {total_files}\n"
        f"  Successful  : {successful}\n"
        f"  Failed      : {failed}\n"
        f"  Duration    : {duration_minutes:.1f} minutes\n\n"
        f"Next step: run seeds/load_historical_states.py to process "
        f"the downloaded files and load hist_flight_events."
    )
    return send_email(subject, body)