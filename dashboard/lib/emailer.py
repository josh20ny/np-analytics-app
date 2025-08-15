# dashboard/lib/emailer.py
import os, requests
from email.utils import parseaddr
from dotenv import load_dotenv

load_dotenv()  # ensure .env is loaded even if caller forgets

EMAIL_FROM = os.getenv("EMAIL_FROM", "NP Analytics <no-reply@example.com>")
REPLY_TO   = os.getenv("REPLY_TO")
TIMEOUT_S  = int(os.getenv("EMAIL_TIMEOUT_SECONDS", "15"))

def _split_name_email(s: str):
    name, email = parseaddr(s or "")
    return (name or None), (email or "no-reply@example.com")

def _sendgrid_send(to: str, subject: str, body: str, html: str | None = None):
    key = os.getenv("SENDGRID_API_KEY")
    if not key:
        raise RuntimeError("SENDGRID_API_KEY is not set (required for SendGrid backend)")

    name, email = _split_name_email(EMAIL_FROM)
    payload = {
        "personalizations": [{"to": [{"email": to}]}],
        "from": {"email": email, "name": name},
        "subject": subject,
        "content": [{"type": "text/plain", "value": body or ""}],
    }
    if html:
        payload["content"].append({"type": "text/html", "value": html})
    if REPLY_TO:
        rn, re = _split_name_email(REPLY_TO)
        payload["reply_to"] = {"email": re, "name": rn}

    r = requests.post(
        "https://api.sendgrid.com/v3/mail/send",
        headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
        json=payload,
        timeout=TIMEOUT_S,
    )
    if r.status_code >= 300:
        raise RuntimeError(f"SendGrid error {r.status_code}: {r.text}")
    return True

def _console_send(to: str, subject: str, body: str, html: str | None = None):
    print("\n— EMAIL (console) —")
    print(f"From: {EMAIL_FROM}\nTo: {to}\nSubject: {subject}")
    print("Text:", body or "")
    if html: print("HTML:", html)
    print("— END —\n")
    return True

def send_email(to: str, subject: str, body: str, html: str | None = None):
    backend = os.getenv("EMAIL_BACKEND", "sendgrid").lower()  # default to sendgrid
    if backend == "console":
        return _console_send(to, subject, body, html)
    if backend == "sendgrid":
        return _sendgrid_send(to, subject, body, html)
    raise ValueError(f"Unknown EMAIL_BACKEND={backend} (use 'sendgrid' or 'console')")

