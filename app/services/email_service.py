from __future__ import annotations

import os
import smtplib
import ssl
from email.message import EmailMessage


def auth_emails_enabled() -> bool:
    return _env_bool("AUTH_EMAILS_ENABLED", default=False)


def send_password_reset_email(*, to_email: str, full_name: str | None, reset_link: str) -> None:
    if not auth_emails_enabled():
        return

    host = os.getenv("SMTP_HOST", "").strip()
    port = _env_int("SMTP_PORT", 0)
    username = os.getenv("SMTP_USERNAME", "").strip()
    password = os.getenv("SMTP_PASSWORD", "")
    use_tls = _env_bool("SMTP_USE_TLS", default=True)
    from_email = os.getenv("SMTP_FROM_EMAIL", "").strip()
    from_name = os.getenv("SMTP_FROM_NAME", "").strip() or "Green Corner ERP"

    if not host or not port or not from_email:
        raise RuntimeError("SMTP configuration is incomplete.")

    message = EmailMessage()
    message["Subject"] = "Reset your Green Corner ERP password"
    message["From"] = f"{from_name} <{from_email}>"
    message["To"] = to_email
    greeting = full_name.strip() if full_name else "there"
    message.set_content(
        "\n".join(
            [
                f"Hello {greeting},",
                "",
                "We received a request to reset your Green Corner ERP password.",
                "Use the link below to choose a new password:",
                reset_link,
                "",
                "If you did not request this change, you can ignore this email.",
            ]
        )
    )

    if use_tls:
        with smtplib.SMTP(host, port, timeout=30) as smtp:
            smtp.ehlo()
            smtp.starttls(context=ssl.create_default_context())
            smtp.ehlo()
            if username and password:
                smtp.login(username, password)
            smtp.send_message(message)
        return

    if port == 465:
        with smtplib.SMTP_SSL(host, port, timeout=30, context=ssl.create_default_context()) as smtp:
            if username and password:
                smtp.login(username, password)
            smtp.send_message(message)
        return

    with smtplib.SMTP(host, port, timeout=30) as smtp:
        smtp.ehlo()
        if username and password:
            smtp.login(username, password)
        smtp.send_message(message)


def _env_bool(name: str, *, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw.strip())
    except (TypeError, ValueError):
        return default
