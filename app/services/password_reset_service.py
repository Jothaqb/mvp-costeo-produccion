from __future__ import annotations

import hashlib
import os
import secrets
from dataclasses import dataclass
from datetime import datetime, timedelta
from urllib.parse import quote

from sqlalchemy import func
from sqlalchemy.orm import Session, joinedload

from app.models import PasswordResetToken, User, UserSession
from app.services.auth_service import hash_password
from app.services.email_service import auth_emails_enabled, send_password_reset_email


class PasswordResetTokenInvalidError(Exception):
    pass


class PasswordResetTokenExpiredError(Exception):
    pass


class PasswordResetTokenUsedError(Exception):
    pass


class PasswordResetEmailDeliveryError(Exception):
    pass


@dataclass(frozen=True)
class PasswordResetRequestResult:
    outcome: str
    email_sent: bool
    user: User | None


@dataclass(frozen=True)
class PasswordResetIssueResult:
    outcome: str
    email_sent: bool


def normalize_reset_email(email: str) -> str:
    return (email or "").strip().lower()


def request_password_reset(
    db: Session,
    *,
    email: str,
    requested_ip: str | None,
    requested_user_agent: str | None,
    app_base_url: str,
) -> PasswordResetRequestResult:
    normalized_email = normalize_reset_email(email)
    if not normalized_email or not _email_appears_valid(normalized_email):
        return PasswordResetRequestResult(outcome="invalid_email", email_sent=False, user=None)

    matching_users = (
        db.query(User)
        .filter(User.email.is_not(None), func.lower(User.email) == normalized_email)
        .order_by(User.id)
        .all()
    )
    if not matching_users:
        return PasswordResetRequestResult(outcome="email_not_found", email_sent=False, user=None)
    if len(matching_users) > 1:
        return PasswordResetRequestResult(outcome="ambiguous_email", email_sent=False, user=None)

    user = matching_users[0]
    if not user.is_active:
        return PasswordResetRequestResult(outcome="user_inactive", email_sent=False, user=user)
    if not user.email or not _email_appears_valid(user.email):
        return PasswordResetRequestResult(outcome="stored_email_invalid", email_sent=False, user=user)

    issue_result = _issue_password_reset_token_for_user(
        db,
        user=user,
        requested_ip=requested_ip,
        requested_user_agent=requested_user_agent,
        app_base_url=app_base_url,
        raise_on_delivery_error=True,
    )
    return PasswordResetRequestResult(outcome=issue_result.outcome, email_sent=issue_result.email_sent, user=user)


def issue_password_setup_for_user(
    db: Session,
    *,
    user: User,
    requested_ip: str | None,
    requested_user_agent: str | None,
    app_base_url: str,
) -> PasswordResetIssueResult:
    if not user.email or not _email_appears_valid(user.email):
        raise ValueError("A valid email is required for password setup.")
    return _issue_password_reset_token_for_user(
        db,
        user=user,
        requested_ip=requested_ip,
        requested_user_agent=requested_user_agent,
        app_base_url=app_base_url,
        raise_on_delivery_error=False,
    )


def validate_password_reset_token(db: Session, raw_token: str) -> PasswordResetToken:
    token_value = (raw_token or "").strip()
    if not token_value:
        raise PasswordResetTokenInvalidError("Password reset token is required.")

    token_record = (
        db.query(PasswordResetToken)
        .options(joinedload(PasswordResetToken.user))
        .filter(PasswordResetToken.token_hash == _hash_reset_token(token_value))
        .one_or_none()
    )
    if token_record is None:
        raise PasswordResetTokenInvalidError("Password reset token is invalid.")
    if token_record.used_at is not None:
        raise PasswordResetTokenUsedError("Password reset token was already used.")
    if token_record.expires_at <= datetime.utcnow():
        raise PasswordResetTokenExpiredError("Password reset token expired.")
    if token_record.user is None or not token_record.user.is_active:
        raise PasswordResetTokenInvalidError("Password reset user is invalid.")
    return token_record


def complete_password_reset(
    db: Session,
    token_record: PasswordResetToken,
    *,
    new_password: str,
    consumed_ip: str | None,
    consumed_user_agent: str | None,
) -> int:
    now = datetime.utcnow()
    user = token_record.user
    if user is None or not user.is_active:
        raise PasswordResetTokenInvalidError("Password reset user is invalid.")

    user.password_hash = hash_password(new_password)
    user.must_change_password = False
    token_record.used_at = now
    token_record.consumed_ip = (consumed_ip or "")[:255] or None
    token_record.consumed_user_agent = (consumed_user_agent or "")[:500] or None
    revoked_count = revoke_all_active_sessions_for_user(db, user.id, revoked_at=now)
    db.flush()
    return revoked_count


def revoke_all_active_sessions_for_user(
    db: Session,
    user_id: int,
    *,
    revoked_at: datetime | None = None,
) -> int:
    timestamp = revoked_at or datetime.utcnow()
    sessions = (
        db.query(UserSession)
        .filter(UserSession.user_id == user_id, UserSession.revoked_at.is_(None))
        .all()
    )
    for session in sessions:
        session.revoked_at = timestamp
    db.flush()
    return len(sessions)


def build_password_reset_link(app_base_url: str, raw_token: str) -> str:
    base_url = (app_base_url or "").strip().rstrip("/")
    return f"{base_url}/auth/reset-password?token={quote(raw_token)}"


def password_reset_token_ttl_minutes() -> int:
    raw_value = os.getenv("PASSWORD_RESET_TOKEN_TTL_MINUTES", "60").strip()
    try:
        ttl = int(raw_value)
    except ValueError:
        ttl = 60
    return max(ttl, 5)


def _invalidate_pending_password_reset_tokens(db: Session, user_id: int, *, used_at: datetime) -> None:
    tokens = (
        db.query(PasswordResetToken)
        .filter(PasswordResetToken.user_id == user_id, PasswordResetToken.used_at.is_(None))
        .all()
    )
    for token in tokens:
        token.used_at = used_at
        if token.consumed_ip is None:
            token.consumed_ip = "superseded"
        if token.consumed_user_agent is None:
            token.consumed_user_agent = "superseded"
    db.flush()


def _hash_reset_token(raw_token: str) -> str:
    return hashlib.sha256((raw_token or "").encode("utf-8")).hexdigest()


def _issue_password_reset_token_for_user(
    db: Session,
    *,
    user: User,
    requested_ip: str | None,
    requested_user_agent: str | None,
    app_base_url: str,
    raise_on_delivery_error: bool,
) -> PasswordResetIssueResult:
    raw_token = secrets.token_urlsafe(32)
    now = datetime.utcnow()
    token_record = PasswordResetToken(
        user_id=user.id,
        token_hash=_hash_reset_token(raw_token),
        created_at=now,
        expires_at=now + timedelta(minutes=password_reset_token_ttl_minutes()),
        used_at=None,
        requested_ip=(requested_ip or "")[:255] or None,
        requested_user_agent=(requested_user_agent or "")[:500] or None,
        consumed_ip=None,
        consumed_user_agent=None,
    )
    reset_link = build_password_reset_link(app_base_url, raw_token)

    _invalidate_pending_password_reset_tokens(db, user.id, used_at=now)
    db.add(token_record)
    db.flush()

    if auth_emails_enabled():
        try:
            send_password_reset_email(
                to_email=user.email or "",
                full_name=user.full_name,
                reset_link=reset_link,
            )
        except Exception as exc:
            if raise_on_delivery_error:
                raise PasswordResetEmailDeliveryError(str(exc)) from exc
            return PasswordResetIssueResult(outcome="email_delivery_failed", email_sent=False)
        return PasswordResetIssueResult(outcome="email_sent", email_sent=True)

    if _should_print_local_reset_link():
        print(f"[auth][local-only] password reset link for {user.email}: {reset_link}")
    return PasswordResetIssueResult(outcome="emails_disabled", email_sent=False)


def _email_appears_valid(email: str) -> bool:
    value = (email or "").strip()
    if not value or "@" not in value:
        return False
    local_part, _, domain_part = value.rpartition("@")
    if not local_part or not domain_part:
        return False
    if domain_part.startswith(".") or domain_part.endswith("."):
        return False
    return "." in domain_part


def _should_print_local_reset_link() -> bool:
    if auth_emails_enabled():
        return False
    environment = os.getenv("ENVIRONMENT", "").strip().lower()
    debug_enabled = os.getenv("DEBUG", "").strip().lower() in {"1", "true", "yes", "on"}
    return environment == "local" or debug_enabled
