from __future__ import annotations

import json
from dataclasses import asdict, is_dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from fastapi import Request

from app.database import SessionLocal
from app.models import AuditLog, User


def _safe_json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if is_dataclass(value):
        return asdict(value)
    return str(value)


def serialize_audit_payload(payload: Any) -> str | None:
    if payload is None:
        return None
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, default=_safe_json_default)


def _request_ip_address(request: Request | None) -> str | None:
    if request is None:
        return None
    forwarded_for = (request.headers.get("x-forwarded-for") or "").strip()
    if forwarded_for:
        first_ip = forwarded_for.split(",")[0].strip()
        if first_ip:
            return first_ip[:255]
    if request.client is not None and request.client.host:
        return request.client.host[:255]
    return None


def _resolve_user_context(
    request: Request | None,
    user: User | None,
    username: str | None,
) -> tuple[int | None, str]:
    resolved_user = user
    if resolved_user is None and request is not None:
        resolved_user = getattr(request.state, "current_user", None)
    if resolved_user is not None:
        return resolved_user.id, resolved_user.username
    if username is not None and username.strip():
        return None, username.strip()[:100]
    return None, "anonymous"


def safe_log_audit_event(
    *,
    module: str,
    action: str,
    entity_type: str | None = None,
    entity_id: str | int | None = None,
    entity_label: str | None = None,
    old_values: Any = None,
    new_values: Any = None,
    notes: str | None = None,
    request: Request | None = None,
    user: User | None = None,
    username: str | None = None,
) -> None:
    db = None
    try:
        db = SessionLocal()
        resolved_user_id, resolved_username = _resolve_user_context(request, user, username)
        entry = AuditLog(
            user_id=resolved_user_id,
            username=resolved_username,
            module=(module or "").strip()[:100] or "unknown",
            action=(action or "").strip()[:100] or "unknown",
            entity_type=(entity_type or "").strip()[:100] or None,
            entity_id=None if entity_id is None else str(entity_id)[:100],
            entity_label=(entity_label or "").strip()[:255] or None,
            old_values=serialize_audit_payload(old_values),
            new_values=serialize_audit_payload(new_values),
            request_path=(request.url.path[:500] if request is not None else None),
            method=((request.method or "").strip()[:20] if request is not None else None) or None,
            ip_address=_request_ip_address(request),
            notes=(notes or "").strip() or None,
        )
        db.add(entry)
        db.commit()
    except Exception as exc:
        if db is not None:
            db.rollback()
        print(f"[audit] failed to log event {module}.{action}: {exc}")
    finally:
        if db is not None:
            db.close()
