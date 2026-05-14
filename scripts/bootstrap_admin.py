from __future__ import annotations

import argparse
import getpass
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import app.models  # noqa: F401
from app.database import Base, SessionLocal, engine
from app.services.audit_service import safe_log_audit_event
from app.services.auth_service import any_active_users, bootstrap_admin_user, ensure_auth_seed_state


MIN_PASSWORD_LENGTH = 10


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Create the first ERP admin user if no active users exist."
    )
    parser.add_argument("--username", help="Username for the first admin user.")
    parser.add_argument("--full-name", dest="full_name", help="Full name for the first admin user.")
    parser.add_argument("--email", help="Optional email for the first admin user.")
    return parser


def _prompt_username(provided_username: str | None) -> str:
    username = (provided_username or "").strip()
    if username:
        return username
    return input("Admin username: ").strip()


def _resolve_full_name(username: str, provided_full_name: str | None) -> str:
    full_name = (provided_full_name or "").strip()
    if full_name:
        return full_name
    if username.lower() == "admin":
        return "ERP Admin"
    return username


def _prompt_password() -> str:
    password = getpass.getpass("Admin password: ")
    password_confirm = getpass.getpass("Confirm password: ")
    if not password.strip():
        raise ValueError("Password is required.")
    if len(password) < MIN_PASSWORD_LENGTH:
        raise ValueError(f"Password must be at least {MIN_PASSWORD_LENGTH} characters long.")
    if password != password_confirm:
        raise ValueError("Password confirmation does not match.")
    return password


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    db = SessionLocal()
    try:
        Base.metadata.create_all(bind=engine)
        ensure_auth_seed_state(db)

        if any_active_users(db):
            db.rollback()
            print("Bootstrap aborted: active users already exist.")
            return 1

        username = _prompt_username(args.username)
        if not username:
            raise ValueError("Username is required.")

        password = _prompt_password()
        full_name = _resolve_full_name(username, args.full_name)
        user = bootstrap_admin_user(
            db,
            username=username,
            full_name=full_name,
            email=args.email,
            password=password,
        )
        db.commit()

        safe_log_audit_event(
            module="auth",
            action="bootstrap_admin_created",
            entity_type="user",
            entity_id=user.id,
            entity_label=user.username,
            notes="Bootstrap admin user created from CLI.",
            request=None,
            username="system",
        )

        print(f"Bootstrap admin created successfully for username '{user.username}'.")
        return 0
    except ValueError as exc:
        db.rollback()
        print(f"Bootstrap failed: {exc}")
        return 1
    except KeyboardInterrupt:
        db.rollback()
        print("Bootstrap cancelled.")
        return 1
    except Exception:
        db.rollback()
        print("Bootstrap failed. Check database connectivity and configuration.")
        return 1
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())
