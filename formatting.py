"""Формат-хелперы: имена, статус подписки, человеко-читаемые даты."""

from __future__ import annotations

import html
import re
from datetime import datetime, timezone

from aiogram.types import User

from config import DEVICE_EMOJI, DEVICE_LABEL_RU, MSK_TZ


def sanitize_nick(user: User | None) -> str:
    """Безопасный nick для email-префикса в панели (a-z0-9_, до 40 символов)."""
    if user is None:
        return "user_unknown"
    if user.username:
        raw = user.username.lower()
    else:
        raw = (user.first_name or "user").replace(" ", "_").lower()
    cleaned = re.sub(r"[^a-z0-9_]", "", raw)
    if not cleaned:
        cleaned = f"u{user.id}"
    return cleaned[:40]


def device_label(kind: str) -> str:
    return DEVICE_LABEL_RU.get(kind, kind)


def device_emoji(kind: str) -> str:
    return DEVICE_EMOJI.get(kind, "🔹")


def device_subscription_label(device_kind: str, slot_index: int) -> str:
    """Например: «📱 Смартфон» или «📱 Смартфон (2)» для второго телефона."""
    label = f"{device_emoji(device_kind)} {device_label(device_kind)}"
    if slot_index > 1:
        label += f" ({slot_index})"
    return label


def format_user_name(
    username: str | None,
    first_name: str | None,
    last_name: str | None,
) -> str:
    parts: list[str] = []
    if username:
        parts.append(f"@{html.escape(username)}")
    name = " ".join(x for x in (first_name or "", last_name or "") if x).strip()
    if name:
        parts.append(html.escape(name))
    return " / ".join(parts) if parts else "без имени"


def greeting_name(user: User | None) -> str:
    if user is None:
        return "друг"
    if user.first_name:
        return user.first_name.strip() or "друг"
    if user.username:
        return f"@{user.username}"
    return "друг"


def format_created_at(created_at: str | None) -> str:
    return created_at or "неизвестно"


def format_expiry_time_ms(expiry_time_ms: int | None) -> str:
    if expiry_time_ms is None:
        return "неизвестно"
    expiry = datetime.fromtimestamp(expiry_time_ms / 1000, tz=timezone.utc)
    expiry = expiry.astimezone(MSK_TZ)
    return expiry.strftime("%d.%m.%Y")


def subscription_status(expiry_time_ms: int | None) -> tuple[str, str]:
    """Возвращает (иконка статуса, человекочитаемый статус)."""
    if expiry_time_ms is None:
        return "⚪️", "срок неизвестен"
    remaining_ms = expiry_time_ms - int(datetime.now(timezone.utc).timestamp() * 1000)
    if remaining_ms <= 0:
        return "🔴", "истекла"
    day_ms = 24 * 60 * 60 * 1000
    days_left = max(1, (remaining_ms + day_ms - 1) // day_ms)
    if days_left <= 3:
        return "🟠", f"осталось {days_left} дн."
    if days_left <= 7:
        return "🟡", f"осталось {days_left} дн."
    return "🟢", f"осталось {days_left} дн."
