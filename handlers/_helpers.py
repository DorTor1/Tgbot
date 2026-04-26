"""Мелкие хелперы для хендлеров."""

from __future__ import annotations

import html
import logging
from contextlib import suppress

import db
from aiogram.types import CallbackQuery, Message, User

from formatting import (
    device_subscription_label,
    format_expiry_time_ms,
    greeting_name,
    subscription_status,
)
from texts import (
    ACCESS_REQUEST_PENDING,
    cabinet_no_devices_hint,
)

logger = logging.getLogger(__name__)


async def safe_clear_markup(query: CallbackQuery) -> None:
    if query.message:
        with suppress(Exception):
            await query.message.edit_reply_markup(reply_markup=None)


async def safe_delete(query: CallbackQuery) -> None:
    if query.message:
        with suppress(Exception):
            await query.message.delete()


async def cabinet_view(user: User | None) -> tuple[str, bool, bool]:
    """Текст кабинета и (has_devices, has_pending_request).

    Кабинет — единая компактная карточка с приветствием, статусом и подсказкой.
    """
    if user is None:
        return "Не удалось открыть кабинет. Попробуйте ещё раз.", False, False

    devices = await db.list_user_devices(user.id)
    pending = await db.get_access_request(user.id)
    name = greeting_name(user)

    lines: list[str] = [
        f"👋 <b>{html.escape(name)}, ваш личный кабинет</b>",
        "",
    ]

    if pending:
        lines.append(ACCESS_REQUEST_PENDING)
        lines.append("")

    if not devices:
        lines.append(cabinet_no_devices_hint())
        return "\n".join(lines), False, pending is not None

    active = sum(1 for d in devices if d.expiry_time_ms and (d.expiry_time_ms > _now_ms()))
    expired = len(devices) - active

    lines.append(f"🔐 <b>Подписки: {len(devices)}</b>")
    for i, d in enumerate(devices, start=1):
        icon, status = subscription_status(d.expiry_time_ms)
        label = device_subscription_label(d.device_kind, d.slot_index)
        lines.append(
            f"  {i}. {icon} {label} — до {format_expiry_time_ms(d.expiry_time_ms)} · {status}"
        )

    lines.append("")
    lines.append(f"Активных: <b>{active}</b> · Истекших: <b>{expired}</b>")
    lines.append("Нажмите «🔗 Открыть подписки», чтобы получить ссылки и инструкции.")
    return "\n".join(lines), True, pending is not None


def _now_ms() -> int:
    from datetime import datetime, timezone

    return int(datetime.now(timezone.utc).timestamp() * 1000)


async def reply_or_edit(
    message: Message, text: str, reply_markup=None
) -> None:
    """Если возможно — отредактируем текущее сообщение, иначе пришлём новое."""
    try:
        await message.edit_text(text, reply_markup=reply_markup)
        return
    except Exception:
        pass
    await message.answer(text, reply_markup=reply_markup)
