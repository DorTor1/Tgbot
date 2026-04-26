"""Уведомления админам о новых заявках и продлениях."""

from __future__ import annotations

import logging

from aiogram import Bot

import db
from config import admin_ids
from formatting import (
    device_subscription_label,
    format_user_name,
    format_expiry_time_ms,
)
from keyboards import access_review_keyboard, renewal_review_keyboard

logger = logging.getLogger(__name__)


async def notify_admins_new_access_request(
    bot: Bot, req: db.AccessRequestRecord
) -> None:
    label = device_subscription_label(req.device_kind, req.slot_index)
    text = (
        "📩 <b>Новая заявка на доступ</b>\n\n"
        f"👤 Кто: {format_user_name(req.username, req.first_name, req.last_name)}\n"
        f"🆔 Telegram ID: <code>{req.telegram_id}</code>\n"
        f"💻 Профиль: {label}\n"
        f"📧 Email-префикс: <code>{req.base_email}</code>\n\n"
        "Свяжитесь с пользователем по кнопке «✉️ Написать», обсудите оплату "
        "и выберите срок подписки. Очередь: /pending"
    )
    kb = access_review_keyboard(req.telegram_id)
    for admin_id in admin_ids():
        try:
            await bot.send_message(admin_id, text, reply_markup=kb)
        except Exception:
            logger.exception("Не удалось отправить уведомление админу %s", admin_id)


async def notify_admins_renewal_request(
    bot: Bot, req: db.RenewalRequestRecord
) -> None:
    label = device_subscription_label(req.device_kind, req.slot_index)
    text = (
        "🔁 <b>Запрос на продление</b>\n\n"
        f"👤 Кто: {format_user_name(req.username, req.first_name, req.last_name)}\n"
        f"🆔 Telegram ID: <code>{req.telegram_id}</code>\n"
        f"💻 Профиль: {label}\n"
        f"📅 Текущий срок: {format_expiry_time_ms(req.current_expiry_time_ms)}\n\n"
        "Свяжитесь с пользователем и выберите срок продления.\n"
        "Очередь: /pending"
    )
    kb = renewal_review_keyboard(req.telegram_id, req.device_kind, req.slot_index)
    for admin_id in admin_ids():
        try:
            await bot.send_message(admin_id, text, reply_markup=kb)
        except Exception:
            logger.exception(
                "Не удалось отправить заявку на продление админу %s", admin_id
            )
