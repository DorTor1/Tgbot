"""Заявки на продление и одобрение/отказ админом."""

from __future__ import annotations

import logging
from contextlib import suppress

from aiogram import Bot, F, Router
from aiogram.types import CallbackQuery

import db
from config import is_admin
from formatting import device_subscription_label, format_expiry_time_ms
from services.notifications import notify_admins_renewal_request
from services.subscriptions import extend_subscription_for_user
from texts import (
    RENEWAL_REQUEST_DUPLICATE,
    RENEWAL_REQUEST_SENT,
    renewal_done_text,
    renewal_rejected_text,
)
from ._helpers import safe_clear_markup

logger = logging.getLogger(__name__)
router = Router()


@router.callback_query(F.data.startswith("rnw_req:"))
async def cb_renewal_request(query: CallbackQuery, bot: Bot) -> None:
    """Пользователь нажал «🔁 Продлить» под подпиской/напоминанием."""
    if query.from_user is None or not query.data:
        await query.answer()
        return
    parts = query.data.split(":")
    if len(parts) != 3:
        await query.answer("Некорректные данные.", show_alert=True)
        return
    _, device_kind, slot_raw = parts
    try:
        slot_index = int(slot_raw)
    except ValueError:
        await query.answer("Некорректные данные.", show_alert=True)
        return

    tid = query.from_user.id
    device = await db.get_user_device(tid, device_kind, slot_index)
    if device is None:
        await query.answer("Подписка не найдена.", show_alert=True)
        return

    inserted = await db.try_insert_renewal_request(
        tid,
        device_kind,
        slot_index,
        query.from_user.username,
        query.from_user.first_name,
        query.from_user.last_name,
    )
    if not inserted:
        await query.answer(RENEWAL_REQUEST_DUPLICATE, show_alert=True)
        return

    req = await db.get_renewal_request(tid, device_kind, slot_index)
    if req is not None:
        await notify_admins_renewal_request(bot, req)

    await query.answer()
    if query.message:
        await query.message.answer(RENEWAL_REQUEST_SENT)


@router.callback_query(F.data.startswith("rnw_apr:"))
async def cb_renewal_approve(query: CallbackQuery, bot: Bot) -> None:
    if not is_admin(query.from_user.id if query.from_user else None):
        await query.answer("Нет прав.", show_alert=True)
        return
    parts = (query.data or "").split(":")
    if len(parts) != 5:
        await query.answer("Некорректные данные.", show_alert=True)
        return
    try:
        tid = int(parts[1])
        device_kind = parts[2]
        slot_index = int(parts[3])
        days = int(parts[4])
    except ValueError:
        await query.answer("Некорректные данные.", show_alert=True)
        return

    pending = await db.get_renewal_request(tid, device_kind, slot_index)
    if pending is None:
        await query.answer("Заявка уже обработана или отозвана.", show_alert=True)
        await safe_clear_markup(query)
        return

    # Атомарный claim до выполнения операции — защита от двойного клика.
    if not await db.try_claim_renewal_request(tid, device_kind, slot_index):
        await query.answer("Заявка уже обработана или отозвана.", show_alert=True)
        await safe_clear_markup(query)
        return

    ok, new_expiry_ms, err = await extend_subscription_for_user(
        tid, device_kind, slot_index, days
    )
    if not ok or new_expiry_ms is None:
        # Восстановим заявку, чтобы админ мог попробовать ещё раз.
        await db.try_insert_renewal_request(
            pending.telegram_id,
            pending.device_kind,
            pending.slot_index,
            pending.username,
            pending.first_name,
            pending.last_name,
        )
        await query.answer((err or "Ошибка")[:180], show_alert=True)
        return

    await query.answer(f"Продлено на {days} дн.")

    label = device_subscription_label(device_kind, slot_index)
    try:
        await bot.send_message(
            tid,
            renewal_done_text(label, format_expiry_time_ms(new_expiry_ms)),
        )
    except Exception:
        logger.exception("Не удалось уведомить %s о продлении", tid)

    await safe_clear_markup(query)
    if query.message:
        with suppress(Exception):
            await query.message.reply(
                f"✅ Продлено <code>{tid}</code> на {days} дн. "
                f"до {format_expiry_time_ms(new_expiry_ms)}."
            )


@router.callback_query(F.data.startswith("rnw_rej:"))
async def cb_renewal_reject(query: CallbackQuery, bot: Bot) -> None:
    if not is_admin(query.from_user.id if query.from_user else None):
        await query.answer("Нет прав.", show_alert=True)
        return
    parts = (query.data or "").split(":")
    if len(parts) != 4:
        await query.answer("Некорректные данные.", show_alert=True)
        return
    try:
        tid = int(parts[1])
        device_kind = parts[2]
        slot_index = int(parts[3])
    except ValueError:
        await query.answer("Некорректные данные.", show_alert=True)
        return

    # Атомарный claim: исключаем гонку с одновременным «одобрить» от другого админа.
    if not await db.try_claim_renewal_request(tid, device_kind, slot_index):
        await query.answer("Заявка уже не активна.", show_alert=True)
        await safe_clear_markup(query)
        return
    await query.answer("Отклонено.")

    label = device_subscription_label(device_kind, slot_index)
    try:
        await bot.send_message(tid, renewal_rejected_text(label))
    except Exception:
        logger.exception("Не удалось уведомить %s об отказе в продлении", tid)

    await safe_clear_markup(query)
    if query.message:
        with suppress(Exception):
            await query.message.reply(
                f"❌ Отказ в продлении <code>{tid}</code> ({label})."
            )
