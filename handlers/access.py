"""Заявка на доступ и одобрение/отказ админом."""

from __future__ import annotations

import logging
from contextlib import suppress

from aiogram import Bot, F, Router
from aiogram.types import CallbackQuery, Message

import db
from config import EMAIL_PREFIX, is_admin, panels_configured, require_approval
from formatting import (
    device_subscription_label,
    format_expiry_time_ms,
    sanitize_nick,
)
from keyboards import (
    agreement_keyboard,
    device_selection_keyboard,
    link_keyboard,
)
from services.links import all_links, refresh_sub_config
from services.notifications import notify_admins_new_access_request
from services.subscriptions import (
    create_subscription_for_user,
    panel_base_email,
)
from texts import (
    ACCESS_REQUEST_DUPLICATE,
    ACCESS_REQUEST_SENT,
    LEGAL_REQUIRED_BEFORE_REQUEST,
    NOT_CONFIGURED_TEXT,
    access_granted_text,
    access_rejected_text,
    access_request_intro,
)
from ._helpers import safe_clear_markup

logger = logging.getLogger(__name__)
router = Router()


async def _ensure_can_request(message: Message) -> bool:
    if message.from_user is None:
        await message.answer("Что-то пошло не так. Попробуйте ещё раз.")
        return False
    if not panels_configured():
        logger.error("Не сконфигурирована ни одна панель.")
        await message.answer(NOT_CONFIGURED_TEXT)
        return False
    if await db.get_access_request(message.from_user.id):
        await message.answer(ACCESS_REQUEST_DUPLICATE)
        return False
    return True


@router.message(F.text == "Получить доступ")
async def get_access_msg(message: Message) -> None:
    if not await _ensure_can_request(message):
        return
    assert message.from_user is not None
    tid = message.from_user.id
    if not await db.has_accepted_user_agreement(tid):
        await message.answer(
            LEGAL_REQUIRED_BEFORE_REQUEST,
            reply_markup=agreement_keyboard(),
        )
        return
    await message.answer(
        access_request_intro(),
        reply_markup=device_selection_keyboard(),
    )


@router.callback_query(F.data == "cab:get_access")
async def cb_cabinet_get_access(query: CallbackQuery) -> None:
    await query.answer()
    if query.from_user is None or query.message is None:
        return

    if not panels_configured():
        await query.message.answer(NOT_CONFIGURED_TEXT)
        return

    if await db.get_access_request(query.from_user.id):
        await query.message.answer(ACCESS_REQUEST_DUPLICATE)
        return

    if not await db.has_accepted_user_agreement(query.from_user.id):
        await query.message.answer(
            LEGAL_REQUIRED_BEFORE_REQUEST,
            reply_markup=agreement_keyboard(),
        )
        return

    await query.message.answer(
        access_request_intro(),
        reply_markup=device_selection_keyboard(),
    )


@router.callback_query(F.data.startswith("dev:"))
async def cb_device_chosen(query: CallbackQuery, bot: Bot) -> None:
    if query.from_user is None or query.data is None:
        await query.answer()
        return
    kind = query.data.split(":", 1)[1]
    if kind not in EMAIL_PREFIX:
        await query.answer("Выберите вариант из списка.", show_alert=True)
        return

    tid = query.from_user.id
    if not await db.has_accepted_user_agreement(tid):
        await query.answer(
            "Сначала примите правила сервиса (нажмите «➕ Получить доступ»).",
            show_alert=True,
        )
        return

    if await db.get_access_request(tid):
        await query.answer("Дождитесь ответа по предыдущей заявке.", show_alert=True)
        return

    if not panels_configured():
        await query.answer("Сервис временно недоступен.", show_alert=True)
        return

    nick = sanitize_nick(query.from_user)
    n_same = await db.count_device_slots(tid, kind)
    slot_index = n_same + 1
    base_email = panel_base_email(nick, kind, slot_index)

    if require_approval():
        inserted = await db.try_insert_access_request(
            tid,
            query.from_user.username,
            query.from_user.first_name,
            query.from_user.last_name,
            base_email,
            kind,
            slot_index,
        )
        if not inserted:
            await query.answer("Заявка уже отправлена. Ожидайте.", show_alert=True)
            return
        req = await db.get_access_request(tid)
        if req:
            await notify_admins_new_access_request(bot, req)
        await safe_clear_markup(query)
        if query.message:
            await query.message.answer(ACCESS_REQUEST_SENT)
        await query.answer()
        return

    await refresh_sub_config()
    ok, sub, expiry_time_ms, err = await create_subscription_for_user(
        tid, base_email, kind, slot_index
    )
    if not ok or sub is None or expiry_time_ms is None:
        await query.answer((err or "Ошибка")[:200], show_alert=True)
        return
    await safe_clear_markup(query)
    links = all_links(sub)
    if query.message and links:
        label = device_subscription_label(kind, slot_index)
        await query.message.answer(
            access_granted_text(label, format_expiry_time_ms(expiry_time_ms)),
            reply_markup=link_keyboard(links[0][1], kind, slot_index),
        )
    await query.answer("Готово")


# --- Админ: одобрение/отклонение -----------------------------------------------------


@router.callback_query(F.data.startswith("apr:"))
async def cb_admin_approve(query: CallbackQuery, bot: Bot) -> None:
    if not is_admin(query.from_user.id if query.from_user else None):
        await query.answer("Нет прав.", show_alert=True)
        return
    parts = (query.data or "").split(":")
    try:
        tid = int(parts[1])
    except (IndexError, ValueError):
        await query.answer("Неверные данные.", show_alert=True)
        return
    days: int | None = None
    if len(parts) >= 3 and parts[2]:
        try:
            days = int(parts[2])
        except ValueError:
            days = None

    # «Claim»: атомарно забираем заявку (защита от двойного клика).
    pending = await db.get_access_request(tid)
    if pending is None:
        await query.answer("Заявка уже обработана.", show_alert=True)
        await safe_clear_markup(query)
        return
    if not await db.try_claim_access_request(tid):
        # Между чтением и клеймом заявку забрал другой обработчик.
        await query.answer("Заявка уже обработана.", show_alert=True)
        await safe_clear_markup(query)
        return

    await refresh_sub_config()
    ok, sub, expiry_time_ms, err = await create_subscription_for_user(
        tid,
        pending.base_email,
        pending.device_kind,
        pending.slot_index,
        days=days,
    )
    if not ok or sub is None or expiry_time_ms is None:
        # Откат claim'а: восстановим заявку, чтобы админ мог попробовать ещё раз.
        await db.try_insert_access_request(
            pending.telegram_id,
            pending.username,
            pending.first_name,
            pending.last_name,
            pending.base_email,
            pending.device_kind,
            pending.slot_index,
        )
        await query.answer((err or "Ошибка")[:180], show_alert=True)
        return

    await query.answer(f"Доступ выдан на {days or '—'} дн.")

    links = all_links(sub)
    label = device_subscription_label(pending.device_kind, pending.slot_index)
    delivered = False
    if links:
        try:
            await bot.send_message(
                tid,
                access_granted_text(label, format_expiry_time_ms(expiry_time_ms)),
                reply_markup=link_keyboard(
                    links[0][1], pending.device_kind, pending.slot_index
                ),
            )
            delivered = True
        except Exception:
            logger.exception("Не удалось написать пользователю %s", tid)
    else:
        logger.error(
            "Нет ссылок подписки для tg_id=%s — не настроены панели/портал.", tid
        )

    await safe_clear_markup(query)
    if query.message:
        with suppress(Exception):
            if delivered:
                await query.message.reply(
                    f"✅ Выдано пользователю <code>{tid}</code>."
                )
            else:
                await query.message.reply(
                    f"⚠️ Клиенты для <code>{tid}</code> созданы, "
                    "но в личку не доставлено (нет чата с ботом). Ссылки:\n"
                    + "\n".join(f"• {n}: <code>{l}</code>" for n, l in links),
                )


@router.callback_query(F.data.startswith("rej:"))
async def cb_admin_reject(query: CallbackQuery, bot: Bot) -> None:
    if not is_admin(query.from_user.id if query.from_user else None):
        await query.answer("Нет прав.", show_alert=True)
        return
    try:
        tid = int((query.data or "").split(":", 1)[1])
    except (IndexError, ValueError):
        await query.answer("Неверные данные.", show_alert=True)
        return

    # Атомарный claim: исключаем гонку с одновременным «одобрить» от другого админа.
    if not await db.try_claim_access_request(tid):
        await query.answer("Заявка уже не активна.", show_alert=True)
        await safe_clear_markup(query)
        return
    await query.answer("Отклонено.")

    try:
        await bot.send_message(tid, access_rejected_text())
    except Exception:
        logger.exception("Не удалось уведомить %s об отказе", tid)

    await safe_clear_markup(query)
    if query.message:
        with suppress(Exception):
            await query.message.reply(f"❌ Отказ пользователю <code>{tid}</code>.")
