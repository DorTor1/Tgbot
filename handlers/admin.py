"""Админ-панель: дашборд, очередь, пользователи, статистика, /reset_legal."""

from __future__ import annotations

import asyncio
import html
import logging

from aiogram import Bot, F, Router
from aiogram.filters import Command
from aiogram.types import CallbackQuery, Message

import db
from config import PANELS, is_admin
from formatting import (
    device_subscription_label,
    format_created_at,
    format_expiry_time_ms,
    format_user_name,
)
from keyboards import (
    access_review_keyboard,
    admin_dashboard_keyboard,
    agreement_keyboard,
    renewal_review_keyboard,
)
from texts import LEGAL_BROADCAST
from vpn_legal import LEGAL_TEXT

logger = logging.getLogger(__name__)
router = Router()


async def _dashboard_text() -> str:
    subscribers = await db.count_distinct_subscribers()
    devices = await db.count_devices()
    pending_access = await db.count_pending_requests()
    pending_renewals = await db.count_pending_renewals()
    panels = ", ".join(p.name for p in PANELS) if PANELS else "не настроены"
    return (
        "🛠 <b>Админ-панель Vibecode VPN</b>\n\n"
        "<b>📊 Статистика</b>\n"
        f"• Пользователей: <b>{subscribers}</b>\n"
        f"• Активных устройств: <b>{devices}</b>\n"
        f"• Новых заявок: <b>{pending_access}</b>\n"
        f"• Заявок на продление: <b>{pending_renewals}</b>\n"
        f"• Панели: {html.escape(panels)}\n\n"
        "<b>⌨️ Команды</b>\n"
        "• /pending — очередь заявок с кнопками\n"
        "• /admin_users — пользователи и статус документов\n"
        "• /user_info <id> — подписки пользователя\n"
        "• /reset_legal confirm — запросить повторное принятие документов\n"
        "• /stats — краткая статистика"
    )


@router.message(Command("admin"))
@router.message(F.text == "Админ-панель")
async def admin_dashboard(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.answer("Команда только для администратора.")
        return
    await message.answer(
        await _dashboard_text(),
        reply_markup=admin_dashboard_keyboard(),
    )


@router.callback_query(F.data == "adm:stats")
async def cb_adm_stats(query: CallbackQuery) -> None:
    if not is_admin(query.from_user.id if query.from_user else None):
        await query.answer("Нет прав.", show_alert=True)
        return
    await query.answer()
    if query.message:
        await query.message.answer(await _dashboard_text())


@router.callback_query(F.data == "adm:pending")
async def cb_adm_pending(query: CallbackQuery) -> None:
    if not is_admin(query.from_user.id if query.from_user else None):
        await query.answer("Нет прав.", show_alert=True)
        return
    await query.answer()
    if query.message:
        await _send_pending_queue(query.message)


@router.callback_query(F.data == "adm:users")
async def cb_adm_users(query: CallbackQuery) -> None:
    if not is_admin(query.from_user.id if query.from_user else None):
        await query.answer("Нет прав.", show_alert=True)
        return
    await query.answer()
    if query.message:
        await _send_users_list(query.message)


@router.message(Command("pending"))
async def cmd_pending(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.answer("Команда только для администратора.")
        return
    await _send_pending_queue(message)


async def _send_pending_queue(message: Message) -> None:
    access_requests = await db.list_pending_access_requests()
    renewal_requests = await db.list_pending_renewal_requests()
    if not access_requests and not renewal_requests:
        await message.answer("✨ Очередь пуста: нет заявок на доступ или продление.")
        return

    await message.answer(
        "📋 <b>Очередь заявок</b>\n\n"
        f"📩 Новые: <b>{len(access_requests)}</b>\n"
        f"🔁 Продления: <b>{len(renewal_requests)}</b>\n\n"
        "Ниже каждая заявка отдельным сообщением с кнопками."
    )

    for req in access_requests:
        text = (
            "📩 <b>Новая заявка</b>\n\n"
            f"👤 {format_user_name(req.username, req.first_name, req.last_name)}\n"
            f"🆔 <code>{req.telegram_id}</code>\n"
            f"💻 {device_subscription_label(req.device_kind, req.slot_index)}\n"
            f"📧 <code>{req.base_email}</code>\n"
            f"🕐 {format_created_at(req.created_at)}\n\n"
            "Свяжитесь с пользователем и выберите срок:"
        )
        await message.answer(text, reply_markup=access_review_keyboard(req.telegram_id))

    for req in renewal_requests:
        text = (
            "🔁 <b>Продление</b>\n\n"
            f"👤 {format_user_name(req.username, req.first_name, req.last_name)}\n"
            f"🆔 <code>{req.telegram_id}</code>\n"
            f"💻 {device_subscription_label(req.device_kind, req.slot_index)}\n"
            f"📅 Текущий срок: {format_expiry_time_ms(req.current_expiry_time_ms)}\n"
            f"🕐 {format_created_at(req.created_at)}\n\n"
            "Свяжитесь с пользователем и выберите срок продления:"
        )
        await message.answer(
            text,
            reply_markup=renewal_review_keyboard(
                req.telegram_id,
                req.device_kind,
                req.slot_index,
            ),
        )


@router.message(Command("admin_users"))
async def cmd_admin_users(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        return
    await _send_users_list(message)


async def _send_users_list(message: Message) -> None:
    users = await db.list_users_legal_status()
    if not users:
        await message.answer("Пользователей пока нет.")
        return

    users = sorted(
        users,
        key=lambda u: (
            int(u["accepted"]),
            -int(u["devices"]),
            int(u["tid"]),
        ),
    )
    total = len(users)
    signed = sum(1 for u in users if u["accepted"])
    with_devices = sum(1 for u in users if u["devices"] > 0)
    lines = [
        "👥 <b>Пользователи</b>",
        f"Всего: <b>{total}</b>",
        f"Подписали документы: <b>{signed}</b>",
        f"С активными устройствами: <b>{with_devices}</b>",
        "",
    ]
    for u in users:
        status = "✅" if u["accepted"] else "❌"
        devices = f"({u['devices']} устр.)" if u["devices"] > 0 else "(нет подписок)"
        if u["username"]:
            display = f"@{html.escape(u['username'])} (<code>{u['tid']}</code>)"
        else:
            display = f"<code>{u['tid']}</code>"
        lines.append(f"• {status} {display} {devices}")

    text = "\n".join(lines)
    if len(text) > 4000:
        for i in range(0, len(lines), 50):
            chunk = "\n".join(lines[i : i + 50])
            await message.answer(chunk)
    else:
        await message.answer(text)


@router.message(Command("user_info"))
async def cmd_user_info(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        return

    parts = (message.text or "").split()
    if len(parts) < 2:
        await message.answer("Использование: <code>/user_info telegram_id</code>")
        return

    try:
        target_tid = int(parts[1])
    except ValueError:
        await message.answer("Некорректный ID.")
        return

    devices = await db.list_user_devices(target_tid)
    if not devices:
        await message.answer(
            f"У пользователя <code>{target_tid}</code> нет активных подписок."
        )
        return

    lines = [
        f"👤 <b>Пользователь <code>{target_tid}</code></b>",
        f"Подписок: <b>{len(devices)}</b>",
        "",
    ]
    for d in devices:
        label = device_subscription_label(d.device_kind, d.slot_index)
        expiry = format_expiry_time_ms(d.expiry_time_ms)
        lines.append(f"🔹 <b>{label}</b>")
        lines.append(f"  • Срок: {expiry}")
        lines.append(f"  • Email: <code>{d.base_email}</code>")
        lines.append(f"  • UUID: <code>{d.uuid}</code>")
        lines.append(f"  • Sub token: <code>{d.sub_token}</code>")
        lines.append("")

    await message.answer("\n".join(lines))


@router.message(Command("reset_legal"))
async def cmd_reset_legal(message: Message, bot: Bot) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.answer("Команда только для администратора.")
        return

    parts = (message.text or "").split()
    if len(parts) < 2 or parts[1].lower() != "confirm":
        await message.answer(
            "Эта команда сбросит принятие документов у всех пользователей с подписками "
            "и отправит им новый текст.\n\n"
            "Для запуска: <code>/reset_legal confirm</code>"
        )
        return

    await db.reset_all_legal_acceptances()
    devices = await db.list_all_user_devices()
    user_ids = sorted({d.telegram_id for d in devices})
    await message.answer(
        f"Начинаю рассылку обновлённых документов для {len(user_ids)} пользователей."
    )

    sent = 0
    failed = 0
    for tid in user_ids:
        try:
            await bot.send_message(tid, LEGAL_BROADCAST)
            await bot.send_message(tid, LEGAL_TEXT, reply_markup=agreement_keyboard())
            sent += 1
            await asyncio.sleep(0.05)
        except Exception as e:
            failed += 1
            logger.warning("Не удалось отправить уведомление %s: %s", tid, e)

    await message.answer(
        "Рассылка завершена.\n"
        f"Отправлено: {sent}\n"
        f"Ошибок доставки: {failed}"
    )


@router.message(Command("stats"))
async def cmd_stats(message: Message) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.answer("Команда только для администратора.")
        return
    u = await db.count_distinct_subscribers()
    d = await db.count_devices()
    p = await db.count_pending_requests()
    r = await db.count_pending_renewals()
    await message.answer(
        f"Уникальных пользователей: {u}\n"
        f"Всего конфигов (устройств): {d}\n"
        f"Заявок на доступ: {p}\n"
        f"Заявок на продление: {r}"
    )
