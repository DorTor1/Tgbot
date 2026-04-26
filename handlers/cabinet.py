"""Личный кабинет: просмотр, ссылки, FAQ, поддержка."""

from __future__ import annotations

import html

from aiogram import F, Router
from aiogram.types import CallbackQuery, Message

import db
from formatting import (
    device_subscription_label,
    format_expiry_time_ms,
    subscription_status,
)
from keyboards import (
    back_to_cabinet_keyboard,
    cabinet_keyboard,
    link_keyboard,
    support_keyboard,
)
from services.links import all_links
from texts import FAQ_TEXT
from ._helpers import cabinet_view, reply_or_edit

router = Router()


@router.message(F.text == "Личный кабинет")
@router.message(F.text == "Мои подписки")
async def show_cabinet_msg(message: Message) -> None:
    text, has_devices, has_pending = await cabinet_view(message.from_user)
    await message.answer(
        text,
        reply_markup=cabinet_keyboard(has_devices, has_pending),
    )


@router.message(F.text == "Помощь")
async def show_help_msg(message: Message) -> None:
    await message.answer(FAQ_TEXT, reply_markup=back_to_cabinet_keyboard())


@router.callback_query(F.data == "cab:home")
async def cb_cabinet_home(query: CallbackQuery) -> None:
    await query.answer()
    if query.message is None:
        return
    text, has_devices, has_pending = await cabinet_view(query.from_user)
    await reply_or_edit(
        query.message, text, reply_markup=cabinet_keyboard(has_devices, has_pending)
    )


@router.callback_query(F.data == "cab:faq")
async def cb_faq(query: CallbackQuery) -> None:
    await query.answer()
    if query.message:
        await reply_or_edit(query.message, FAQ_TEXT, reply_markup=back_to_cabinet_keyboard())


@router.callback_query(F.data == "cab:support")
async def cb_support(query: CallbackQuery) -> None:
    await query.answer()
    if query.message:
        await reply_or_edit(
            query.message,
            "💬 <b>Связаться с админом</b>\n\n"
            "Нажмите кнопку ниже — откроется чат с администратором. "
            "Опишите вопрос, и вам ответят.",
            reply_markup=support_keyboard(),
        )


@router.callback_query(F.data == "cab:links")
async def cb_links(query: CallbackQuery) -> None:
    await query.answer()
    if query.from_user is None or query.message is None:
        return
    devices = await db.list_user_devices(query.from_user.id)
    if not devices:
        await query.message.answer(
            "Пока нет активных подписок. Нажмите «➕ Получить доступ», когда будете готовы.",
            reply_markup=back_to_cabinet_keyboard(),
        )
        return

    await query.message.answer(
        f"🔗 <b>Ваши подписки: {len(devices)}</b>\n"
        "Ниже — отдельные карточки с кнопкой инструкции."
    )
    for i, d in enumerate(devices, start=1):
        links = all_links(d.sub_token)
        if not links:
            await query.message.answer(
                "Не настроены источники ссылок подписки. Напишите администратору."
            )
            return
        primary_url = links[0][1]
        icon, status = subscription_status(d.expiry_time_ms)
        label = device_subscription_label(d.device_kind, d.slot_index)
        body = (
            f"{icon} <b>{i}. {label}</b>\n"
            f"📅 Действует до: <b>{format_expiry_time_ms(d.expiry_time_ms)}</b>\n"
            f"⏳ Статус: {status}\n\n"
            "Инструкция и подключение — по кнопке ниже."
        )
        if len(links) > 1:
            body += "\n\n<b>Доп. ссылки:</b>\n" + "\n".join(
                f"• {html.escape(name)}: <code>{html.escape(link)}</code>"
                for name, link in links[1:]
            )
        await query.message.answer(
            body,
            reply_markup=link_keyboard(primary_url, d.device_kind, d.slot_index),
        )
