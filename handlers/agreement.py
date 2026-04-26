"""Принятие правил и пользовательского соглашения."""

from __future__ import annotations

from aiogram import F, Router
from aiogram.types import CallbackQuery

import db
from keyboards import agreement_keyboard, device_selection_keyboard
from texts import (
    LEGAL_DECLINED,
    access_request_intro,
)
from vpn_legal import LEGAL_TEXT
from ._helpers import safe_delete

router = Router()


@router.callback_query(F.data == "agr:show")
async def cb_agreement_show(query: CallbackQuery) -> None:
    await query.answer()
    if query.message:
        await query.message.answer(LEGAL_TEXT, reply_markup=agreement_keyboard())


@router.callback_query(F.data == "agr:yes")
async def cb_agreement_accept(query: CallbackQuery) -> None:
    if query.from_user is None:
        await query.answer()
        return
    await db.set_rules_accepted(query.from_user.id)
    await db.set_agreement_accepted(query.from_user.id)
    await query.answer("Условия приняты")
    await safe_delete(query)
    if query.message:
        await query.message.answer(
            access_request_intro(),
            reply_markup=device_selection_keyboard(),
        )


@router.callback_query(F.data == "agr:no")
async def cb_agreement_decline(query: CallbackQuery) -> None:
    if query.from_user is None:
        await query.answer()
        return
    await query.answer()
    if query.message:
        try:
            await query.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
        await query.message.answer(LEGAL_DECLINED)
