"""Команды /start и главное меню."""

from __future__ import annotations

from aiogram import Router
from aiogram.filters import CommandStart
from aiogram.types import Message

from config import is_admin
from keyboards import cabinet_keyboard, main_reply_keyboard
from ._helpers import cabinet_view

router = Router()


@router.message(CommandStart())
async def cmd_start(message: Message) -> None:
    user = message.from_user
    is_admin_user = is_admin(user.id if user else None)
    text, has_devices, has_pending = await cabinet_view(user)
    if is_admin_user:
        text += "\n\n🛠 Для администратора: /admin"
    await message.answer(text, reply_markup=main_reply_keyboard(is_admin_user))
    await message.answer(
        "Действия:",
        reply_markup=cabinet_keyboard(has_devices, has_pending),
    )
