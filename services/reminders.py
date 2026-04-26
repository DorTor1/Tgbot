"""Фоновый воркер: рассылка напоминаний 3д/1д/expired."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from aiogram import Bot

import db
from config import REMINDER_1D_MS, REMINDER_3D_MS, REMINDER_CHECK_INTERVAL_SECONDS
from formatting import device_subscription_label, format_expiry_time_ms
from keyboards import renew_only_keyboard
from texts import reminder_text

logger = logging.getLogger(__name__)


async def _send_reminder(bot: Bot, device: db.UserDeviceRecord, stage: str) -> bool:
    label = device_subscription_label(device.device_kind, device.slot_index)
    expiry = format_expiry_time_ms(device.expiry_time_ms)
    text = reminder_text(label, expiry, stage)
    kb = renew_only_keyboard(device.device_kind, device.slot_index)
    try:
        await bot.send_message(device.telegram_id, text, reply_markup=kb)
    except Exception:
        logger.exception(
            "Не удалось отправить напоминание stage=%s tg_id=%s device=%s/%s",
            stage,
            device.telegram_id,
            device.device_kind,
            device.slot_index,
        )
        return False
    return True


async def send_due_reminders(bot: Bot) -> None:
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    devices = await db.list_all_user_devices()
    for device in devices:
        if device.expiry_time_ms is None:
            continue
        remaining_ms = device.expiry_time_ms - now_ms
        if remaining_ms <= 0:
            if device.expired_notified_at is None:
                ok = await _send_reminder(bot, device, "expired")
                if ok:
                    await db.mark_subscription_notice_sent(
                        device.telegram_id,
                        device.device_kind,
                        device.slot_index,
                        "expired",
                    )
            continue
        if remaining_ms <= REMINDER_1D_MS:
            if device.reminder_1d_sent_at is None:
                ok = await _send_reminder(bot, device, "1d")
                if ok:
                    await db.mark_subscription_notice_sent(
                        device.telegram_id,
                        device.device_kind,
                        device.slot_index,
                        "1d",
                    )
            continue
        if remaining_ms <= REMINDER_3D_MS and device.reminder_3d_sent_at is None:
            ok = await _send_reminder(bot, device, "3d")
            if ok:
                await db.mark_subscription_notice_sent(
                    device.telegram_id,
                    device.device_kind,
                    device.slot_index,
                    "3d",
                )


async def reminder_worker(bot: Bot) -> None:
    """Бесконечный цикл: раз в час проверять подписки и слать напоминания."""
    while True:
        try:
            await send_due_reminders(bot)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Ошибка фоновой проверки напоминаний")
        await asyncio.sleep(REMINDER_CHECK_INTERVAL_SECONDS)
