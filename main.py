"""Telegram-бот: регистрация в 3x-ui. Точка входа.

Архитектура:
- ``config.py`` — окружение, панели, константы.
- ``texts.py`` — все пользовательские/админские строки.
- ``formatting.py`` — форматирование статусов, дат, имён.
- ``keyboards.py`` — все клавиатуры.
- ``services/`` — бизнес-логика (панели, подписки, ссылки, напоминания).
- ``handlers/`` — роутеры aiogram (start, cabinet, access, renewal, admin, agreement).
- ``db.py`` — слой SQLite.
- ``panel_api.py`` — клиент 3x-ui.
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import suppress

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties

import db
from config import (
    BOT_TOKEN,
    PANELS,
    SUBSCRIPTION_AGGREGATOR_BASE,
    SUBSCRIPTION_PORTAL_BASE,
    admin_ids,
    require_approval,
)
from handlers import build_router
from services.links import refresh_sub_config
from services.reminders import reminder_worker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def _validate_config() -> None:
    if not BOT_TOKEN:
        raise SystemExit("Укажите BOT_TOKEN в .env")
    if not PANELS:
        raise SystemExit(
            "Укажите хотя бы одну панель: PANEL_BASE_URL_1 / PANEL_LOGIN_1 / "
            "PANEL_PASSWORD_1 (или старые PANEL_BASE_URL / PANEL_LOGIN / PANEL_PASSWORD "
            "для одной панели)."
        )
    for p in PANELS:
        if not p.login or not p.password:
            raise SystemExit(
                f"Для панели #{p.index} ({p.name}) не задан PANEL_LOGIN_{p.index} "
                f"или PANEL_PASSWORD_{p.index}."
            )


def _log_startup_info() -> None:
    logger.info(
        "Сконфигурировано панелей: %d — %s",
        len(PANELS),
        ", ".join(f"#{p.index} {p.name} ({p.base_url})" for p in PANELS),
    )
    if SUBSCRIPTION_PORTAL_BASE:
        logger.info(
            "Ссылки на подписку: HTML-портал %s?name=<sub_token>",
            SUBSCRIPTION_PORTAL_BASE,
        )
    elif SUBSCRIPTION_AGGREGATOR_BASE:
        logger.info(
            "Ссылки на подписку: агрегатор %s/<sub_token>",
            SUBSCRIPTION_AGGREGATOR_BASE,
        )
    else:
        logger.info(
            "Ссылки на подписку: отдельный URL с каждой панели "
            "(нет SUBSCRIPTION_PORTAL_BASE и SUBSCRIPTION_AGGREGATOR_BASE)."
        )

    if require_approval() and not admin_ids():
        logger.warning(
            "REQUIRE_APPROVAL=1, но не заданы ADMINS/ADMIN_ID — заявки некому подтверждать"
        )


async def main() -> None:
    _validate_config()
    await db.init_db()
    await refresh_sub_config()
    _log_startup_info()

    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode="HTML"),
    )
    dp = Dispatcher()
    dp.include_router(build_router())
    reminder_task = asyncio.create_task(reminder_worker(bot))
    logger.info("Бот запущен")
    try:
        await dp.start_polling(bot)
    finally:
        reminder_task.cancel()
        with suppress(asyncio.CancelledError):
            await reminder_task


if __name__ == "__main__":
    asyncio.run(main())
