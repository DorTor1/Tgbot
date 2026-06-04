"""HTTP-эндпоинт для вебхуков ЮKassa.

Запускается отдельным процессом (systemd unit) на порту из YOOKASSA_WEBHOOK_PORT
(по умолчанию 8080). ЮKassa шлёт сюда POST /yookassa/notify с уведомлениями
о смене статуса платежа.

При payment.succeeded — создаём клиента в 3x-ui и отправляем пользователю ссылку.
При payment.canceled — уведомляем пользователя, что оплата не прошла.
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Any

from aiohttp import web
from dotenv import load_dotenv

import db
import payments
from bot_ui import PAYMENT_CANCELED, PAYMENT_FAILED, PAYMENT_SUCCEEDED_HEADER

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)


def _load_panel_helpers_from_main():
    """Импортируем из main.py функции создания подписки и формирования текста.

    Импорт именно main, потому что _create_subscription_for_user и
    _all_links/_device_subscription_label_from_parts живут там.
    Чтобы не тащить aiogram Dispatcher и не запускать polling, дёргаем
    только нужные функции.
    """
    import main  # type: ignore

    return {
        "create_subscription": main._create_subscription_for_user,
        "extend_subscription": main._extend_subscription_for_user,
        "all_links": main._all_links,
        "device_label": main._device_subscription_label_from_parts,
        "subscription_text": main._subscription_message_text,
        "subscription_keyboard": main._subscription_reply_keyboard,
        "notify_admins_payment": main._notify_admins_new_payment,
        "group_size": main.GROUP_SIZE,
    }


HELPERS: dict[str, Any] = {}


async def _process_succeeded(payment_record: db.PaymentRecord, bot) -> None:
    """Платёж успешно оплачен → создаём/продлеваем подписку и уведомляем.

    Для «новой» подписки (kind=new): создаём ОДИН слот.
    Для «продления» (kind=renewal): продлеваем ВСЮ десятку, в которой
    находится оплаченный слот (slot_index = ведущий 1, 11, 21, ...).
    """
    helpers = HELPERS
    days = payment_record.plan_days
    tid = payment_record.telegram_id
    is_renewal = payment_record.kind == "renewal"

    # Помечаем оплату в БД
    await db.mark_payment_paid(payment_record.yookassa_payment_id)

    if is_renewal:
        # Продление: продлеваем всю десятку, привязывая срок к lead'у
        global_slot = await db.get_user_global_slot_index(
            tid, payment_record.device_kind, payment_record.slot_index
        )
        from datetime import datetime, timezone

        group_size = helpers.get("group_size", 10)
        extend = helpers.get("extend_subscription")
        label = helpers["device_label"](
            payment_record.device_kind, payment_record.slot_index
        )
        renewed_count = 0
        total_in_group = 0

        if global_slot is None:
            logger.error(
                "Webhook: renewal — слот не найден tid=%s %s/%s",
                tid,
                payment_record.device_kind,
                payment_record.slot_index,
            )
        elif extend is None:
            logger.error("Webhook: helpers.extend_subscription не зарегистрирован")
        else:
            # Сначала определяем lead'а, потом берём устройства его группы
            lead_global = ((global_slot - 1) // group_size) * group_size + 1
            group_devices = await db.list_user_devices_in_group(
                tid, lead_global, group_size
            )
            total_in_group = len(group_devices)
            lead_dev = await db.get_user_device_by_global_slot(tid, lead_global)
            now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
            if (
                lead_dev
                and lead_dev.expiry_time_ms
                and lead_dev.expiry_time_ms > now_ms
            ):
                # Lead ещё активен — продлеваем от его expiry
                target_expiry_ms = lead_dev.expiry_time_ms + days * 24 * 60 * 60 * 1000
            else:
                # Lead истёк или его нет — продлеваем от сегодня
                target_expiry_ms = now_ms + days * 24 * 60 * 60 * 1000

            for dev in group_devices:
                ok, _, _ = await extend(
                    tid=tid,
                    device_kind=dev.device_kind,
                    slot_index=dev.slot_index,
                    target_expiry_ms=target_expiry_ms,
                )
                if ok:
                    renewed_count += 1
            logger.info(
                "Webhook: продлено %d/%d устройств в группе lead=%d для tid=%s до %s",
                renewed_count,
                total_in_group,
                lead_global,
                tid,
                target_expiry_ms,
            )

        # Сообщаем пользователю и админам по результату
        from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

        if renewed_count == 0:
            # Деньги списаны, но ничего не продлилось — алерт админам
            logger.error(
                "Webhook: renewal — 0 устройств продлено для tid=%s "
                "(в группе было %d, payment=%s)",
                tid,
                total_in_group,
                payment_record.yookassa_payment_id,
            )
            try:
                await bot.send_message(
                    tid,
                    f"✅ Оплата получена, но при продлении возникла ошибка.\n"
                    f"Админ уже разбирается — скоро всё заработает.\n\n"
                    f"Оплаченный счёт: «{label}».",
                    reply_markup=InlineKeyboardMarkup(
                        inline_keyboard=[
                            [
                                InlineKeyboardButton(
                                    text="🏠 Главное меню",
                                    callback_data="menu_main",
                                )
                            ]
                        ]
                    ),
                )
            except Exception:
                logger.exception("Webhook: не удалось уведомить %s об ошибке", tid)
            # Алерт админам
            try:
                notify_admins = helpers.get("notify_admins_payment")
                if notify_admins is not None:
                    await notify_admins(bot, payment_record)
            except Exception:
                logger.exception(
                    "Webhook: не удалось уведомить админов об ошибке renewal"
                )
        else:
            # Успех — уведомляем и админов для контроля
            try:
                await bot.send_message(
                    tid,
                    f"✅ Оплата получена! Все устройства группы продлены до одной даты "
                    f"(включая «{label}»).",
                    reply_markup=InlineKeyboardMarkup(
                        inline_keyboard=[
                            [
                                InlineKeyboardButton(
                                    text="📋 Мои подписки",
                                    callback_data="menu_my_subs",
                                ),
                                InlineKeyboardButton(
                                    text="🏠 Главное меню",
                                    callback_data="menu_main",
                                ),
                            ]
                        ]
                    ),
                )
            except Exception:
                logger.exception("Webhook: не удалось уведомить %s", tid)
            try:
                notify_admins = helpers.get("notify_admins_payment")
                if notify_admins is not None:
                    await notify_admins(bot, payment_record)
            except Exception:
                logger.exception("Webhook: не удалось уведомить админов о продлении")
        return

    # kind=new: создаём ОДИН слот (как раньше)
    ok, sub, expiry_ms, err = await helpers["create_subscription"](
        tid=tid,
        base_email=payment_record.base_email,
        device_kind=payment_record.device_kind,
        slot_index=payment_record.slot_index,
        days=days,
    )
    if not ok or sub is None or expiry_ms is None:
        logger.error(
            "Webhook: не удалось создать подписку для tid=%s payment=%s: %s",
            tid,
            payment_record.yookassa_payment_id,
            err,
        )
        try:
            from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

            await bot.send_message(
                tid,
                f"✅ Оплата получена, но при активации возникла ошибка.\n"
                f"Админ уже разбирается — скоро всё заработает.\n\n"
                f"Ошибка: {err or 'неизвестно'}",
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[
                        [
                            InlineKeyboardButton(
                                text="🏠 Главное меню", callback_data="menu_main"
                            )
                        ]
                    ]
                ),
            )
        except Exception:
            logger.exception("Webhook: не удалось уведомить пользователя %s", tid)
        return

    label = helpers["device_label"](
        payment_record.device_kind, payment_record.slot_index
    )
    links = helpers["all_links"](sub)
    text = helpers["subscription_text"](label, expiry_ms, links)
    text = f"{PAYMENT_SUCCEEDED_HEADER}\n\n{text}"
    kb = helpers["subscription_keyboard"](
        sub_token=sub,
        device_label=label,
        device_kind=payment_record.device_kind,
        slot_index=payment_record.slot_index,
        back_subs=True,
        back_menu=True,
    )
    try:
        await bot.send_message(tid, text, reply_markup=kb, parse_mode="HTML")
    except Exception:
        logger.exception("Webhook: не удалось отправить ссылку пользователю %s", tid)

    # Уведомляем админов (для контроля)
    try:
        await helpers["notify_admins_payment"](bot, payment_record)
    except Exception:
        logger.exception("Webhook: не удалось уведомить админов о платеже")


async def _process_canceled(payment_record: db.PaymentRecord, bot) -> None:
    await db.mark_payment_canceled(payment_record.yookassa_payment_id)
    try:
        if payment_record.kind == "renewal":
            await bot.send_message(payment_record.telegram_id, PAYMENT_CANCELED)
        else:
            await bot.send_message(payment_record.telegram_id, PAYMENT_FAILED)
    except Exception:
        logger.exception(
            "Webhook: не удалось уведомить %s об отмене платежа",
            payment_record.telegram_id,
        )


async def _handle_notify(request: web.Request) -> web.Response:
    """POST /yookassa/notify — сюда стучится ЮKassa."""
    try:
        body = await request.json()
    except Exception:
        logger.warning("Webhook: некорректный JSON")
        return web.Response(status=400, text="bad json")

    event = body.get("event")
    obj = body.get("object") or {}
    payment_id = obj.get("id")
    if not event or not payment_id:
        logger.warning("Webhook: пустой event/object: %s", body)
        return web.Response(status=400, text="bad payload")

    # ЮKassa шлёт уведомления многократно, поэтому идемпотентность через БД
    record = await db.get_payment_by_yookassa_id(payment_id)
    if record is None:
        logger.warning("Webhook: платёж %s не найден в БД", payment_id)
        return web.Response(status=200, text="ok")

    if record.status in ("succeeded", "canceled"):
        # Уже обработан
        return web.Response(status=200, text="already processed")

    bot = request.app["bot"]

    if event == "payment.succeeded":
        await _process_succeeded(record, bot)
    elif event in ("payment.canceled", "payment.expired"):
        await _process_canceled(record, bot)
    else:
        logger.info("Webhook: неизвестный event=%s payment=%s", event, payment_id)

    return web.Response(status=200, text="ok")


async def _handle_health(_: web.Request) -> web.Response:
    return web.json_response({"status": "ok"})


async def main() -> None:
    load_dotenv()
    if not payments.is_configured():
        raise SystemExit("Задайте YOOKASSA_SHOP_ID и YOOKASSA_SECRET_KEY в .env")

    await db.init_db()

    # Бот нужен только для отправки сообщений пользователю
    from aiogram import Bot

    bot_token = os.getenv("BOT_TOKEN", "").strip()
    if not bot_token:
        raise SystemExit("Задайте BOT_TOKEN в .env")
    bot = Bot(token=bot_token)

    HELPERS.update(_load_panel_helpers_from_main())

    app = web.Application()
    app["bot"] = bot
    app.router.add_post("/yookassa/notify", _handle_notify)
    app.router.add_get("/health", _handle_health)

    host = os.getenv("YOOKASSA_WEBHOOK_HOST", "0.0.0.0")
    port = int(os.getenv("YOOKASSA_WEBHOOK_PORT", "8080"))
    logger.info("Webhook ЮKassa слушает http://%s:%s/yookassa/notify", host, port)

    try:
        await web._run_app(app, host=host, port=port, print=None)
    finally:
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
