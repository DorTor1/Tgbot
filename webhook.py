"""HTTP-эндпоинт для вебхуков ЮKassa.

Запускается отдельным процессом (systemd unit) на порту из YOOKASSA_WEBHOOK_PORT
(по умолчанию 8080). ЮKassa шлёт сюда POST /yookassa/notify с уведомлениями
о смене статуса платежа.

При payment.succeeded — создаём клиента в 3x-ui и отправляем пользователю ссылку.
При payment.canceled — уведомляем пользователя, что оплата не прошла.

C4+C5: безопасность webhook'а — IP-фильтр (whitelist ЮKassa) + HMAC-SHA256
проверка подписи тела через Content-Signature. Оба фильтра независимы —
если любой отказывает, запрос отклоняется. Kill-switch через
YOOKASSA_REQUIRE_SIGNATURE=0 в .env (для аварийного отключения проверок).
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import ipaddress
import json
import logging
import os
import signal
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


# C5: IP-адреса, с которых ЮKassa шлёт webhook'и.
# Источник: https://yookassa.ru/developers/using-api/webhooks
# (актуальный список — на момент реализации; перепроверять при изменениях в доке).
YOOKASSA_IP_RANGES_RAW: tuple[str, ...] = (
    "185.71.76.0/27",
    "185.71.77.0/27",
    "77.75.153.0/25",
    "77.75.154.128/25",
    "77.75.156.11/32",
    "77.75.156.35/32",
    "2a02:5180::/32",
)
YOOKASSA_IP_NETWORKS: tuple[Any, ...] = tuple(
    ipaddress.ip_network(c) for c in YOOKASSA_IP_RANGES_RAW
)


def _signature_required() -> bool:
    """Kill-switch: True = проверять подпись + IP, False = пропускать всё."""
    return os.getenv("YOOKASSA_REQUIRE_SIGNATURE", "0").strip() == "1"


def _trust_proxy() -> bool:
    """True = читать реальный IP из X-Forwarded-For (стоит за nginx)."""
    return os.getenv("YOOKASSA_TRUST_PROXY", "0").strip() == "1"


def parse_content_signature_header(raw: str | None) -> str | None:
    """Парсит заголовок Content-Signature.

    ЮKassa шлёт в формате `value=<hex>` (см. официальную доку).
    Для совместимости принимаем и "голый" hex.
    Возвращает чистый hex (lowercase) или None, если заголовок пустой.
    """
    if not raw:
        return None
    raw = raw.strip()
    if not raw:
        return None
    if raw.lower().startswith("value="):
        raw = raw[6:].strip()
    return raw or None


def verify_yookassa_signature(
    body: bytes, header_value: str | None, secret: str
) -> bool:
    """Проверяет HMAC-SHA256 подпись тела webhook'а.

    ЮKassa подписывает сырое тело запроса (до JSON-парсинга) твоим
    YOOKASSA_SECRET_KEY. Подпись приходит в Content-Signature.
    Безопасное сравнение через hmac.compare_digest.
    """
    if not secret or not header_value:
        return False
    hex_sig = parse_content_signature_header(header_value)
    if not hex_sig:
        return False
    expected = hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(hex_sig.lower(), expected.lower())


def ip_allowed(
    remote: str,
    trust_proxy: bool,
    x_forwarded_for: str | None,
    networks: tuple[Any, ...],
) -> bool:
    """True, если remote (или первый IP из X-Forwarded-For при trust_proxy)
    входит в whitelist networks.
    """
    candidate = (remote or "").strip()
    if trust_proxy and x_forwarded_for:
        first = x_forwarded_for.split(",", 1)[0].strip()
        if first:
            candidate = first
    try:
        ip = ipaddress.ip_address(candidate)
    except ValueError:
        return False
    return any(ip in net for net in networks)


@web.middleware
async def yookassa_security_middleware(
    request: web.Request, handler: Any
) -> web.StreamResponse:
    """C4+C5: IP-фильтр + HMAC-проверка подписи для /yookassa/notify.

    Проверки работают только если YOOKASSA_REQUIRE_SIGNATURE=1 (kill-switch).
    На других путях (например /health) middleware пропускает без проверок.
    Body читается в middleware, кладётся в request._body_for_handler —
    handler не должен вызывать request.json()/request.read() повторно.
    """
    if request.path != "/yookassa/notify":
        return await handler(request)

    if not _signature_required():
        return await handler(request)

    # 1. IP-фильтр
    if not ip_allowed(
        remote=request.remote or "",
        trust_proxy=_trust_proxy(),
        x_forwarded_for=request.headers.get("X-Forwarded-For"),
        networks=YOOKASSA_IP_NETWORKS,
    ):
        logger.warning(
            "Webhook: отклонён по IP remote=%s xff=%s",
            request.remote,
            request.headers.get("X-Forwarded-For"),
        )
        return web.Response(status=403, text="forbidden ip")

    # 2. Подпись
    body = await request.read()
    secret = os.getenv("YOOKASSA_SECRET_KEY", "").strip()
    if not secret:
        logger.error(
            "Webhook: YOOKASSA_REQUIRE_SIGNATURE=1, но YOOKASSA_SECRET_KEY пуст — "
            "проверка подписи невозможна. Задайте ключ в .env или выключите kill-switch."
        )
        return web.Response(status=500, text="signature misconfigured")

    if not verify_yookassa_signature(
        body, request.headers.get("Content-Signature"), secret
    ):
        logger.warning(
            "Webhook: неверная подпись remote=%s",
            request.remote,
        )
        return web.Response(status=401, text="bad signature")

    request._body_for_handler = body
    return await handler(request)


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

    # Помечаем оплату в БД. rowcount==0 → другой webhook уже обработал (N2 race).
    updated = await db.mark_payment_paid(payment_record.yookassa_payment_id)
    if updated == 0:
        logger.info(
            "Webhook: payment %s уже обработан (rowcount=0), пропускаем",
            payment_record.yookassa_payment_id,
        )
        return

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
    updated = await db.mark_payment_canceled(payment_record.yookassa_payment_id)
    if updated == 0:
        logger.info(
            "Webhook: payment %s уже не pending, пропускаем",
            payment_record.yookassa_payment_id,
        )
        return
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
    """POST /yookassa/notify — сюда стучится ЮKassa.

    C4: если включён kill-switch — body прочитан в middleware и лежит в
    request._body_for_handler. Иначе читаем сами (старое поведение).
    """
    body = getattr(request, "_body_for_handler", None)
    if body is None:
        try:
            body = await request.read()
        except Exception:
            logger.warning("Webhook: не удалось прочитать тело")
            return web.Response(status=400, text="bad request")

    try:
        data = json.loads(body)
    except Exception:
        logger.warning("Webhook: некорректный JSON")
        return web.Response(status=400, text="bad json")

    event = data.get("event")
    obj = data.get("object") or {}
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

    app = web.Application(middlewares=[yookassa_security_middleware])
    app["bot"] = bot
    app.router.add_post("/yookassa/notify", _handle_notify)
    app.router.add_get("/health", _handle_health)

    host = os.getenv("YOOKASSA_WEBHOOK_HOST", "0.0.0.0")
    port = int(os.getenv("YOOKASSA_WEBHOOK_PORT", "8080"))
    logger.info("Webhook ЮKassa слушает http://%s:%s/yookassa/notify", host, port)

    try:
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, host=host, port=port)
        await site.start()
        logger.info(
            "Webhook ЮKassa listening on http://%s:%s/yookassa/notify", host, port
        )
        stop_event = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, stop_event.set)
            except NotImplementedError:
                pass
        await stop_event.wait()
    finally:
        await runner.cleanup()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
