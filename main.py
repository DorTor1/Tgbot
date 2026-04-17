"""Telegram-бот: регистрация в 3x-ui"""

from __future__ import annotations

import asyncio
from contextlib import suppress
from datetime import datetime, timezone
import logging
import os
import re
import secrets
import string
import uuid
from pathlib import Path
from urllib.parse import quote, urlparse

from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
    User,
)
from dotenv import load_dotenv

import db
from panel_api import PanelAPI, PanelAPIError, subscription_expiry_time_ms
from vpn_rules import RULES_TEXT
from vpn_user_agreement import AGREEMENT_TEXT

load_dotenv(Path(__file__).resolve().parent / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
PANEL_LOGIN = os.getenv("PANEL_LOGIN", "").strip()
PANEL_PASSWORD = os.getenv("PANEL_PASSWORD", "").strip()
ADMIN_ID_RAW = os.getenv("ADMIN_ID", "0").strip()
# Несколько админов: ADMINS=111,222,333 (приоритетнее одиночного ADMIN_ID)
ADMINS_RAW = os.getenv("ADMINS", "").strip()
PANEL_BASE_URL = os.getenv("PANEL_BASE_URL", "").strip().rstrip("/")
# Публичная ссылка подписки часто без секретного префикса панели (только хост).
SUBSCRIPTION_BASE_URL = os.getenv("SUBSCRIPTION_BASE_URL", "").strip().rstrip("/")

# Путь страницы/эндпоинта подписки в панели (как в настройках 3x-ui).
SUBSCRIPTION_PATH = os.getenv("SUBSCRIPTION_PATH", "").strip()
# Имя query-параметра для subId (у портала подписки часто ?name=<subId>). bare/legacy — старый вид ?<токен>.
SUBSCRIPTION_QUERY_PARAM = os.getenv("SUBSCRIPTION_QUERY_PARAM", "name").strip() or "name"

# Тип устройства → префикс в email панели (до _nick и номера слота).
EMAIL_PREFIX = {
    "phone": "phone",
    "laptop": "laptop",
    "pc": "pc",
    "other": "other",
}
DEVICE_LABEL_RU = {
    "phone": "Смартфон",
    "laptop": "Ноутбук",
    "pc": "ПК",
    "other": "Другое устройство",
}

REMINDER_3D_MS = 3 * 24 * 60 * 60 * 1000
REMINDER_1D_MS = 1 * 24 * 60 * 60 * 1000
REMINDER_CHECK_INTERVAL_SECONDS = 3600
RENEWAL_NOTE = "Для продления напишите администратору. Он продлит подписку вручную и скажет срок."

router = Router()


def _admin_id() -> int | None:
    try:
        return int(ADMIN_ID_RAW)
    except ValueError:
        return None


def _admin_ids() -> set[int]:
    ids: set[int] = set()
    if ADMINS_RAW:
        for part in ADMINS_RAW.split(","):
            part = part.strip()
            if not part:
                continue
            try:
                ids.add(int(part))
            except ValueError:
                continue
    if not ids:
        a = _admin_id()
        if a is not None:
            ids.add(a)
    return ids


def _is_admin(user_id: int | None) -> bool:
    if user_id is None:
        return False
    return user_id in _admin_ids()


def _require_approval() -> bool:
    v = os.getenv("REQUIRE_APPROVAL", "1").strip().lower()
    return v not in ("0", "false", "no", "off", "")


def _sanitize_nick(user: User | None) -> str:
    if user is None:
        return "user_unknown"
    if user.username:
        raw = user.username.lower()
    else:
        raw = (user.first_name or "user").replace(" ", "_").lower()
    cleaned = re.sub(r"[^a-z0-9_]", "", raw)
    if not cleaned:
        cleaned = f"u{user.id}"
    return cleaned[:40]


def _device_label_ru(kind: str) -> str:
    return DEVICE_LABEL_RU.get(kind, kind)


def _format_expiry_time_ms(expiry_time_ms: int | None) -> str:
    if expiry_time_ms is None:
        return "неизвестно"
    expiry = datetime.fromtimestamp(expiry_time_ms / 1000, tz=timezone.utc)
    return expiry.strftime("%d.%m.%Y %H:%M UTC")


def _device_subscription_label_from_parts(device_kind: str, slot_index: int) -> str:
    label = _device_label_ru(device_kind)
    if slot_index > 1:
        label += f" ({slot_index})"
    return label


def _subscription_message_text(
    device_label: str,
    expiry_time_ms: int | None,
    link: str,
    renewal_note: str = "",
) -> str:
    parts = [
        f"{device_label}\n"
        f"Подписка действует до: {_format_expiry_time_ms(expiry_time_ms)}\n\n"
    ]
    if renewal_note:
        parts.append(f"{renewal_note}\n")
    parts.append(f"Ссылка на подписку:\n{link}")
    return "\n".join(parts)


def _renewal_inline_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🔁 Продлить",
                    callback_data="renew:info",
                ),
            ]
        ]
    )


def _panel_base_email(nick: str, device_kind: str, slot_index: int) -> str:
    """Префикс для панели: phone_nick, второй смартфон — phone_nick2 (далее _1.._4 — inbound)."""
    p = EMAIL_PREFIX.get(device_kind, "other")
    nick = nick.strip()[:40] or "user"
    if slot_index <= 1:
        return f"{p}_{nick}"
    return f"{p}_{nick}{slot_index}"


def _sub_token() -> str:
    alphabet = string.ascii_letters + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(10))


def _subscription_link_base() -> str:
    if SUBSCRIPTION_BASE_URL:
        return SUBSCRIPTION_BASE_URL
    u = urlparse(PANEL_BASE_URL)
    if u.scheme and u.netloc:
        return f"{u.scheme}://{u.netloc}".rstrip("/")
    return PANEL_BASE_URL


def _instruction_link(sub_token: str) -> str:
    root = _subscription_link_base().rstrip("/")
    path = SUBSCRIPTION_PATH.strip().strip("/")
    base = f"{root}/{path}"
    enc = quote(sub_token, safe="")
    q = SUBSCRIPTION_QUERY_PARAM.lower()
    if q in ("bare", "legacy", "none"):
        return f"{base}?{enc}"
    return f"{base}?{quote(SUBSCRIPTION_QUERY_PARAM, safe='')}={enc}"


def _greeting_name(user: User | None) -> str:
    if user is None:
        return "друг"
    if user.first_name:
        return user.first_name.strip() or "друг"
    if user.username:
        return f"@{user.username}"
    return "друг"


def _main_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Получить доступ")],
            [KeyboardButton(text="Мои подписки")],
        ],
        resize_keyboard=True,
    )


def _device_inline_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="📱 Смартфон", callback_data="dev:phone"),
                InlineKeyboardButton(text="💻 Ноутбук", callback_data="dev:laptop"),
            ],
            [
                InlineKeyboardButton(text="🖥 ПК", callback_data="dev:pc"),
                InlineKeyboardButton(text="📟 Другое", callback_data="dev:other"),
            ],
        ]
    )


def _device_selection_text() -> str:
    """Текст перед выбором устройства (оплата + тариф при модели с заявкой админу)."""
    if _require_approval():
        return (
            "💳 <b>Как проходит оплата</b>\n\n"
            "Выберите тип устройства ниже — заявку получит администратор. "
            "Он напишет вам здесь, в Telegram, и пришлёт реквизиты для перевода.\n\n"
            "Стоимость доступа — <b>80 ₽</b> на <b>30 дней</b>. После перевода "
            "администратор подтвердит оплату и вы получите ссылку на подписку.\n\n"
            "<b>Шаг 1 — выберите устройство:</b>"
        )
    return "Выберите устройство, для которого нужна ссылка:"


def _terms_inline_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ Подписать",
                    callback_data="terms:yes",
                ),
                InlineKeyboardButton(
                    text="❌ Отказаться",
                    callback_data="terms:no",
                ),
            ],
        ]
    )


def _agreement_inline_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ Подписать",
                    callback_data="agr:yes",
                ),
                InlineKeyboardButton(
                    text="❌ Отказаться",
                    callback_data="agr:no",
                ),
            ],
        ]
    )


def _access_review_keyboard(target_telegram_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ Принять",
                    callback_data=f"apr:{target_telegram_id}",
                ),
                InlineKeyboardButton(
                    text="❌ Отклонить",
                    callback_data=f"rej:{target_telegram_id}",
                ),
            ]
        ]
    )


def _format_request_who(req: db.AccessRequestRecord) -> str:
    parts: list[str] = []
    if req.username:
        parts.append(f"@{req.username}")
    name = " ".join(
        x for x in (req.first_name or "", req.last_name or "") if x
    ).strip()
    if name:
        parts.append(name)
    return " / ".join(parts) if parts else "без имени"


async def _notify_admins_new_request(bot: Bot, req: db.AccessRequestRecord) -> None:
    text = (
        "📩 Запрос доступа к VPN\n"
        f"Telegram ID: <code>{req.telegram_id}</code>\n"
        f"Кто: {_format_request_who(req)}\n"
        f"Устройство: {_device_label_ru(req.device_kind)}"
        f" (слот {req.slot_index})\n"
        f"Префикс email в панели: <code>{req.base_email}</code>"
    )
    kb = _access_review_keyboard(req.telegram_id)
    for admin_id in _admin_ids():
        try:
            await bot.send_message(
                admin_id, text, reply_markup=kb, parse_mode="HTML"
            )
        except Exception:
            logger.exception("Не удалось отправить уведомление админу %s", admin_id)


async def _create_subscription_for_user(
    tid: int,
    base_email: str,
    device_kind: str,
    slot_index: int,
) -> tuple[bool, str | None, int | None, str]:
    """
    Создаёт клиентов в 3x-ui и запись в user_devices.
    Возвращает (успех, sub_token, expiry_time_ms, текст ошибки для пользователя).
    """
    if not PANEL_LOGIN or not PANEL_PASSWORD:
        return False, None, None, "Сейчас выдать ссылку нельзя. Напишите администратору."
    client_uuid = str(uuid.uuid4())
    sub = _sub_token()
    expiry_time_ms = subscription_expiry_time_ms()
    try:
        async with PanelAPI(PANEL_BASE_URL, PANEL_LOGIN, PANEL_PASSWORD) as api:
            await api.register_user_on_all_inbounds(
                base_email,
                client_uuid,
                sub,
                expiry_time_ms,
            )
    except PanelAPIError as e:
        logger.warning("Ошибка панели для tg_id=%s: %s", tid, e)
        return (
            False,
            None,
            None,
            "Не удалось выдать подписку. Попробуйте позже или напишите администратору.",
        )
    except Exception:
        logger.exception("Неожиданная ошибка при регистрации tg_id=%s", tid)
        return False, None, None, "Что-то пошло не так. Попробуйте позже."
    await db.create_user_device(
        tid,
        device_kind,
        slot_index,
        base_email,
        client_uuid,
        sub,
        expiry_time_ms,
    )
    return True, sub, expiry_time_ms, ""


async def _send_subscription_reminder(bot: Bot, device: db.UserDeviceRecord, stage: str) -> bool:
    label = _device_subscription_label_from_parts(device.device_kind, device.slot_index)
    expiry = _format_expiry_time_ms(device.expiry_time_ms)
    if stage == "3d":
        text = (
            f"Напоминание: подписка {label} закончится через 3 дня.\n"
            f"Окончание: {expiry}\n\n"
            f"{RENEWAL_NOTE}"
        )
    elif stage == "1d":
        text = (
            f"Напоминание: подписка {label} закончится через 1 день.\n"
            f"Окончание: {expiry}\n\n"
            f"{RENEWAL_NOTE}"
        )
    else:
        text = (
            f"Подписка {label} закончилась.\n"
            f"Окончание: {expiry}\n\n"
            f"{RENEWAL_NOTE}"
        )
    try:
        await bot.send_message(device.telegram_id, text)
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


async def _send_due_subscription_reminders(bot: Bot) -> None:
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    devices = await db.list_all_user_devices()
    for device in devices:
        if device.expiry_time_ms is None:
            continue
        remaining_ms = device.expiry_time_ms - now_ms
        if remaining_ms <= 0:
            if device.expired_notified_at is None:
                ok = await _send_subscription_reminder(bot, device, "expired")
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
                ok = await _send_subscription_reminder(bot, device, "1d")
                if ok:
                    await db.mark_subscription_notice_sent(
                        device.telegram_id,
                        device.device_kind,
                        device.slot_index,
                        "1d",
                    )
            continue
        if remaining_ms <= REMINDER_3D_MS and device.reminder_3d_sent_at is None:
            ok = await _send_subscription_reminder(bot, device, "3d")
            if ok:
                await db.mark_subscription_notice_sent(
                    device.telegram_id,
                    device.device_kind,
                    device.slot_index,
                    "3d",
                )


async def _subscription_reminder_worker(bot: Bot) -> None:
    while True:
        try:
            await _send_due_subscription_reminders(bot)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Ошибка фоновой проверки напоминаний")
        await asyncio.sleep(REMINDER_CHECK_INTERVAL_SECONDS)


@router.message(CommandStart())
async def cmd_start(message: Message) -> None:
    name = _greeting_name(message.from_user)
    approval_note = (
        "\n\nЗаявку перед выдачей может подтвердить администратор — ссылка придёт в этот чат."
        if _require_approval()
        else ""
    )
    text = (
        f"⚡️ Добро пожаловать, {name}, в Vibecode VPN!\n"
        "Твой личный доступ к свободному интернету — быстрый, стабильный и честный.\n\n"
        "Наши условия:\n\n"
        "Цена: всего 80 руб / месяц.\n\n"
        "Устройства: подключай любое количество гаджетов без доплат.\n\n"
        "Трафик: комфортный объем для повседневного использования.\n\n"
        "⚠️ Важное требование:\n\n"
        "Для корректной работы сервиса обязательна настройка раздельного туннелирования "
        "(Split Tunneling). Это позволит VPN работать только в нужных приложениях, "
        "сохраняя высокую скорость для остальных."
        f"{approval_note}\n\n"
        "Готов начать?\n"
        "Нажми кнопку ниже, чтобы получить конфиг и инструкцию по настройке."
    )
    await message.answer(text, reply_markup=_main_keyboard())


@router.message(F.text == "Получить доступ")
async def get_access(message: Message) -> None:
    if message.from_user is None:
        await message.answer("Что-то пошло не так. Попробуйте ещё раз.")
        return

    if not PANEL_LOGIN or not PANEL_PASSWORD:
        logger.error("Не заданы PANEL_LOGIN / PANEL_PASSWORD")
        await message.answer(
            "Сейчас выдать доступ нельзя. Напишите администратору."
        )
        return

    if await db.get_access_request(message.from_user.id):
        await message.answer(
            "Мы уже получили вашу заявку. Ожидайте ответа."
        )
        return

    tid = message.from_user.id
    if not await db.has_accepted_usage_rules(tid):
        await message.answer(
            RULES_TEXT
            + "\n\nЧтобы продолжить, подтвердите согласие с правилами — кнопки ниже.",
            reply_markup=_terms_inline_keyboard(),
        )
        return

    if not await db.has_accepted_user_agreement(tid):
        await message.answer(
            AGREEMENT_TEXT
            + "\n\nЧтобы продолжить, подтвердите согласие с пользовательским соглашением — кнопки ниже.",
            reply_markup=_agreement_inline_keyboard(),
        )
        return

    await message.answer(
        _device_selection_text(),
        reply_markup=_device_inline_keyboard(),
        parse_mode="HTML",
    )


@router.callback_query(F.data == "terms:yes")
async def cb_terms_accept(query: CallbackQuery) -> None:
    if query.from_user is None:
        await query.answer()
        return
    await db.set_rules_accepted(query.from_user.id)
    await query.answer("Спасибо!")
    if query.message:
        try:
            await query.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
        await query.message.answer(
            AGREEMENT_TEXT
            + "\n\nЧтобы продолжить, подтвердите согласие с пользовательским соглашением — кнопки ниже.",
            reply_markup=_agreement_inline_keyboard(),
        )


@router.callback_query(F.data == "terms:no")
async def cb_terms_decline(query: CallbackQuery) -> None:
    if query.from_user is None:
        await query.answer()
        return
    await query.answer()
    if query.message:
        try:
            await query.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
        await query.message.answer(
            "Пока вы не примете правила, оформить подписку нельзя. "
            "Когда будете готовы — снова нажмите «Получить доступ»."
        )


@router.callback_query(F.data == "agr:yes")
async def cb_agreement_accept(query: CallbackQuery) -> None:
    if query.from_user is None:
        await query.answer()
        return
    if not await db.has_accepted_usage_rules(query.from_user.id):
        await query.answer(
            "Сначала примите правила использования.", show_alert=True
        )
        return
    await db.set_agreement_accepted(query.from_user.id)
    await query.answer("Спасибо!")
    if query.message:
        try:
            await query.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
        await query.message.answer(
            _device_selection_text(),
            reply_markup=_device_inline_keyboard(),
            parse_mode="HTML",
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
        await query.message.answer(
            "Без принятия пользовательского соглашения оформить подписку нельзя. "
            "Когда будете готовы — снова нажмите «Получить доступ»."
        )


@router.message(F.text == "Мои подписки")
async def my_subscriptions(message: Message) -> None:
    if message.from_user is None:
        return
    devices = await db.list_user_devices(message.from_user.id)
    if not devices:
        await message.answer(
            "Пока нет ссылок. Нажмите «Получить доступ», когда будете готовы."
        )
        return
    chunks: list[str] = []
    for d in devices:
        chunks.append(
            _subscription_message_text(
                _device_subscription_label_from_parts(d.device_kind, d.slot_index),
                d.expiry_time_ms,
                _instruction_link(d.sub_token),
                RENEWAL_NOTE,
            )
        )
    await message.answer(
        "Ваши подписки:\n\n" + "\n\n".join(chunks),
        reply_markup=_renewal_inline_keyboard(),
    )


@router.callback_query(F.data == "renew:info")
async def cb_renew_info(query: CallbackQuery) -> None:
    await query.answer()
    if query.message:
        await query.message.answer(RENEWAL_NOTE)


@router.callback_query(F.data.startswith("dev:"))
async def cb_device_chosen(query: CallbackQuery, bot: Bot) -> None:
    if query.from_user is None:
        await query.answer()
        return
    kind = query.data.split(":", 1)[1]
    if kind not in EMAIL_PREFIX:
        await query.answer("Выберите вариант из списка.", show_alert=True)
        return

    tid = query.from_user.id
    if not await db.has_accepted_user_agreement(tid):
        await query.answer(
            "Сначала пройдите шаги в «Получить доступ»: правила и соглашение.",
            show_alert=True,
        )
        return

    if await db.get_access_request(tid):
        await query.answer(
            "Дождитесь ответа по предыдущей заявке.", show_alert=True
        )
        return

    if not PANEL_LOGIN or not PANEL_PASSWORD:
        await query.answer("Сервис временно недоступен.", show_alert=True)
        return

    nick = _sanitize_nick(query.from_user)
    n_same = await db.count_device_slots(tid, kind)
    slot_index = n_same + 1
    base_email = _panel_base_email(nick, kind, slot_index)

    if _require_approval():
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
            await query.answer(
                "Заявка уже отправлена. Ожидайте.",
                show_alert=True,
            )
            return
        req = await db.get_access_request(tid)
        if req:
            await _notify_admins_new_request(bot, req)
        if query.message:
            try:
                await query.message.edit_reply_markup(reply_markup=None)
            except Exception:
                pass
            await query.message.answer(
                "Заявка отправлена. Когда её одобрят, ссылка на подписку придёт в этот чат."
            )
        await query.answer()
        return

    ok, sub, expiry_time_ms, err = await _create_subscription_for_user(
        tid, base_email, kind, slot_index
    )
    if not ok or sub is None or expiry_time_ms is None:
        await query.answer((err or "Ошибка")[:200], show_alert=True)
        return
    if query.message:
        try:
            await query.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
    link = _instruction_link(sub)
    if query.message:
        label = _device_subscription_label_from_parts(kind, slot_index)
        await query.message.answer(
            _subscription_message_text(
                label,
                expiry_time_ms,
                link,
            )
        )
    await query.answer("Готово")


@router.callback_query(F.data.startswith("apr:"))
async def cb_approve_access(query: CallbackQuery, bot: Bot) -> None:
    if not _is_admin(query.from_user.id if query.from_user else None):
        await query.answer("Нет прав.", show_alert=True)
        return
    try:
        tid = int(query.data.split(":", 1)[1])
    except (IndexError, ValueError):
        await query.answer("Неверные данные.", show_alert=True)
        return

    pending = await db.get_access_request(tid)
    if pending is None:
        await query.answer("Заявка уже обработана или отозвана.", show_alert=True)
        return

    ok, sub, expiry_time_ms, err = await _create_subscription_for_user(
        tid,
        pending.base_email,
        pending.device_kind,
        pending.slot_index,
    )
    if not ok or sub is None or expiry_time_ms is None:
        msg = (err or "Ошибка")[:180]
        await query.answer(msg, show_alert=True)
        return

    await db.delete_access_request(tid)
    await query.answer("Доступ выдан.")

    link = _instruction_link(sub)
    user_text = _subscription_message_text(
        _device_subscription_label_from_parts(
            pending.device_kind,
            pending.slot_index,
        ),
        expiry_time_ms,
        link,
    )
    delivered = False
    try:
        await bot.send_message(tid, user_text)
        delivered = True
    except Exception:
        logger.exception("Не удалось написать пользователю %s", tid)

    if query.message:
        try:
            await query.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
        try:
            if delivered:
                await query.message.reply(
                    f"Выдано пользователю <code>{tid}</code>.", parse_mode="HTML"
                )
            else:
                await query.message.reply(
                    f"Клиенты для <code>{tid}</code> созданы, в личку не доставлено "
                    f"(нужен чат с ботом). Ссылка:\n{link}",
                    parse_mode="HTML",
                )
        except Exception:
            pass


@router.callback_query(F.data.startswith("rej:"))
async def cb_reject_access(query: CallbackQuery, bot: Bot) -> None:
    if not _is_admin(query.from_user.id if query.from_user else None):
        await query.answer("Нет прав.", show_alert=True)
        return
    try:
        tid = int(query.data.split(":", 1)[1])
    except (IndexError, ValueError):
        await query.answer("Неверные данные.", show_alert=True)
        return

    pending = await db.get_access_request(tid)
    if pending is None:
        await query.answer("Заявка уже не активна.", show_alert=True)
        return

    await db.delete_access_request(tid)
    await query.answer("Отклонено.")

    try:
        await bot.send_message(
            tid,
            "Запрос не одобрен. Если вы считаете, что это ошибка — напишите администратору.",
        )
    except Exception:
        logger.exception("Не удалось уведомить пользователя %s об отказе", tid)

    if query.message:
        try:
            await query.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
        try:
            await query.message.reply(
                f"Отказ пользователю <code>{tid}</code>.", parse_mode="HTML"
            )
        except Exception:
            pass


@router.message(Command("stats"))
async def cmd_stats(message: Message) -> None:
    if not _is_admin(message.from_user.id if message.from_user else None):
        await message.answer("Команда только для администратора.")
        return
    u = await db.count_distinct_subscribers()
    d = await db.count_devices()
    p = await db.count_pending_requests()
    await message.answer(
        f"Уникальных пользователей: {u}\n"
        f"Всего конфигов (устройств): {d}\n"
        f"Заявок в ожидании: {p}"
    )


async def main() -> None:
    if not BOT_TOKEN:
        raise SystemExit("Укажите BOT_TOKEN в .env")
    if not PANEL_BASE_URL:
        raise SystemExit(
            "Укажите PANEL_BASE_URL в .env (URL до секретного префикса панели, без /panel/ на конце)."
        )
    if not SUBSCRIPTION_PATH:
        raise SystemExit(
            "Укажите SUBSCRIPTION_PATH в .env — сегмент пути из URL подписки в настройках 3x-ui."
        )

    await db.init_db()
    if _require_approval() and not _admin_ids():
        logger.warning(
            "REQUIRE_APPROVAL=1, но не заданы ADMINS/ADMIN_ID — заявки некому подтверждать"
        )
    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher()
    dp.include_router(router)
    reminder_task = asyncio.create_task(_subscription_reminder_worker(bot))
    logger.info("Бот запущен")
    try:
        await dp.start_polling(bot)
    finally:
        reminder_task.cancel()
        with suppress(asyncio.CancelledError):
            await reminder_task


if __name__ == "__main__":
    asyncio.run(main())
