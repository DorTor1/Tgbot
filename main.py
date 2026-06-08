"""Telegram-бот: регистрация в 3x-ui"""

from __future__ import annotations

import asyncio
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import datetime, timezone
import logging
import os
import re
import secrets
import string
import uuid
from pathlib import Path
from typing import Any
from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    CallbackQuery,
    CopyTextButton,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    LinkPreviewOptions,
    Message,
    ReplyKeyboardRemove,
    User,
)
from dotenv import load_dotenv

import bot_ui as ui
import db
import payments
from services import payments_service
from panel_api import (
    PANEL_API_BUILD,
    PanelAPI,
    PanelAPIError,
    build_subscription_link,
    expiry_time_ms_for_days,
    inbound_ids_config,
    panel_client_email,
    subscription_expiry_time_ms,
)

load_dotenv(Path(__file__).resolve().parent / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
ADMIN_ID_RAW = os.getenv("ADMIN_ID", "0").strip()
# Несколько админов: ADMINS=111,222,333 (приоритетнее одиночного ADMIN_ID)
ADMINS_RAW = os.getenv("ADMINS", "").strip()


@dataclass
class PanelConfig:
    """Конфиг одной 3x-ui панели (одного VPS)."""

    index: int
    name: str  # отображаемое название (гео)
    base_url: str
    login: str
    password: str
    api_token: str = ""  # Settings → Security → API Token (предпочтительно)
    sub_base_url: str = ""
    sub_path: str = ""
    sub_query_param: str = "name"
    sub_config_cache: dict[str, Any] = field(default_factory=dict)


def _panel_has_credentials(panel: PanelConfig) -> bool:
    return bool(panel.api_token or (panel.login and panel.password))


def _panel_api(panel: PanelConfig) -> PanelAPI:
    return PanelAPI(
        panel.base_url,
        panel.login,
        panel.password,
        api_token=panel.api_token,
    )


def _env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def _load_panels() -> list[PanelConfig]:
    """Мастер-панель из .env: PANEL_BASE_URL_1 / PANEL_API_TOKEN_1 или без суффикса _1."""
    panels: list[PanelConfig] = []
    for i in range(1, 21):
        base_url = _env(f"PANEL_BASE_URL_{i}").rstrip("/")
        if not base_url:
            continue
        login = _env(f"PANEL_LOGIN_{i}")
        password = _env(f"PANEL_PASSWORD_{i}")
        api_token = _env(f"PANEL_API_TOKEN_{i}") or _env("PANEL_API_TOKEN")
        name = _env(f"PANEL_NAME_{i}") or f"Сервер {i}"
        sub_base = _env(f"SUBSCRIPTION_BASE_URL_{i}").rstrip("/")
        sub_path = _env(f"SUBSCRIPTION_PATH_{i}")
        sub_qp = (
            _env(f"SUBSCRIPTION_QUERY_PARAM_{i}")
            or _env("SUBSCRIPTION_QUERY_PARAM")
            or "path"
        )
        panels.append(
            PanelConfig(
                index=i,
                name=name,
                base_url=base_url,
                login=login,
                password=password,
                api_token=api_token,
                sub_base_url=sub_base,
                sub_path=sub_path,
                sub_query_param=sub_qp,
            )
        )

    if panels:
        return panels

    base_url = _env("PANEL_BASE_URL").rstrip("/")
    if not base_url:
        return []
    return [
        PanelConfig(
            index=1,
            name=_env("PANEL_NAME") or "Сервер",
            base_url=base_url,
            login=_env("PANEL_LOGIN"),
            password=_env("PANEL_PASSWORD"),
            api_token=_env("PANEL_API_TOKEN"),
            sub_base_url=_env("SUBSCRIPTION_BASE_URL").rstrip("/"),
            sub_path=_env("SUBSCRIPTION_PATH"),
            sub_query_param=_env("SUBSCRIPTION_QUERY_PARAM") or "path",
        )
    ]


PANELS: list[PanelConfig] = _load_panels()


def _master_panel() -> PanelConfig | None:
    """Мастер-панель 3x-ui: первая в .env; ноды (США и т.д.) синхронизирует сама панель."""
    for panel in PANELS:
        if panel.base_url and _panel_has_credentials(panel):
            return panel
    return None


def _api_panels() -> list[PanelConfig]:
    """Мастер-панель 3x-ui (ноды синхронизирует сама панель)."""
    master = _master_panel()
    return [master] if master else []


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
GROUP_SIZE = int(os.getenv("GROUP_SIZE", "10"))


def is_lead_slot(global_slot_index: int) -> bool:
    """«Ведущий» (платный) слот в группе: 1, 11, 21, ... → True."""
    return global_slot_index == 1 or (global_slot_index - 1) % GROUP_SIZE == 0


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
    links: list[tuple[str, str]],
    renewal_note: str = "",
) -> str:
    """links: список (название_сервера, ссылка). Если панель одна — заголовок сервера не печатается."""
    header = ui.subscription_header(
        device_label,
        _format_expiry_time_ms(expiry_time_ms),
    )
    lines: list[str] = [header, ""]
    if links:
        lines.append(ui.SUBSCRIPTION_HOWTO)
        lines.append("")
        lines.append(ui.SUBSCRIPTION_LINK_LABEL)
        lines.append(links[0][1])
    if renewal_note:
        lines.append("")
        lines.append(renewal_note)
    return "\n".join(lines)


# Telegram: copy_text до 256 символов; callback_data — до 64 байт
_COPY_TEXT_MAX = 256
_CALLBACK_DATA_MAX = 64


def _inline_copy_button(
    url: str,
    *,
    device_kind: str | None = None,
    slot_index: int | None = None,
) -> InlineKeyboardButton | None:
    url = url.strip()
    if not url:
        return None
    if len(url) <= _COPY_TEXT_MAX:
        return InlineKeyboardButton(
            text=ui.BTN_COPY_LINK,
            copy_text=CopyTextButton(text=url),
        )
    if device_kind is not None and slot_index is not None:
        cb = f"cp:{device_kind}:{slot_index}"
        if len(cb.encode()) <= _CALLBACK_DATA_MAX:
            return InlineKeyboardButton(text=ui.BTN_COPY_LINK, callback_data=cb)
    return None


def _subscription_reply_keyboard(
    *,
    sub_token: str | None = None,
    device_label: str = "",
    device_kind: str | None = None,
    slot_index: int | None = None,
    show_renew: bool = False,
    back_subs: bool = False,
    back_menu: bool = False,
) -> InlineKeyboardMarkup | None:
    """Копирование ссылки + навигация."""
    rows: list[list[InlineKeyboardButton]] = []

    if sub_token:
        links = _all_links(sub_token)
        if links:
            copy_btn = _inline_copy_button(
                links[0][1],
                device_kind=device_kind,
                slot_index=slot_index,
            )
            if copy_btn:
                rows.append([copy_btn])

    if show_renew and device_kind is not None and slot_index is not None:
        rows.append(
            [
                InlineKeyboardButton(
                    text=ui.BTN_RENEW,
                    callback_data=f"rnw_req:{device_kind}:{slot_index}",
                )
            ]
        )

    nav: list[InlineKeyboardButton] = []
    if back_subs:
        nav.append(
            InlineKeyboardButton(text=ui.BTN_BACK_SUBS, callback_data="menu_my_subs")
        )
    if back_menu:
        nav.append(
            InlineKeyboardButton(text=ui.BTN_BACK_MENU, callback_data="menu_main")
        )
    if nav:
        rows.append(nav)

    return InlineKeyboardMarkup(inline_keyboard=rows) if rows else None


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


async def _refresh_sub_config() -> None:
    """Читает настройки подписки с мастер-панели (для мультиподписки 3x-ui)."""
    for panel in _api_panels():
        if not panel.base_url or not _panel_has_credentials(panel):
            continue
        if panel.sub_base_url:
            logger.info(
                "Панель %s (%s): ссылки подписки из .env "
                "(SUBSCRIPTION_BASE_URL_%s → %s/{subId})",
                panel.index,
                panel.name,
                panel.index,
                panel.sub_base_url.rstrip("/"),
            )
            continue
        try:
            async with _panel_api(panel) as api:
                cfg = await api.get_sub_config()
            if cfg:
                panel.sub_config_cache = cfg
                logger.info(
                    "Панель %s (%s): настройки подписки %s",
                    panel.index,
                    panel.name,
                    cfg,
                )
        except Exception as e:
            logger.warning(
                "Панель %s (%s): не удалось получить настройки подписки: %s",
                panel.index,
                panel.name,
                e,
            )


def _instruction_link(panel: PanelConfig, sub_token: str) -> str:
    """Ссылка подписки с мастер-панели (включая ноды при мультиподписке 3x-ui)."""
    return build_subscription_link(
        sub_token,
        panel_base_url=panel.base_url,
        sub_config=panel.sub_config_cache,
        sub_base_url=panel.sub_base_url,
        sub_path=panel.sub_path,
        sub_query_param=panel.sub_query_param,
    )


def _all_links(sub_token: str) -> list[tuple[str, str]]:
    """Прямая ссылка мультиподписки с мастер-панели 3x-ui."""
    master = _master_panel()
    if master is not None:
        return [(master.name, _instruction_link(master, sub_token))]
    return []


def _panels_configured() -> bool:
    return _master_panel() is not None


# URL страницы с публичной офертой и политикой конфиденциальности на сайте.
# Обязателен: без него бот не сможет показать пользователю ссылки на документы.
OFFER_URL = _env("OFFER_URL")


def _legal_text() -> str:
    """Текст согласия: короткое сообщение со ссылкой на оферту на сайте."""
    if not OFFER_URL:
        raise RuntimeError(
            "Задайте OFFER_URL в .env (URL страницы с офертой и пользовательским "
            "соглашением)."
        )
    return ui.offer_prompt(OFFER_URL)


def _greeting_name(user: User | None) -> str:
    if user is None:
        return "друг"
    if user.first_name:
        return user.first_name.strip() or "друг"
    if user.username:
        return f"@{user.username}"
    return "друг"


def _welcome_text(user: User | None) -> str:
    return ui.welcome(ui.e(_greeting_name(user)), approval=True)


def _main_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=ui.BTN_GET_ACCESS, callback_data="menu_get_access"
                ),
            ],
            [
                InlineKeyboardButton(text=ui.BTN_MY_SUBS, callback_data="menu_my_subs"),
            ],
        ]
    )


def _my_subs_keyboard(devices: list[db.UserDeviceRecord]) -> InlineKeyboardMarkup:
    buttons = []
    for d in devices:
        label = _device_subscription_label_from_parts(d.device_kind, d.slot_index)
        buttons.append(
            [
                InlineKeyboardButton(
                    text=f"🔹 {label}",
                    callback_data=f"sub_view:{d.device_kind}:{d.slot_index}",
                )
            ]
        )
    buttons.append(
        [InlineKeyboardButton(text=ui.BTN_BACK_MENU, callback_data="menu_main")]
    )
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def _terms_inline_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=ui.BTN_AGREE,
                    callback_data="terms:yes",
                ),
                InlineKeyboardButton(
                    text=ui.BTN_DECLINE,
                    callback_data="terms:no",
                ),
            ],
            [
                InlineKeyboardButton(
                    text=ui.BTN_BACK_MENU,
                    callback_data="menu_main",
                ),
            ],
        ]
    )


def _agreement_inline_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=ui.BTN_AGREE,
                    callback_data="agr:yes",
                ),
                InlineKeyboardButton(
                    text=ui.BTN_DECLINE,
                    callback_data="agr:no",
                ),
            ],
            [
                InlineKeyboardButton(
                    text=ui.BTN_BACK_MENU,
                    callback_data="menu_main",
                ),
            ],
        ]
    )


def _format_payment_who(record: db.PaymentRecord) -> str:
    parts: list[str] = []
    if record.username:
        parts.append(f"@{ui.e(record.username)}")
    name = " ".join(
        ui.e(x) for x in (record.first_name or "", record.last_name or "") if x
    ).strip()
    if name:
        parts.append(name)
    return " / ".join(parts) if parts else "без имени"


async def _notify_admins_new_payment(bot: Bot, record: db.PaymentRecord) -> None:
    """Уведомление админам о новом платеже (информативно)."""
    text = ui.ADMIN_NEW_PAYMENT.format(
        tid=record.telegram_id,
        who=_format_payment_who(record),
        device=_device_label_ru(record.device_kind),
        slot=record.slot_index,
        days=record.plan_days,
        amount=record.amount,
        payment_id=record.yookassa_payment_id,
    )
    for admin_id in _admin_ids():
        try:
            await bot.send_message(admin_id, text, parse_mode="HTML")
        except Exception:
            logger.exception("Не удалось уведомить админа %s о платеже", admin_id)


# C2: per-slot lock, чтобы в окне между create_pending и attach
# параллельный callback не сходил второй раз в ЮKassa.
_payment_creation_locks: dict[tuple[int, str, int], asyncio.Lock] = {}
_payment_creation_locks_guard = asyncio.Lock()


async def _get_payment_creation_lock(
    tid: int, kind: str, slot_index: int
) -> asyncio.Lock:
    key = (tid, kind, slot_index)
    async with _payment_creation_locks_guard:
        lock = _payment_creation_locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            _payment_creation_locks[key] = lock
        return lock


async def _create_subscription_for_user(
    tid: int,
    base_email: str,
    device_kind: str,
    slot_index: int,
    days: int | None = None,
    expiry_time_ms: int | None = None,
) -> tuple[bool, str | None, int | None, str]:
    """
    Создаёт клиентов в 3x-ui и запись в user_devices.
    days — срок подписки в днях (если None, берётся SUBSCRIPTION_DAYS).
    expiry_time_ms — явная дата истечения (ms). Если задана, перекрывает days.
    Возвращает (успех, sub_token, expiry_time_ms, текст ошибки для пользователя).
    """
    if not _panels_configured():
        return False, None, None, ui.ERR_NO_PANEL
    client_uuid = str(uuid.uuid4())
    sub = _sub_token()
    if expiry_time_ms is None:
        expiry_time_ms = (
            expiry_time_ms_for_days(days)
            if days is not None
            else subscription_expiry_time_ms()
        )
    api_panels = _api_panels()
    if not api_panels:
        return False, None, None, ui.ERR_NO_PANEL
    for panel in api_panels:
        try:
            async with _panel_api(panel) as api:
                await api.register_user_on_all_inbounds(
                    base_email,
                    client_uuid,
                    sub,
                    expiry_time_ms,
                    telegram_id=tid,
                )
        except PanelAPIError as e:
            logger.warning(
                "Ошибка панели %s (%s) для tg_id=%s: %s",
                panel.index,
                panel.name,
                tid,
                e,
            )
            return (
                False,
                None,
                None,
                f"Не удалось выдать подписку на «{panel.name}». "
                "Попробуйте позже или напишите администратору.",
            )
        except Exception:
            logger.exception(
                "Неожиданная ошибка при регистрации tg_id=%s на панели %s",
                tid,
                panel.index,
            )
            return False, None, None, ui.ERR_GENERIC
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


async def _send_subscription_reminder(
    bot: Bot, device: db.UserDeviceRecord, stage: str
) -> bool:
    label = _device_subscription_label_from_parts(device.device_kind, device.slot_index)
    expiry = _format_expiry_time_ms(device.expiry_time_ms)
    note = ui.REMINDER_NOTE
    if stage == "3d":
        text = ui.REMINDER_3D.format(label=label, expiry=expiry, note=note)
    elif stage == "1d":
        text = ui.REMINDER_1D.format(label=label, expiry=expiry, note=note)
    else:
        text = ui.REMINDER_EXPIRED.format(label=label, expiry=expiry, note=note)
    kb = _subscription_reply_keyboard(
        sub_token=device.sub_token,
        device_label=label,
        device_kind=device.device_kind,
        slot_index=device.slot_index,
        show_renew=True,
    )
    try:
        await bot.send_message(
            device.telegram_id, text, reply_markup=kb, parse_mode="HTML"
        )
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
        except Exception as exc:
            logger.exception("Ошибка фоновой проверки напоминаний: %s", exc)
        await asyncio.sleep(REMINDER_CHECK_INTERVAL_SECONDS)


async def _expire_old_payments_worker(bot: Bot) -> None:
    """Фоновая отмена «зависших» pending-платежей.

    Если юзер создал счёт и ушёл (не нажал «Отменить» и не оплатил),
    через PAYMENT_EXPIRES_SECONDS переводим запись в canceled,
    чтобы она не блокировала get_active_payment().

    C2: после SELECT expire_old_pending_payments возвращает все «старые»
    pending, но UPDATE мог пропустить часть из них (если параллельный
    webhook уже поставил succeeded). Поэтому перед каждым действием
    в ЮKassa/уведомлением перечитываем актуальный статус и реагируем
    только если он по-прежнему canceled.
    """
    check_interval = 60
    while True:
        try:
            expired = await db.expire_old_pending_payments(
                payments.PAYMENT_EXPIRES_SECONDS
            )
            for rec in expired:
                # rec.yookassa_payment_id может быть None, если ЮKassa
                # не ответила на этапе 2 (см. _create_payment_for_user) —
                # тогда и в ЮKassa отменять нечего, юзеру просто скажем
                # «счёт истёк».
                current = None
                if rec.yookassa_payment_id:
                    current = await db.get_payment_by_yookassa_id(
                        rec.yookassa_payment_id
                    )
                effective_status = current.status if current else rec.status
                if effective_status != "canceled":
                    # Webhook успел раньше воркера — ничего не делаем.
                    continue
                if rec.yookassa_payment_id:
                    try:
                        await asyncio.to_thread(
                            payments.cancel_payment, rec.yookassa_payment_id
                        )
                    except Exception:
                        logger.exception(
                            "Не удалось отменить счёт %s в ЮKassa",
                            rec.yookassa_payment_id,
                        )
                try:
                    await bot.send_message(
                        rec.telegram_id,
                        "⏰ Счёт на оплату истёк. Вы можете оформить новый.",
                    )
                except Exception:
                    logger.exception(
                        "Не удалось уведомить %s об истечении счёта", rec.telegram_id
                    )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Ошибка фоновой отмены просроченных платежей")
        await asyncio.sleep(check_interval)


@router.message(CommandStart())
async def cmd_start(message: Message) -> None:
    # Очищаем Reply-клавиатуру, если она была у пользователя
    msg = await message.answer("Запуск...", reply_markup=ReplyKeyboardRemove())
    with suppress(Exception):
        await msg.delete()

    text = _welcome_text(message.from_user)
    await message.answer(text, reply_markup=_main_keyboard(), parse_mode="HTML")


@router.message(F.text == "Получить доступ")
async def get_access_text(message: Message) -> None:
    # Очищаем Reply-клавиатуру, если она была у пользователя
    msg = await message.answer("Очистка меню...", reply_markup=ReplyKeyboardRemove())
    with suppress(Exception):
        await msg.delete()

    if message.from_user is None:
        await message.answer(ui.ERR_GENERIC)
        return

    if not _panels_configured():
        logger.error(
            "Не сконфигурирована ни одна панель (PANEL_BASE_URL_* / PANEL_LOGIN_* / PANEL_PASSWORD_*)"
        )
        await message.answer(ui.ERR_NO_PANEL)
        return

    tid = message.from_user.id
    if await db.get_active_payment(tid) is not None:
        await message.answer(
            ui.ERR_ALREADY_REQUESTED,
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [
                        InlineKeyboardButton(
                            text=ui.BTN_BACK_MENU, callback_data="menu_main"
                        )
                    ]
                ]
            ),
            parse_mode="HTML",
        )
        return

    if not await db.has_accepted_user_agreement(tid):
        await message.answer(
            _legal_text(),
            reply_markup=_agreement_inline_keyboard(),
        )
        return

    # Если новый слот будет lead'ом (1, 11, 21, ...) — сначала выбор тарифа.
    # Иначе — сразу выбор устройства (срок возьмётся от lead'а).
    total_devices = await db.count_user_devices(tid)
    new_global = total_devices + 1
    if is_lead_slot(new_global):
        await message.answer(
            ui.device_selection(approval=True),
            reply_markup=_plan_inline_keyboard(),
            parse_mode="HTML",
        )
    else:
        lead_global = ((new_global - 1) // GROUP_SIZE) * GROUP_SIZE + 1
        lead_dev = await db.get_user_device_by_global_slot(tid, lead_global)
        if lead_dev and lead_dev.expiry_time_ms:
            from datetime import datetime, timezone

            now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
            remaining_days = max(
                1, (lead_dev.expiry_time_ms - now_ms) // (24 * 60 * 60 * 1000)
            )
        else:
            remaining_days = 30
        text = (
            f"➕ <b>Добавление устройства</b>\n\n"
            f"Слот #{new_global} (бесплатный в группе).\n"
            f"Срок подписки привязан к ведущему слоту #{lead_global}: "
            f"<b>{remaining_days} дн.</b>\n\n"
            f"👇 <b>Выберите устройство:</b>"
        )
        await message.answer(
            text,
            reply_markup=_device_inline_keyboard_for_additional(),
            parse_mode="HTML",
        )


@router.callback_query(F.data == "menu_main")
async def cb_menu_main(query: CallbackQuery) -> None:
    if query.message:
        text = _welcome_text(query.from_user)
        with suppress(Exception):
            await query.message.edit_text(
                text, reply_markup=_main_keyboard(), parse_mode="HTML"
            )
    await query.answer()


@router.callback_query(F.data == "menu_get_access")
async def cb_menu_get_access(query: CallbackQuery) -> None:
    if query.from_user is None or not query.message:
        await query.answer()
        return

    tid = query.from_user.id
    if not _panels_configured():
        await query.answer(ui.ERR_NO_PANEL, show_alert=True)
        return

    if not await db.has_accepted_user_agreement(tid):
        with suppress(Exception):
            await query.message.edit_text(
                _legal_text(),
                reply_markup=_agreement_inline_keyboard(),
            )
        await query.answer()
        return

    if await db.get_active_payment(tid) is not None:
        with suppress(Exception):
            await query.message.edit_text(
                ui.ERR_ALREADY_REQUESTED,
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[
                        [
                            InlineKeyboardButton(
                                text=ui.BTN_BACK_MENU, callback_data="menu_main"
                            )
                        ]
                    ]
                ),
                parse_mode="HTML",
            )
        await query.answer()
        return

    with suppress(Exception):
        total_devices = await db.count_user_devices(tid)
        new_global = total_devices + 1
        if is_lead_slot(new_global):
            await query.message.edit_text(
                ui.device_selection(approval=True),
                reply_markup=_plan_inline_keyboard(),
                parse_mode="HTML",
            )
        else:
            lead_global = ((new_global - 1) // GROUP_SIZE) * GROUP_SIZE + 1
            lead_dev = await db.get_user_device_by_global_slot(tid, lead_global)
            if lead_dev and lead_dev.expiry_time_ms:
                from datetime import datetime, timezone

                now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
                remaining_days = max(
                    1, (lead_dev.expiry_time_ms - now_ms) // (24 * 60 * 60 * 1000)
                )
            else:
                remaining_days = 30
            text = (
                f"➕ <b>Добавление устройства</b>\n\n"
                f"Слот #{new_global} (бесплатный в группе).\n"
                f"Срок привязан к ведущему слоту #{lead_global}: "
                f"<b>{remaining_days} дн.</b>\n\n"
                f"👇 <b>Выберите устройство:</b>"
            )
            await query.message.edit_text(
                text,
                reply_markup=_device_inline_keyboard_for_additional(),
                parse_mode="HTML",
            )
    await query.answer()


@router.callback_query(F.data == "terms:yes")
async def cb_terms_accept(query: CallbackQuery) -> None:
    if query.from_user is None:
        await query.answer()
        return
    await db.set_rules_accepted(query.from_user.id)
    await query.answer(ui.RULES_ACCEPTED_TOAST)
    if query.message:
        with suppress(Exception):
            await query.message.edit_text(
                _legal_text(),
                reply_markup=_agreement_inline_keyboard(),
            )


@router.callback_query(F.data == "terms:no")
async def cb_terms_decline(query: CallbackQuery) -> None:
    if query.from_user is None:
        await query.answer()
        return
    await query.answer()
    if query.message:
        with suppress(Exception):
            await query.message.edit_text(
                ui.RULES_DECLINED,
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[
                        [
                            InlineKeyboardButton(
                                text=ui.BTN_GET_ACCESS, callback_data="menu_get_access"
                            )
                        ],
                        [
                            InlineKeyboardButton(
                                text=ui.BTN_BACK_MENU, callback_data="menu_main"
                            )
                        ],
                    ]
                ),
                parse_mode="HTML",
            )


@router.callback_query(F.data == "agr:yes")
async def cb_agreement_accept(query: CallbackQuery) -> None:
    if query.from_user is None:
        await query.answer()
        return
    # Принимаем сразу всё
    await db.set_rules_accepted(query.from_user.id)
    await db.set_agreement_accepted(query.from_user.id)
    await query.answer(ui.AGREEMENT_ACCEPTED_TOAST)
    if query.message:
        with suppress(Exception):
            await query.message.edit_text(
                ui.device_selection(approval=True),
                reply_markup=_plan_inline_keyboard(),
                parse_mode="HTML",
            )


@router.message(Command("admin_users"))
async def admin_list_users(message: Message) -> None:
    if not _is_admin(message.from_user.id if message.from_user else None):
        return

    users = await db.list_users_legal_status()
    if not users:
        await message.answer("Пользователей пока нет.")
        return

    lines = ["<b>Список пользователей и статус:</b>\n"]
    for u in users:
        status = "✅ Подписал" if u["accepted"] else "❌ Не подписал"
        devices = f"({u['devices']} устр.)" if u["devices"] > 0 else "(нет подписок)"

        user_display = f"<code>{u['tid']}</code>"
        if u["username"]:
            user_display = f"@{ui.e(u['username'])} ({user_display})"

        line = f"• {user_display}: {status} {devices}"
        lines.append(line)

    # Разбиваем на части, если список слишком длинный
    text = "\n".join(lines)
    if len(text) > 4000:
        for i in range(0, len(lines), 50):
            chunk = "\n".join(lines[i : i + 50])
            await message.answer(chunk, parse_mode="HTML")
    else:
        await message.answer(text, parse_mode="HTML")


@router.message(Command("user_info"))
async def admin_user_info(message: Message) -> None:
    if not _is_admin(message.from_user.id if message.from_user else None):
        return

    parts = message.text.split()
    if len(parts) < 2:
        await message.answer(
            "Использование: <code>/user_info [telegram_id]</code>", parse_mode="HTML"
        )
        return

    try:
        target_tid = int(parts[1])
    except ValueError:
        await message.answer("Некорректный ID.")
        return

    devices = await db.list_user_devices(target_tid)
    if not devices:
        await message.answer(
            f"У пользователя <code>{target_tid}</code> нет активных подписок.",
            parse_mode="HTML",
        )
        return

    lines = [f"<b>Подписки пользователя <code>{target_tid}</code>:</b>\n"]
    for d in devices:
        label = _device_subscription_label_from_parts(d.device_kind, d.slot_index)
        expiry = _format_expiry_time_ms(d.expiry_time_ms)
        lines.append(f"🔹 {label}")
        lines.append(f"   Срок: {expiry}")
        panel_email = panel_client_email(d.base_email, list(inbound_ids_config()))
        lines.append(f"   Email в панели: <code>{panel_email}</code>")
        lines.append(f"   Telegram ID (tgId): <code>{target_tid}</code>")
        lines.append(f"   UUID: <code>{d.uuid}</code>\n")

    await message.answer("\n".join(lines), parse_mode="HTML")


@router.message(Command("send"))
async def admin_send_to_user(message: Message, bot: Bot) -> None:
    """Админ отправляет пользователю текст от имени бота (прямая связь)."""
    if not _is_admin(message.from_user.id if message.from_user else None):
        return

    raw = (message.text or "").strip()
    parts = raw.split(maxsplit=2)
    if len(parts) < 3:
        await message.answer(
            "Использование:\n"
            "<code>/send TELEGRAM_ID текст сообщения</code>\n\n"
            "Текст может быть многострочным (всё после ID).\n"
            "Пользователь должен был хотя бы раз написать боту (/start).",
            parse_mode="HTML",
        )
        return

    try:
        target_tid = int(parts[1])
    except ValueError:
        await message.answer(
            "Некорректный Telegram ID (второй аргумент — целое число)."
        )
        return

    body = parts[2].strip()
    if not body:
        await message.answer("Текст сообщения пустой.")
        return

    prefix = ui.ADMIN_MSG_PREFIX
    try:
        await bot.send_message(target_tid, prefix + body)
    except Exception as e:
        logger.exception("Админ /send: не удалось доставить tg_id=%s", target_tid)
        await message.answer(
            f"Не удалось отправить пользователю <code>{target_tid}</code>.\n"
            f"Частые причины: пользователь не нажимал /start, заблокировал бота, неверный ID.\n"
            f"Технически: {ui.e(str(e))}"[:3500],
            parse_mode="HTML",
        )
        return

    await message.answer(
        f"Сообщение доставлено пользователю <code>{target_tid}</code>.",
        parse_mode="HTML",
    )


@router.callback_query(F.data == "agr:no")
async def cb_agreement_decline(query: CallbackQuery) -> None:
    if query.from_user is None:
        await query.answer()
        return
    await query.answer()
    if query.message:
        with suppress(Exception):
            await query.message.edit_text(
                ui.AGREEMENT_DECLINED,
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[
                        [
                            InlineKeyboardButton(
                                text=ui.BTN_GET_ACCESS, callback_data="menu_get_access"
                            )
                        ],
                        [
                            InlineKeyboardButton(
                                text=ui.BTN_BACK_MENU, callback_data="menu_main"
                            )
                        ],
                    ]
                ),
                parse_mode="HTML",
            )


@router.message(F.text == "Мои подписки")
async def my_subscriptions_text(message: Message) -> None:
    # Очищаем Reply-клавиатуру, если она была у пользователя
    msg = await message.answer("Очистка меню...", reply_markup=ReplyKeyboardRemove())
    with suppress(Exception):
        await msg.delete()

    if message.from_user is None:
        return
    tid = message.from_user.id
    devices = await db.list_user_devices(tid)
    if not devices:
        await message.answer(
            ui.NO_SUBS_YET,
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [
                        InlineKeyboardButton(
                            text=ui.BTN_GET_ACCESS, callback_data="menu_get_access"
                        )
                    ],
                    [
                        InlineKeyboardButton(
                            text=ui.BTN_BACK_MENU, callback_data="menu_main"
                        )
                    ],
                ]
            ),
            parse_mode="HTML",
        )
        return
    await message.answer(
        ui.SUBS_LIST_TITLE.format(count=len(devices)),
        reply_markup=_my_subs_keyboard(devices),
        parse_mode="HTML",
    )


@router.callback_query(F.data == "menu_my_subs")
async def cb_menu_my_subs(query: CallbackQuery) -> None:
    if query.from_user is None or not query.message:
        await query.answer()
        return
    tid = query.from_user.id
    devices = await db.list_user_devices(tid)
    if not devices:
        with suppress(Exception):
            await query.message.edit_text(
                ui.NO_SUBS_YET,
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[
                        [
                            InlineKeyboardButton(
                                text=ui.BTN_GET_ACCESS, callback_data="menu_get_access"
                            )
                        ],
                        [
                            InlineKeyboardButton(
                                text=ui.BTN_BACK_MENU, callback_data="menu_main"
                            )
                        ],
                    ]
                ),
                parse_mode="HTML",
            )
        await query.answer()
        return

    with suppress(Exception):
        await query.message.edit_text(
            ui.SUBS_LIST_TITLE.format(count=len(devices)),
            reply_markup=_my_subs_keyboard(devices),
            parse_mode="HTML",
        )
    await query.answer()


@router.callback_query(F.data.startswith("sub_view:"))
async def cb_sub_view(query: CallbackQuery) -> None:
    if query.from_user is None or not query.message:
        await query.answer()
        return
    parts = query.data.split(":")
    if len(parts) != 3:
        await query.answer(ui.ERR_BAD_DATA, show_alert=True)
        return
    _, device_kind, slot_raw = parts
    try:
        slot_index = int(slot_raw)
    except ValueError:
        await query.answer(ui.ERR_BAD_DATA, show_alert=True)
        return

    tid = query.from_user.id
    device = await db.get_user_device(tid, device_kind, slot_index)
    if device is None:
        await query.answer(ui.ERR_SUB_NOT_FOUND, show_alert=True)
        return

    pending = await db.get_active_payment(tid)
    show_renew = not (
        pending is not None
        and pending.device_kind == device_kind
        and pending.slot_index == slot_index
        and pending.kind == "renewal"
    )
    renewal_note = ui.RENEWAL_PENDING_NOTE if not show_renew else ""

    text = _subscription_message_text(
        _device_subscription_label_from_parts(device_kind, slot_index),
        device.expiry_time_ms,
        _all_links(device.sub_token),
        renewal_note=renewal_note,
    )

    label = _device_subscription_label_from_parts(device_kind, slot_index)
    kb = _subscription_reply_keyboard(
        sub_token=device.sub_token,
        device_label=label,
        device_kind=device_kind,
        slot_index=slot_index,
        show_renew=show_renew,
        back_subs=True,
        back_menu=True,
    )

    with suppress(Exception):
        await query.message.edit_text(
            text,
            reply_markup=kb,
            parse_mode="HTML",
        )
    await query.answer()


async def _device_subscription_url(
    tid: int,
    device_kind: str,
    slot_index: int,
) -> str | None:
    device = await db.get_user_device(tid, device_kind, slot_index)
    if device is None:
        return None
    links = _all_links(device.sub_token)
    if not links:
        return None
    return links[0][1]


@router.callback_query(F.data.startswith("cp:"))
async def cb_copy_link_fallback(query: CallbackQuery, bot: Bot) -> None:
    """Если ссылка длиннее 256 символов — copy_text недоступен, шлём в чат."""
    if query.from_user is None or not query.data:
        await query.answer()
        return
    parts = query.data.split(":")
    if len(parts) != 3:
        await query.answer(ui.ERR_BAD_DATA, show_alert=True)
        return
    _, device_kind, slot_raw = parts
    try:
        slot_index = int(slot_raw)
    except ValueError:
        await query.answer(ui.ERR_BAD_DATA, show_alert=True)
        return

    tid = query.from_user.id
    sub_url = await _device_subscription_url(tid, device_kind, slot_index)
    if not sub_url:
        await query.answer(ui.ERR_SUB_NOT_FOUND, show_alert=True)
        return
    await query.answer(ui.COPY_LINK_SENT, show_alert=False)
    try:
        await bot.send_message(
            tid,
            sub_url,
            link_preview_options=LinkPreviewOptions(is_disabled=True),
        )
    except Exception as exc:
        logger.exception("Не удалось отправить ссылку пользователю %s: %s", tid, exc)


@router.callback_query(F.data.startswith("rnw_req:"))
async def cb_renewal_request(query: CallbackQuery, bot: Bot) -> None:
    """Пользователь нажал «🔁 Продлить» → предлагаем выбрать тариф."""
    if query.from_user is None or not query.data:
        await query.answer()
        return
    parts = query.data.split(":")
    if len(parts) != 3:
        await query.answer(ui.ERR_BAD_DATA, show_alert=True)
        return
    _, device_kind, slot_raw = parts
    try:
        slot_index = int(slot_raw)
    except ValueError:
        await query.answer(ui.ERR_BAD_DATA, show_alert=True)
        return

    tid = query.from_user.id
    device = await db.get_user_device(tid, device_kind, slot_index)
    if device is None:
        await query.answer(ui.ERR_SUB_NOT_FOUND, show_alert=True)
        return

    # Шаг 1 для продления: показать клавиатуру тарифов с пометкой устройства
    prices = payments.load_plan_prices()
    rows: list[list[InlineKeyboardButton]] = []
    row: list[InlineKeyboardButton] = []
    for d in sorted(prices):
        amount = prices[d]
        cb = f"rnw_plan:{device_kind}:{slot_index}:{d}"
        row.append(
            InlineKeyboardButton(
                text=ui.BTN_PLAN_PREFIX.format(days=d, amount=amount),
                callback_data=cb,
            )
        )
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append(
        [InlineKeyboardButton(text=ui.BTN_BACK_SUBS, callback_data="menu_my_subs")]
    )
    kb = InlineKeyboardMarkup(inline_keyboard=rows)
    text = f"🔁 <b>Продление подписки</b>\n\n{ui.plan_selection()}"
    if query.message:
        with suppress(Exception):
            await query.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await query.answer()


@router.callback_query(F.data.startswith("rnw_plan:"))
async def cb_renewal_plan_chosen(query: CallbackQuery, bot: Bot) -> None:
    """Продление: выбран тариф → создаём платёж в ЮKassa."""
    if query.from_user is None:
        await query.answer()
        return
    parts = (query.data or "").split(":")
    if len(parts) != 4:
        await query.answer(ui.ERR_BAD_DATA, show_alert=True)
        return
    try:
        device_kind = parts[1]
        slot_index = int(parts[2])
        days = int(parts[3])
    except ValueError:
        await query.answer(ui.ERR_BAD_DATA, show_alert=True)
        return

    tid = query.from_user.id
    device = await db.get_user_device(tid, device_kind, slot_index)
    if device is None:
        await query.answer(ui.ERR_SUB_NOT_FOUND, show_alert=True)
        return

    # Определяем глобальный номер слота и lead'а в его группе
    global_slot = await db.get_user_global_slot_index(tid, device_kind, slot_index)
    if global_slot is None:
        await query.answer(ui.ERR_SUB_NOT_FOUND, show_alert=True)
        return
    lead_slot = ((global_slot - 1) // GROUP_SIZE) * GROUP_SIZE + 1

    # Любое продление = оплата (за всю десятку сразу продлевается)
    record, err = await _create_payment_for_user(
        bot,
        kind="renewal",
        query_from_user=query.from_user,
        days=days,
        device_kind=device_kind,
        slot_index=slot_index,
        base_email=device.base_email,
    )
    if record is None:
        await query.answer(err or ui.ERR_GENERIC, show_alert=True)
        return

    label = _device_subscription_label_from_parts(device_kind, slot_index)
    text = (
        f"🔁 <b>Счёт на продление</b>\n\n"
        f"🛒 Тариф: <b>{days} дней</b>\n"
        f"📱 Устройство: <b>{label}</b>\n"
        f"💵 К оплате: <b>{record.amount} ₽</b>\n\n"
        f"{ui.PAYMENT_CREATED}\n\n"
        f"💡 После оплаты автоматически продлятся все устройства группы "
        f"#{lead_slot}–#{lead_slot + GROUP_SIZE - 1}."
    )
    if query.message:
        with suppress(Exception):
            await query.message.edit_text(
                text, reply_markup=_payment_keyboard(record), parse_mode="HTML"
            )
    await query.answer(ui.PAYMENT_CREATED, show_alert=True)


def _plan_inline_keyboard() -> InlineKeyboardMarkup:
    """Шаг 1: выбор тарифа (срока подписки)."""
    prices = payments.load_plan_prices()
    rows: list[list[InlineKeyboardButton]] = []
    row: list[InlineKeyboardButton] = []
    for days in sorted(prices):
        amount = prices[days]
        row.append(
            InlineKeyboardButton(
                text=ui.BTN_PLAN_PREFIX.format(days=days, amount=amount),
                callback_data=f"plan:{days}",
            )
        )
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append(
        [InlineKeyboardButton(text=ui.BTN_BACK_MENU, callback_data="menu_main")]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _device_inline_keyboard_for_plan(days: int) -> InlineKeyboardMarkup:
    """Шаг 2: выбор устройства (после выбора тарифа)."""
    rows: list[list[InlineKeyboardButton]] = [
        [
            InlineKeyboardButton(
                text="📱 Смартфон", callback_data=f"plan_dev:{days}:phone"
            ),
            InlineKeyboardButton(
                text="💻 Ноутбук", callback_data=f"plan_dev:{days}:laptop"
            ),
        ],
        [
            InlineKeyboardButton(text="🖥 ПК", callback_data=f"plan_dev:{days}:pc"),
            InlineKeyboardButton(
                text="📟 Другое", callback_data=f"plan_dev:{days}:other"
            ),
        ],
        [
            InlineKeyboardButton(
                text=ui.BTN_BACK_TO_PLANS, callback_data="menu_get_access"
            ),
        ],
        [
            InlineKeyboardButton(text=ui.BTN_BACK_MENU, callback_data="menu_main"),
        ],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _device_inline_keyboard_for_additional() -> InlineKeyboardMarkup:
    """Выбор устройства для дополнительного слота (не lead, без тарифа)."""
    rows: list[list[InlineKeyboardButton]] = [
        [
            InlineKeyboardButton(text="📱 Смартфон", callback_data="add_dev:phone"),
            InlineKeyboardButton(text="💻 Ноутбук", callback_data="add_dev:laptop"),
        ],
        [
            InlineKeyboardButton(text="🖥 ПК", callback_data="add_dev:pc"),
            InlineKeyboardButton(text="📟 Другое", callback_data="add_dev:other"),
        ],
        [
            InlineKeyboardButton(text=ui.BTN_BACK_MENU, callback_data="menu_main"),
        ],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _payment_keyboard(record: db.PaymentRecord) -> InlineKeyboardMarkup:
    """Кнопки для оплаты + проверка статуса + отмена."""
    rows: list[list[InlineKeyboardButton]] = []
    if record.confirmation_url:
        rows.append(
            [
                InlineKeyboardButton(
                    text=ui.BTN_PAY.format(amount=record.amount),
                    url=record.confirmation_url,
                )
            ]
        )
    rows.append(
        [
            InlineKeyboardButton(
                text=ui.BTN_CHECK_PAYMENT,
                callback_data=f"pay_check:{record.yookassa_payment_id}",
            )
        ]
    )
    rows.append(
        [
            InlineKeyboardButton(
                text=ui.BTN_CANCEL_PAYMENT,
                callback_data=f"pay_cancel:{record.yookassa_payment_id}",
            )
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def _create_payment_for_user(
    bot: Bot,
    *,
    kind: str,
    query_from_user: User,
    days: int,
    device_kind: str,
    slot_index: int,
    base_email: str,
) -> tuple[db.PaymentRecord | None, str | None]:
    """Создаёт платёж в ЮKassa и запись в БД. Возвращает (record, error_text).

    C2: трёхшаговый flow вместо двух — сначала резервируем pending в БД
    по уникальному idempotence_key, потом дёргаем ЮKassa, потом привязываем
    payment_id. Плюс in-memory asyncio.Lock на слот пользователя, чтобы
    параллельный callback (двойной клик) не уехал в ЮKassa в окне между
    шагами 1 и 3. Это гарантирует, что останется ровно один pending и
    один счёт в ЮKassa, а не «висящий» второй счёт.
    """
    if not payments.is_configured():
        return None, "Оплата временно недоступна (ЮKassa не настроена)."

    tid = query_from_user.id
    try:
        amount = payments.plan_amount(days)
    except ValueError as e:
        return None, f"Тариф недоступен: {e}"

    # C2: per-slot lock, чтобы исключить окно между create_pending и attach.
    lock = await _get_payment_creation_lock(tid, device_kind, slot_index)
    async with lock:
        # 1. Создаём pending с уникальным ключом (атомарно).
        idempotence_key = (
            f"tg-{tid}-{device_kind}-{slot_index}-{uuid.uuid4().hex}"
        )
        record = await db.create_pending_payment_with_key(
            idempotence_key=idempotence_key,
            telegram_id=tid,
            username=query_from_user.username,
            first_name=query_from_user.first_name,
            last_name=query_from_user.last_name,
            kind=kind,
            device_kind=device_kind,
            slot_index=slot_index,
            base_email=base_email,
            plan_days=days,
            amount=amount,
        )
        if record is None:
            return None, ui.ERR_REQUEST_ALREADY
        if record.yookassa_payment_id is not None:
            # Уже создан ранее (другой параллельный запрос с тем же ключом
            # успел дойти до шага 3) — отдать готовый счёт.
            return record, None

        # 2. Создаём счёт в ЮKassa (async, не блокирует event loop).
        try:
            yookassa = await payments_service.create_payment_async(
                days=days,
                amount_rub=amount,
                telegram_id=tid,
                device_kind=device_kind,
                slot_index=slot_index,
            )
        except Exception as e:
            # Провалились — pending-запись в БД остаётся, через 30 минут её
            # подберёт expire_old_pending_payments и погасит. Это OK.
            logger.exception("Не удалось создать платёж в ЮKassa: %s", e)
            return None, "Не удалось создать счёт. Попробуйте позже."

        # 3. Привязываем yookassa_payment_id к нашей pending-записи.
        ok = await db.attach_yookassa_to_pending(
            idempotence_key=idempotence_key,
            yookassa_payment_id=yookassa["id"],
            confirmation_url=yookassa.get("confirmation_url"),
        )
        if not ok:
            # Параллельно либо webhook уже пометил succeeded, либо в БД пропала
            # запись (маловероятно). Не плодим дубль — отдадим то, что есть.
            existing = await db.get_payment_by_idempotence_key(idempotence_key)
            if existing is not None and existing.yookassa_payment_id is not None:
                return existing, None
            return None, ui.ERR_REQUEST_ALREADY

        record = await db.get_payment_by_idempotence_key(idempotence_key)
        if record is None:
            return None, ui.ERR_REQUEST_ALREADY

    # Уведомляем админов (информативно) — ВНЕ lock, чтобы не задерживать
    # других пользователей, если Telegram API тормозит.
    with suppress(Exception):
        await _notify_admins_new_payment(bot, record)

    return record, None


async def _extend_subscription_for_user(
    tid: int,
    device_kind: str,
    slot_index: int,
    days: int | None = None,
    target_expiry_ms: int | None = None,
) -> tuple[bool, int | None, str]:
    """Обновляет expiryTime клиента на панелях, где он есть, и в БД.

    Если передан target_expiry_ms — используется он (синхронизация всей группы).
    Иначе берётся сегодня + days.
    Панель без нужных инбаундов или inbound без клиента не рвёт продление.
    Возвращает (успех, новый_expiry_time_ms, текст ошибки для админа).
    """
    device = await db.get_user_device(tid, device_kind, slot_index)
    if device is None:
        return False, None, "Подписка пользователя не найдена в БД."
    if not _panels_configured():
        return False, None, "Нет сконфигурированных панелей."

    if target_expiry_ms is not None:
        new_expiry_ms = target_expiry_ms
    else:
        new_expiry_ms = expiry_time_ms_for_days(days or 30)
    renewed = False
    for panel in _api_panels():
        try:
            async with _panel_api(panel) as api:
                ok = await api.update_user_on_all_inbounds(
                    device.base_email,
                    device.uuid,
                    device.sub_token,
                    new_expiry_ms,
                    telegram_id=tid,
                )
                if ok:
                    renewed = True
                else:
                    logger.warning(
                        "Продление: клиент subId=%s не найден на %s (%s)",
                        device.sub_token,
                        panel.index,
                        panel.name,
                    )
        except PanelAPIError as e:
            logger.warning(
                "Продление: ошибка панели %s (%s) для tg_id=%s: %s",
                panel.index,
                panel.name,
                tid,
                e,
            )
            return False, None, f"Ошибка панели «{panel.name}»: {e}"
        except Exception:
            logger.exception(
                "Продление: неожиданная ошибка на панели %s для tg_id=%s",
                panel.index,
                tid,
            )
            return False, None, f"Неожиданная ошибка на панели «{panel.name}»."

    if not renewed:
        return (
            False,
            None,
            "Не удалось продлить: клиент не найден на мастер-панели. "
            "Проверьте PANEL_INBOUND_IDS и что подписка выдавалась после переустановки панели.",
        )

    await db.extend_device_expiry(tid, device_kind, slot_index, new_expiry_ms)
    return True, new_expiry_ms, ""


@router.callback_query(F.data.startswith("rnw_apr:"))
async def cb_renewal_approve(query: CallbackQuery, bot: Bot) -> None:
    # Удалено: продление теперь через оплату ЮKassa (webhook)
    await query.answer("Устаревшая кнопка.", show_alert=True)


@router.callback_query(F.data.startswith("rnw_rej:"))
async def cb_renewal_reject(query: CallbackQuery, bot: Bot) -> None:
    await query.answer("Устаревшая кнопка.", show_alert=True)


@router.callback_query(F.data.startswith("apr:"))
async def cb_approve_access(query: CallbackQuery, bot: Bot) -> None:
    await query.answer("Устаревшая кнопка.", show_alert=True)


@router.callback_query(F.data.startswith("rej:"))
async def cb_reject_access(query: CallbackQuery, bot: Bot) -> None:
    await query.answer("Устаревшая кнопка.", show_alert=True)


@router.callback_query(F.data.startswith("plan:"))
async def cb_plan_chosen(query: CallbackQuery) -> None:
    """Шаг 1 → шаг 2: выбран срок → выбор устройства."""
    if query.from_user is None or not query.message:
        await query.answer()
        return
    try:
        days = int((query.data or "").split(":", 1)[1])
    except (IndexError, ValueError):
        await query.answer(ui.ERR_BAD_DATA, show_alert=True)
        return
    prices = payments.load_plan_prices()
    if days not in prices:
        await query.answer(ui.ERR_BAD_DATA, show_alert=True)
        return
    if not await db.has_accepted_user_agreement(query.from_user.id):
        await query.answer(ui.ERR_NEED_AGREEMENT, show_alert=True)
        return
    if await db.get_active_payment(query.from_user.id) is not None:
        await query.answer(ui.ERR_ALREADY_REQUESTED, show_alert=True)
        return
    if not _panels_configured():
        await query.answer(ui.ERR_NO_PANEL, show_alert=True)
        return

    # Если новый слот не lead — тариф не нужен, сразу выбор устройства
    total_devices = await db.count_user_devices(query.from_user.id)
    new_global = total_devices + 1
    if not is_lead_slot(new_global):
        with suppress(Exception):
            await query.message.edit_text(
                "Слот бесплатный — срок возьмётся от ведущего. Выберите устройство:",
                reply_markup=_device_inline_keyboard_for_additional(),
                parse_mode="HTML",
            )
        await query.answer()
        return

    text = (
        f"{ui.device_selection(approval=True)}\n\n"
        f"🛒 <b>Тариф:</b> {days} дней — <b>{prices[days]} ₽</b>\n\n"
        f"👇 <b>Шаг 2 — ваше устройство:</b>"
    )
    with suppress(Exception):
        await query.message.edit_text(
            text, reply_markup=_device_inline_keyboard_for_plan(days), parse_mode="HTML"
        )
    await query.answer()


@router.callback_query(F.data.startswith("plan_dev:"))
async def cb_plan_device_chosen(query: CallbackQuery, bot: Bot) -> None:
    """Шаг 2: выбран тариф + устройство → создаём платёж ЮKassa (для lead)."""
    if query.from_user is None:
        await query.answer()
        return
    parts = (query.data or "").split(":")
    if len(parts) != 3:
        await query.answer(ui.ERR_BAD_DATA, show_alert=True)
        return
    try:
        days = int(parts[1])
        kind = parts[2]
    except ValueError:
        await query.answer(ui.ERR_BAD_DATA, show_alert=True)
        return
    if kind not in EMAIL_PREFIX:
        await query.answer(ui.ERR_BAD_DATA, show_alert=True)
        return
    if not await db.has_accepted_user_agreement(query.from_user.id):
        await query.answer(ui.ERR_NEED_AGREEMENT, show_alert=True)
        return
    if not _panels_configured():
        await query.answer(ui.ERR_NO_PANEL, show_alert=True)
        return

    nick = _sanitize_nick(query.from_user)
    n_same = await db.count_device_slots(query.from_user.id, kind)
    slot_index = n_same + 1
    base_email = _panel_base_email(nick, kind, slot_index)
    tid = query.from_user.id

    total_devices = await db.count_user_devices(tid)
    new_global_slot = total_devices + 1
    if not is_lead_slot(new_global_slot):
        # Не должно сюда попасть — для не-lead'а есть отдельный callback.
        await query.answer(ui.ERR_GENERIC, show_alert=True)
        return

    record, err = await _create_payment_for_user(
        bot,
        kind="new",
        query_from_user=query.from_user,
        days=days,
        device_kind=kind,
        slot_index=slot_index,
        base_email=base_email,
    )
    if record is None:
        await query.answer(err or ui.ERR_GENERIC, show_alert=True)
        return

    label = _device_subscription_label_from_parts(kind, slot_index)
    text = (
        f"💳 <b>Счёт на оплату</b>\n\n"
        f"🛒 Тариф: <b>{days} дней</b>\n"
        f"📱 Устройство: <b>{label}</b>\n"
        f"💵 К оплате: <b>{record.amount} ₽</b>\n\n"
        f"Слот #{new_global_slot} — ведущий в группе (платный).\n"
        f"Следующие 9 слотов в этой группе — бесплатные.\n\n"
        f"{ui.PAYMENT_CREATED}"
    )
    if query.message:
        with suppress(Exception):
            await query.message.edit_text(
                text, reply_markup=_payment_keyboard(record), parse_mode="HTML"
            )
    await query.answer(ui.PAYMENT_CREATED, show_alert=True)


@router.callback_query(F.data.startswith("add_dev:"))
async def cb_add_device_chosen(query: CallbackQuery, bot: Bot) -> None:
    """Добавление дополнительного устройства (не lead) — без тарифа и оплаты.

    Срок берётся от lead'а десятки, чтобы все слоты в группе
    истекали в один день.
    """
    if query.from_user is None:
        await query.answer()
        return
    parts = (query.data or "").split(":")
    if len(parts) != 2:
        await query.answer(ui.ERR_BAD_DATA, show_alert=True)
        return
    kind = parts[1]
    if kind not in EMAIL_PREFIX:
        await query.answer(ui.ERR_BAD_DATA, show_alert=True)
        return
    if not await db.has_accepted_user_agreement(query.from_user.id):
        await query.answer(ui.ERR_NEED_AGREEMENT, show_alert=True)
        return
    if not _panels_configured():
        await query.answer(ui.ERR_NO_PANEL, show_alert=True)
        return

    tid = query.from_user.id
    total_devices = await db.count_user_devices(tid)
    new_global_slot = total_devices + 1
    if is_lead_slot(new_global_slot):
        await query.answer(ui.ERR_GENERIC, show_alert=True)
        return

    lead_global = ((new_global_slot - 1) // GROUP_SIZE) * GROUP_SIZE + 1
    lead_dev = await db.get_user_device_by_global_slot(tid, lead_global)
    from datetime import datetime, timezone

    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    if (
        lead_dev is None
        or lead_dev.expiry_time_ms is None
        or lead_dev.expiry_time_ms <= now_ms
    ):
        await query.answer(
            "Сначала нужно продлить ведущий слот группы.",
            show_alert=True,
        )
        return

    nick = _sanitize_nick(query.from_user)
    n_same = await db.count_device_slots(tid, kind)
    slot_index = n_same + 1
    base_email = _panel_base_email(nick, kind, slot_index)

    ok, sub, expiry_ms, err_text = await _create_subscription_for_user(
        tid=tid,
        base_email=base_email,
        device_kind=kind,
        slot_index=slot_index,
        expiry_time_ms=lead_dev.expiry_time_ms,
    )
    if not ok:
        await query.answer(err_text or ui.ERR_GENERIC, show_alert=True)
        return
    label = _device_subscription_label_from_parts(kind, slot_index)
    text = (
        f"🎉 <b>Устройство добавлено</b>\n\n"
        f"📱 Устройство: <b>{label}</b>\n"
        f"⏰ Действует до: <b>{_format_expiry_time_ms(expiry_ms)}</b>\n\n"
        f"✅ Бесплатно. Срок привязан к ведущему слоту #{lead_global}."
    )
    if query.message:
        with suppress(Exception):
            await query.message.edit_text(
                text,
                reply_markup=_subscription_reply_keyboard(
                    sub_token=sub,
                    device_label=label,
                    device_kind=kind,
                    slot_index=slot_index,
                    show_renew=True,
                    back_subs=True,
                    back_menu=True,
                ),
                parse_mode="HTML",
            )
    await query.answer("Устройство добавлено!", show_alert=True)


@router.callback_query(F.data.startswith("pay_check:"))
async def cb_pay_check(query: CallbackQuery) -> None:
    """Кнопка «Проверить оплату»: опрашиваем ЮKassa, на случай задержки вебхука."""
    if query.from_user is None:
        await query.answer()
        return
    payment_id = (query.data or "").split(":", 1)[1]
    record = await db.get_payment_by_yookassa_id(payment_id)
    if record is None or record.telegram_id != query.from_user.id:
        await query.answer(ui.ERR_BAD_DATA, show_alert=True)
        return
    if record.status == "succeeded":
        await query.answer("Оплата уже получена — ссылка выше ✅", show_alert=True)
        return
    if record.status == "canceled":
        await query.answer(ui.PAYMENT_EXPIRED, show_alert=True)
        return
    try:
        info = await payments_service.get_payment_status_async(payment_id)
    except Exception:
        await query.answer("Не удалось проверить. Попробуйте позже.", show_alert=True)
        return
    if info["status"] == "succeeded":
        await query.answer(
            "Оплата прошла! Подписка активируется через пару секунд ✅", show_alert=True
        )
    else:
        await query.answer(
            f"Статус: {info['status']}. Если платили — подождите немного.",
            show_alert=True,
        )


@router.callback_query(F.data.startswith("pay_cancel:"))
async def cb_pay_cancel(query: CallbackQuery) -> None:
    """Пользователь сам отменил неоплаченный счёт."""
    if query.from_user is None:
        await query.answer()
        return
    payment_id = (query.data or "").split(":", 1)[1]
    record = await db.get_payment_by_yookassa_id(payment_id)
    if record is None or record.telegram_id != query.from_user.id:
        await query.answer(ui.ERR_BAD_DATA, show_alert=True)
        return
    if record.status != "pending":
        await query.answer("Счёт уже не активен.", show_alert=True)
        return
    await payments_service.cancel_payment_async(payment_id)
    await db.mark_payment_canceled(payment_id)
    await query.answer(ui.PAYMENT_CANCELED, show_alert=True)
    if query.message:
        with suppress(Exception):
            await query.message.edit_text(
                "❌ Счёт отменён.\n\n"
                "Когда захотите — нажмите «🚀 Получить VPN» и оформите заново.",
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[
                        [
                            InlineKeyboardButton(
                                text=ui.BTN_GET_ACCESS,
                                callback_data="menu_get_access",
                            )
                        ],
                        [
                            InlineKeyboardButton(
                                text=ui.BTN_BACK_MENU, callback_data="menu_main"
                            )
                        ],
                    ]
                ),
                parse_mode="HTML",
            )


@router.message(Command("stats"))
async def cmd_stats(message: Message) -> None:
    if not _is_admin(message.from_user.id if message.from_user else None):
        await message.answer("Команда только для администратора.")
        return
    u = await db.count_distinct_subscribers()
    d = await db.count_devices()
    p = await db.count_pending_payments()
    await message.answer(
        f"📊 <b>Статистика</b>\n\n"
        f"👥 Пользователей: <b>{u}</b>\n"
        f"📱 Устройств: <b>{d}</b>\n"
        f"💳 Неоплаченных счетов: <b>{p}</b>",
        parse_mode="HTML",
    )


async def main() -> None:
    if not BOT_TOKEN:
        raise SystemExit("Укажите BOT_TOKEN в .env")
    if not PANELS:
        raise SystemExit(
            "Укажите хотя бы одну панель: PANEL_BASE_URL_1 и PANEL_API_TOKEN_1 "
            "(или PANEL_LOGIN_1 / PANEL_PASSWORD_1; для одной панели — без суффикса _1)."
        )
    for p in PANELS:
        if not _panel_has_credentials(p):
            raise SystemExit(
                f"Для панели #{p.index} ({p.name}) задайте PANEL_API_TOKEN_{p.index} "
                f"или пару PANEL_LOGIN_{p.index} / PANEL_PASSWORD_{p.index}."
            )
    master = _master_panel()
    if master:
        logger.info(
            "Панель: #%s %s (%s), API /panel/api/clients/*",
            master.index,
            master.name,
            master.base_url,
        )
    if len(PANELS) > 1:
        ignored = [p for p in PANELS if p is not master]
        logger.warning(
            "В .env указано несколько панелей; используется только #%s. "
            "Лишние записи можно убрать: %s",
            master.index if master else "?",
            ", ".join(f"#{p.index} {p.name}" for p in ignored),
        )
    if master:
        if master.sub_base_url:
            logger.info(
                "Панель API: %s | Ссылки подписки: %s/{subId}",
                master.base_url,
                master.sub_base_url.rstrip("/"),
            )
        else:
            logger.info(
                "Ссылки на подписку: %s/{subId} (из PANEL_BASE_URL_%s; "
                "задайте SUBSCRIPTION_BASE_URL_%s, если URI подписки другой)",
                master.base_url.rstrip("/"),
                master.index,
                master.index,
            )

    await db.init_db()
    await _refresh_sub_config()
    if not payments.is_configured():
        logger.warning(
            "ЮKassa не настроена (нет YOOKASSA_SHOP_ID / YOOKASSA_SECRET_KEY) — "
            "оплата работать не будет."
        )
    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher()
    dp.include_router(router)
    reminder_task = asyncio.create_task(_subscription_reminder_worker(bot))
    expire_task = asyncio.create_task(_expire_old_payments_worker(bot))
    logger.info("Бот запущен (panel_api build=%s)", PANEL_API_BUILD)
    try:
        await dp.start_polling(bot)
    finally:
        reminder_task.cancel()
        expire_task.cancel()
        with suppress(asyncio.CancelledError):
            await reminder_task
        with suppress(asyncio.CancelledError):
            await expire_task


if __name__ == "__main__":
    asyncio.run(main())
