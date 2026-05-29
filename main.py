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
from urllib.parse import quote

from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    User,
)
from dotenv import load_dotenv

import db
from panel_api import (
    PANEL_API_BUILD,
    PanelAPI,
    PanelAPIError,
    build_subscription_link,
    expiry_time_ms_for_days,
    inbound_ids_config,
    panel_client_email,
    subscription_days,
    subscription_expiry_time_ms,
)
from vpn_legal import LEGAL_TEXT

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

# Варианты срока подписки (в днях), которые админ выбирает кнопкой при одобрении.
APPROVAL_DURATION_CHOICES: tuple[int, ...] = (7, 30, 90, 180, 365)


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
        sub_qp = _env(f"SUBSCRIPTION_QUERY_PARAM_{i}") or "name"
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
            sub_query_param=_env("SUBSCRIPTION_QUERY_PARAM") or "name",
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


# HTML-страница с инструкцией (репозиторий: index.html). Без ?name= на конце.
# Пример: https://macbookairm4.mooo.com/Oab4HaruLF — бот добавит ?name=<sub_token>.
# Страницу нужно отдавать nginx’ом с вашего index.html; иначе откроется портал 3x-ui.
SUBSCRIPTION_PORTAL_BASE = _env("SUBSCRIPTION_PORTAL_BASE").rstrip("/")

# Когда в .env задан SUBSCRIPTION_PORTAL_BASE — в Telegram короче (детали на странице).
SUBSCRIPTION_TG_INSTRUCTION_PORTAL = (
    "📋 Откройте ссылку ниже в браузере: на странице инструкция под разные системы "
    "и кнопки импорта в клиенты."
)

# Мультиподписка 3x-ui 3.2+ (мастер + ноды в одной ссылке).
SUBSCRIPTION_TG_INSTRUCTION_MULTISUB = """📋 Как подключиться

1. Установите клиент с поддержкой подписок — v2RayTun, Hiddify, v2rayN и т.п.

2. Добавьте подписку по ссылке из сообщения ниже («Подписка по URL» / Import from URL).

3. Обновите список узлов — появятся серверы со всех локаций (мастер и подключённые ноды).

4. Выберите узел и включите VPN."""


def _subscription_portal_link(sub_token: str) -> str:
    enc = quote(sub_token, safe="")
    base = SUBSCRIPTION_PORTAL_BASE
    return f"{base}&name={enc}" if "?" in base else f"{base}?name={enc}"


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
    links: list[tuple[str, str]],
    renewal_note: str = "",
) -> str:
    """links: список (название_сервера, ссылка). Если панель одна — заголовок сервера не печатается."""
    header = (
        f"{device_label}\n"
        f"Подписка действует до: {_format_expiry_time_ms(expiry_time_ms)}"
    )
    lines: list[str] = [header, ""]
    if len(links) == 1:
        if SUBSCRIPTION_PORTAL_BASE:
            lines.append(SUBSCRIPTION_TG_INSTRUCTION_PORTAL)
            lines.append("")
            lines.append("Страница с инструкцией и подпиской:")
        elif _master_panel() is not None:
            lines.append(SUBSCRIPTION_TG_INSTRUCTION_MULTISUB)
            lines.append("")
            lines.append("Ссылка на мультиподписку (все локации в одном профиле):")
        else:
            lines.append("Ссылка на подписку:")
        lines.append(links[0][1])
    else:
        lines.append("Ссылки на подписку (добавьте обе в клиент):")
        for name, link in links:
            lines.append("")
            lines.append(f"🌍 {name}:")
            lines.append(link)
    if renewal_note:
        lines.append("")
        lines.append(renewal_note)
    return "\n".join(lines)


def _renew_device_keyboard(device_kind: str, slot_index: int) -> InlineKeyboardMarkup:
    """Кнопка «🔁 Продлить» под сообщением с конкретной подпиской устройства."""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🔁 Продлить",
                    callback_data=f"rnw_req:{device_kind}:{slot_index}",
                )
            ]
        ]
    )


def _renewal_review_keyboard(tid: int, device_kind: str, slot_index: int) -> InlineKeyboardMarkup:
    """Клавиатура для админа: выбрать срок продления или отклонить."""
    rows: list[list[InlineKeyboardButton]] = []
    row: list[InlineKeyboardButton] = []
    for days in APPROVAL_DURATION_CHOICES:
        row.append(
            InlineKeyboardButton(
                text=f"✅ {days} дн.",
                callback_data=f"rnw_apr:{tid}:{device_kind}:{slot_index}:{days}",
            )
        )
        if len(row) == 3:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append(
        [
            InlineKeyboardButton(
                text="❌ Отклонить",
                callback_data=f"rnw_rej:{tid}:{device_kind}:{slot_index}",
            )
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


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
        if panel.sub_base_url and panel.sub_path:
            logger.info(
                "Панель %s (%s): ссылки подписки из .env "
                "(SUBSCRIPTION_BASE_URL_%s / SUBSCRIPTION_PATH_%s)",
                panel.index,
                panel.name,
                panel.index,
                panel.index,
            )
            continue
        try:
            async with _panel_api(panel) as api:
                cfg = await api.get_sub_config()
            if cfg:
                panel.sub_config_cache = cfg
                logger.info("Панель %s (%s): настройки подписки %s", panel.index, panel.name, cfg)
        except Exception as e:
            logger.warning(
                "Панель %s (%s): не удалось получить настройки подписки: %s",
                panel.index, panel.name, e,
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
    """Мультиподписка с мастер-панели или HTML-портал из .env."""
    if SUBSCRIPTION_PORTAL_BASE:
        return [("Инструкция и подписка", _subscription_portal_link(sub_token))]
    master = _master_panel()
    if master is not None:
        return [(master.name, _instruction_link(master, sub_token))]
    return []


def _panels_configured() -> bool:
    return _master_panel() is not None


def _greeting_name(user: User | None) -> str:
    if user is None:
        return "друг"
    if user.first_name:
        return user.first_name.strip() or "друг"
    if user.username:
        return f"@{user.username}"
    return "друг"


def _welcome_text(user: User | None) -> str:
    name = _greeting_name(user)
    approval_note = (
        "\n\nЗаявку перед выдачей может подтвердить администратор — ссылка придёт в этот чат."
        if _require_approval()
        else ""
    )
    return (
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


def _main_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="💳 Получить доступ", callback_data="menu_get_access"),
            ],
            [
                InlineKeyboardButton(text="📱 Мои подписки", callback_data="menu_my_subs"),
            ],
        ]
    )


def _my_subs_keyboard(devices: list[db.UserDeviceRecord]) -> InlineKeyboardMarkup:
    buttons = []
    for d in devices:
        label = _device_subscription_label_from_parts(d.device_kind, d.slot_index)
        buttons.append([
            InlineKeyboardButton(
                text=f"🔹 {label}",
                callback_data=f"sub_view:{d.device_kind}:{d.slot_index}"
            )
        ])
    buttons.append([
        InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="menu_main")
    ])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


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
            [
                InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="menu_main"),
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
            [
                InlineKeyboardButton(
                    text="⬅️ Назад в меню",
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
                    text="✅ Принимаю всё",
                    callback_data="agr:yes",
                ),
                InlineKeyboardButton(
                    text="❌ Не согласен",
                    callback_data="agr:no",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="⬅️ Назад в меню",
                    callback_data="menu_main",
                ),
            ],
        ]
    )


def _access_review_keyboard(target_telegram_id: int) -> InlineKeyboardMarkup:
    """Клавиатура одобрения заявки: срок выбирает админ (дни)."""
    rows: list[list[InlineKeyboardButton]] = []
    row: list[InlineKeyboardButton] = []
    for days in APPROVAL_DURATION_CHOICES:
        row.append(
            InlineKeyboardButton(
                text=f"✅ {days} дн.",
                callback_data=f"apr:{target_telegram_id}:{days}",
            )
        )
        if len(row) == 3:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append(
        [
            InlineKeyboardButton(
                text="❌ Отклонить",
                callback_data=f"rej:{target_telegram_id}",
            )
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


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
        f"Префикс email в панели: <code>{req.base_email}</code>\n\n"
        "Выберите срок подписки или отклоните заявку:"
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
    days: int | None = None,
) -> tuple[bool, str | None, int | None, str]:
    """
    Создаёт клиентов в 3x-ui и запись в user_devices.
    days — срок подписки в днях (если None, берётся SUBSCRIPTION_DAYS).
    Возвращает (успех, sub_token, expiry_time_ms, текст ошибки для пользователя).
    """
    if not _panels_configured():
        return False, None, None, "Сейчас выдать ссылку нельзя. Напишите администратору."
    client_uuid = str(uuid.uuid4())
    sub = _sub_token()
    expiry_time_ms = (
        expiry_time_ms_for_days(days) if days is not None else subscription_expiry_time_ms()
    )
    api_panels = _api_panels()
    if not api_panels:
        return False, None, None, "Сейчас выдать ссылку нельзя. Напишите администратору."
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
                panel.index, panel.name, tid, e,
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
                tid, panel.index,
            )
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
    note = "Нажмите «🔁 Продлить», чтобы отправить заявку администратору."
    if stage == "3d":
        text = (
            f"Напоминание: подписка {label} закончится через 3 дня.\n"
            f"Окончание: {expiry}\n\n"
            f"{note}"
        )
    elif stage == "1d":
        text = (
            f"Напоминание: подписка {label} закончится через 1 день.\n"
            f"Окончание: {expiry}\n\n"
            f"{note}"
        )
    else:
        text = (
            f"Подписка {label} закончилась.\n"
            f"Окончание: {expiry}\n\n"
            f"{note}"
        )
    kb = _renew_device_keyboard(device.device_kind, device.slot_index)
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
    # Очищаем Reply-клавиатуру, если она была у пользователя
    msg = await message.answer("Запуск...", reply_markup=ReplyKeyboardRemove())
    with suppress(Exception):
        await msg.delete()

    text = _welcome_text(message.from_user)
    await message.answer(text, reply_markup=_main_keyboard())


@router.message(F.text == "Получить доступ")
async def get_access_text(message: Message) -> None:
    # Очищаем Reply-клавиатуру, если она была у пользователя
    msg = await message.answer("Очистка меню...", reply_markup=ReplyKeyboardRemove())
    with suppress(Exception):
        await msg.delete()

    if message.from_user is None:
        await message.answer("Что-то пошло не так. Попробуйте ещё раз.")
        return

    if not _panels_configured():
        logger.error("Не сконфигурирована ни одна панель (PANEL_BASE_URL_* / PANEL_LOGIN_* / PANEL_PASSWORD_*)")
        await message.answer(
            "Сейчас выдать доступ нельзя. Напишите администратору."
        )
        return

    tid = message.from_user.id
    if await db.get_access_request(tid):
        await message.answer(
            "Мы уже получили вашу заявку. Ожидайте ответа.",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="menu_main")]
                ]
            )
        )
        return

    if not await db.has_accepted_user_agreement(tid):
        await message.answer(
            LEGAL_TEXT,
            reply_markup=_agreement_inline_keyboard(),
        )
        return

    await message.answer(
        _device_selection_text(),
        reply_markup=_device_inline_keyboard(),
        parse_mode="HTML",
    )


@router.callback_query(F.data == "menu_main")
async def cb_menu_main(query: CallbackQuery) -> None:
    if query.message:
        text = _welcome_text(query.from_user)
        with suppress(Exception):
            await query.message.edit_text(text, reply_markup=_main_keyboard())
    await query.answer()


@router.callback_query(F.data == "menu_get_access")
async def cb_menu_get_access(query: CallbackQuery) -> None:
    if query.from_user is None or not query.message:
        await query.answer()
        return

    tid = query.from_user.id
    if not _panels_configured():
        await query.answer("Сейчас выдать доступ нельзя. Напишите администратору.", show_alert=True)
        return

    if await db.get_access_request(tid):
        with suppress(Exception):
            await query.message.edit_text(
                "Мы уже получили вашу заявку. Ожидайте ответа.",
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[
                        [InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="menu_main")]
                    ]
                )
            )
        await query.answer()
        return

    if not await db.has_accepted_user_agreement(tid):
        with suppress(Exception):
            await query.message.edit_text(
                LEGAL_TEXT,
                reply_markup=_agreement_inline_keyboard(),
            )
        await query.answer()
        return

    with suppress(Exception):
        await query.message.edit_text(
            _device_selection_text(),
            reply_markup=_device_inline_keyboard(),
            parse_mode="HTML",
        )
    await query.answer()


@router.callback_query(F.data == "terms:yes")
async def cb_terms_accept(query: CallbackQuery) -> None:
    if query.from_user is None:
        await query.answer()
        return
    await db.set_rules_accepted(query.from_user.id)
    await query.answer("Правила приняты")
    if query.message:
        with suppress(Exception):
            await query.message.edit_text(
                LEGAL_TEXT,
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
                "Пока вы не примете правила, оформить подписку нельзя. "
                "Когда будете готовы — вы сможете вернуться к соглашению.",
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[
                        [InlineKeyboardButton(text="💳 Получить доступ", callback_data="menu_get_access")],
                        [InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="menu_main")]
                    ]
                )
            )


@router.callback_query(F.data == "agr:yes")
async def cb_agreement_accept(query: CallbackQuery) -> None:
    if query.from_user is None:
        await query.answer()
        return
    # Принимаем сразу всё
    await db.set_rules_accepted(query.from_user.id)
    await db.set_agreement_accepted(query.from_user.id)
    await query.answer("Условия приняты")
    if query.message:
        with suppress(Exception):
            await query.message.edit_text(
                _device_selection_text(),
                reply_markup=_device_inline_keyboard(),
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
            user_display = f"@{u['username']} ({user_display})"
            
        line = f"• {user_display}: {status} {devices}"
        lines.append(line)

    # Разбиваем на части, если список слишком длинный
    text = "\n".join(lines)
    if len(text) > 4000:
        for i in range(0, len(lines), 50):
            chunk = "\n".join(lines[i:i+50])
            await message.answer(chunk, parse_mode="HTML")
    else:
        await message.answer(text, parse_mode="HTML")


@router.message(Command("user_info"))
async def admin_user_info(message: Message) -> None:
    if not _is_admin(message.from_user.id if message.from_user else None):
        return

    parts = message.text.split()
    if len(parts) < 2:
        await message.answer("Использование: <code>/user_info [telegram_id]</code>", parse_mode="HTML")
        return

    try:
        target_tid = int(parts[1])
    except ValueError:
        await message.answer("Некорректный ID.")
        return

    devices = await db.list_user_devices(target_tid)
    if not devices:
        await message.answer(f"У пользователя <code>{target_tid}</code> нет активных подписок.", parse_mode="HTML")
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
        await message.answer("Некорректный Telegram ID (второй аргумент — целое число).")
        return

    body = parts[2].strip()
    if not body:
        await message.answer("Текст сообщения пустой.")
        return

    prefix = "💬 Сообщение от администратора:\n\n"
    try:
        await bot.send_message(target_tid, prefix + body)
    except Exception as e:
        logger.exception("Админ /send: не удалось доставить tg_id=%s", target_tid)
        await message.answer(
            f"Не удалось отправить пользователю <code>{target_tid}</code>.\n"
            f"Частые причины: пользователь не нажимал /start, заблокировал бота, неверный ID.\n"
            f"Технически: {e!s}"[:3500],
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
                "Без принятия условий использование Сервиса невозможно. "
                "Если передумаете — вы всегда можете принять их позже.",
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[
                        [InlineKeyboardButton(text="💳 Получить доступ", callback_data="menu_get_access")],
                        [InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="menu_main")]
                    ]
                )
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
            "Пока нет ссылок. Нажмите «Получить доступ», когда будете готовы.",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="💳 Получить доступ", callback_data="menu_get_access")],
                    [InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="menu_main")]
                ]
            )
        )
        return
    await message.answer(
        f"Ваши активные подписки ({len(devices)}):",
        reply_markup=_my_subs_keyboard(devices),
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
                "Пока нет ссылок. Нажмите «Получить доступ», когда будете готовы.",
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[
                        [InlineKeyboardButton(text="💳 Получить доступ", callback_data="menu_get_access")],
                        [InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="menu_main")]
                    ]
                )
            )
        await query.answer()
        return

    with suppress(Exception):
        await query.message.edit_text(
            f"Ваши активные подписки ({len(devices)}):",
            reply_markup=_my_subs_keyboard(devices),
        )
    await query.answer()


@router.callback_query(F.data.startswith("sub_view:"))
async def cb_sub_view(query: CallbackQuery) -> None:
    if query.from_user is None or not query.message:
        await query.answer()
        return
    parts = query.data.split(":")
    if len(parts) != 3:
        await query.answer("Некорректные данные.", show_alert=True)
        return
    _, device_kind, slot_raw = parts
    try:
        slot_index = int(slot_raw)
    except ValueError:
        await query.answer("Некорректные данные.", show_alert=True)
        return

    tid = query.from_user.id
    device = await db.get_user_device(tid, device_kind, slot_index)
    if device is None:
        await query.answer("Подписка не найдена.", show_alert=True)
        return

    req = await db.get_renewal_request(tid, device_kind, slot_index)
    renewal_note = ""
    if req:
        renewal_note = "⏳ Заявка на продление отправлена и ожидает одобрения администратора."

    text = _subscription_message_text(
        _device_subscription_label_from_parts(device_kind, slot_index),
        device.expiry_time_ms,
        _all_links(device.sub_token),
        renewal_note=renewal_note,
    )

    buttons = []
    if not req:
        buttons.append([
            InlineKeyboardButton(
                text="🔁 Продлить",
                callback_data=f"rnw_req:{device_kind}:{slot_index}",
            )
        ])
    buttons.append([
        InlineKeyboardButton(text="⬅️ К списку подписок", callback_data="menu_my_subs"),
        InlineKeyboardButton(text="⬅️ В меню", callback_data="menu_main"),
    ])

    with suppress(Exception):
        await query.message.edit_text(
            text,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        )
    await query.answer()


@router.callback_query(F.data.startswith("rnw_req:"))
async def cb_renewal_request(query: CallbackQuery, bot: Bot) -> None:
    """Пользователь нажал «🔁 Продлить» у конкретной подписки."""
    if query.from_user is None or not query.data:
        await query.answer()
        return
    parts = query.data.split(":")
    if len(parts) != 3:
        await query.answer("Некорректные данные.", show_alert=True)
        return
    _, device_kind, slot_raw = parts
    try:
        slot_index = int(slot_raw)
    except ValueError:
        await query.answer("Некорректные данные.", show_alert=True)
        return

    tid = query.from_user.id
    device = await db.get_user_device(tid, device_kind, slot_index)
    if device is None:
        await query.answer("Подписка не найдена.", show_alert=True)
        return

    inserted = await db.try_insert_renewal_request(
        tid,
        device_kind,
        slot_index,
        query.from_user.username,
        query.from_user.first_name,
        query.from_user.last_name,
    )
    if not inserted:
        await query.answer(
            "Заявка на продление уже отправлена. Ожидайте ответа администратора.",
            show_alert=True,
        )
        return

    req = await db.get_renewal_request(tid, device_kind, slot_index)
    if req is not None:
        await _notify_admins_renewal_request(bot, req)

    await query.answer("Заявка отправлена", show_alert=True)
    if query.message:
        # Если это было детальное окно подписки, мы можем обновить его
        if "Ссылка на подписку" in (query.message.text or "") or "Ссылки на подписку" in (query.message.text or "") or "Страница с инструкцией" in (query.message.text or ""):
            text = _subscription_message_text(
                _device_subscription_label_from_parts(device_kind, slot_index),
                device.expiry_time_ms,
                _all_links(device.sub_token),
                renewal_note="⏳ Заявка на продление отправлена и ожидает одобрения администратора. Администратор свяжется с вами по оплате.",
            )
            buttons = [
                [
                    InlineKeyboardButton(text="⬅️ К списку подписок", callback_data="menu_my_subs"),
                    InlineKeyboardButton(text="⬅️ В меню", callback_data="menu_main"),
                ]
            ]
            with suppress(Exception):
                await query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))
        else:
            # Для других сообщений (например, автоматических напоминаний) просто дописываем статус
            with suppress(Exception):
                await query.message.edit_text(
                    text=f"{query.message.text}\n\n⏳ Заявка на продление отправлена. Администратор свяжется с вами по оплате.",
                    reply_markup=None
                )


async def _notify_admins_renewal_request(
    bot: Bot, req: db.RenewalRequestRecord
) -> None:
    who_parts: list[str] = []
    if req.username:
        who_parts.append(f"@{req.username}")
    name = " ".join(x for x in (req.first_name or "", req.last_name or "") if x).strip()
    if name:
        who_parts.append(name)
    who = " / ".join(who_parts) if who_parts else "без имени"
    text = (
        "🔁 Запрос на продление\n"
        f"Telegram ID: <code>{req.telegram_id}</code>\n"
        f"Кто: {who}\n"
        f"Устройство: {_device_label_ru(req.device_kind)} (слот {req.slot_index})\n"
        f"Текущий срок: {_format_expiry_time_ms(req.current_expiry_time_ms)}\n\n"
        "Напишите пользователю по оплате, затем выберите срок продления:"
    )
    kb = _renewal_review_keyboard(req.telegram_id, req.device_kind, req.slot_index)
    for admin_id in _admin_ids():
        try:
            await bot.send_message(admin_id, text, reply_markup=kb, parse_mode="HTML")
        except Exception:
            logger.exception(
                "Не удалось отправить заявку на продление админу %s", admin_id
            )


async def _extend_subscription_for_user(
    tid: int,
    device_kind: str,
    slot_index: int,
    days: int,
) -> tuple[bool, int | None, str]:
    """Обновляет expiryTime клиента на панелях, где он есть, и в БД.

    Панель без нужных инбаундов или inbound без клиента не рвёт продление.
    Возвращает (успех, новый_expiry_time_ms, текст ошибки для админа).
    """
    device = await db.get_user_device(tid, device_kind, slot_index)
    if device is None:
        return False, None, "Подписка пользователя не найдена в БД."
    if not _panels_configured():
        return False, None, "Нет сконфигурированных панелей."

    new_expiry_ms = expiry_time_ms_for_days(days)
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
                panel.index, panel.name, tid, e,
            )
            return False, None, f"Ошибка панели «{panel.name}»: {e}"
        except Exception:
            logger.exception(
                "Продление: неожиданная ошибка на панели %s для tg_id=%s",
                panel.index, tid,
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
    if not _is_admin(query.from_user.id if query.from_user else None):
        await query.answer("Нет прав.", show_alert=True)
        return
    parts = (query.data or "").split(":")
    if len(parts) != 5:
        await query.answer("Некорректные данные.", show_alert=True)
        return
    try:
        tid = int(parts[1])
        device_kind = parts[2]
        slot_index = int(parts[3])
        days = int(parts[4])
    except ValueError:
        await query.answer("Некорректные данные.", show_alert=True)
        return

    pending = await db.get_renewal_request(tid, device_kind, slot_index)
    if pending is None:
        await query.answer("Заявка уже обработана или отозвана.", show_alert=True)
        if query.message:
            with suppress(Exception):
                await query.message.edit_reply_markup(reply_markup=None)
        return

    ok, new_expiry_ms, err = await _extend_subscription_for_user(
        tid, device_kind, slot_index, days
    )
    if not ok or new_expiry_ms is None:
        await query.answer((err or "Ошибка")[:180], show_alert=True)
        return

    await db.delete_renewal_request(tid, device_kind, slot_index)
    await query.answer(f"Продлено на {days} дн.")

    label = _device_subscription_label_from_parts(device_kind, slot_index)
    try:
        await bot.send_message(
            tid,
            (
                f"✅ Подписка продлена\n"
                f"{label}\n"
                f"Новый срок: {_format_expiry_time_ms(new_expiry_ms)}\n\n"
                "Ссылка на подписку остаётся прежней — ничего перенастраивать не нужно."
            ),
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="📱 Мои подписки", callback_data="menu_my_subs")]
                ]
            )
        )
    except Exception:
        logger.exception("Не удалось уведомить пользователя %s о продлении", tid)

    if query.message:
        status_text = f"\n\n✅ <b>Продлено на {days} дн. до {_format_expiry_time_ms(new_expiry_ms)}</b>"
        with suppress(Exception):
            await query.message.edit_text(
                text=(query.message.html_text or "") + status_text,
                reply_markup=None,
                parse_mode="HTML"
            )


@router.callback_query(F.data.startswith("rnw_rej:"))
async def cb_renewal_reject(query: CallbackQuery, bot: Bot) -> None:
    if not _is_admin(query.from_user.id if query.from_user else None):
        await query.answer("Нет прав.", show_alert=True)
        return
    parts = (query.data or "").split(":")
    if len(parts) != 4:
        await query.answer("Некорректные данные.", show_alert=True)
        return
    try:
        tid = int(parts[1])
        device_kind = parts[2]
        slot_index = int(parts[3])
    except ValueError:
        await query.answer("Некорректные данные.", show_alert=True)
        return

    pending = await db.get_renewal_request(tid, device_kind, slot_index)
    if pending is None:
        await query.answer("Заявка уже не активна.", show_alert=True)
        if query.message:
            with suppress(Exception):
                await query.message.edit_reply_markup(reply_markup=None)
        return

    await db.delete_renewal_request(tid, device_kind, slot_index)
    await query.answer("Отклонено.")

    label = _device_subscription_label_from_parts(device_kind, slot_index)
    try:
        await bot.send_message(
            tid,
            f"Запрос на продление ({label}) отклонён. "
            "Если считаете это ошибкой — напишите администратору.",
        )
    except Exception:
        logger.exception("Не удалось уведомить пользователя %s об отказе в продлении", tid)

    if query.message:
        status_text = f"\n\n❌ <b>Запрос на продление отклонён</b>"
        with suppress(Exception):
            await query.message.edit_text(
                text=(query.message.html_text or "") + status_text,
                reply_markup=None,
                parse_mode="HTML"
            )


@router.callback_query(F.data.startswith("dev:"))
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

    if not _panels_configured():
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
            with suppress(Exception):
                await query.message.edit_text(
                    "Заявка отправлена. Когда её одобрят, ссылка на подписку придёт в этот чат.",
                    reply_markup=InlineKeyboardMarkup(
                        inline_keyboard=[
                            [InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="menu_main")]
                        ]
                    )
                )
        await query.answer()
        return

    await _refresh_sub_config()
    ok, sub, expiry_time_ms, err = await _create_subscription_for_user(
        tid, base_email, kind, slot_index
    )
    if not ok or sub is None or expiry_time_ms is None:
        await query.answer((err or "Ошибка")[:200], show_alert=True)
        return
    links = _all_links(sub)
    if query.message:
        label = _device_subscription_label_from_parts(kind, slot_index)
        text = _subscription_message_text(
            label,
            expiry_time_ms,
            links,
        )
        with suppress(Exception):
            await query.message.edit_text(
                text,
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[
                        [InlineKeyboardButton(text="📱 Мои подписки", callback_data="menu_my_subs")],
                        [InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="menu_main")]
                    ]
                )
            )
    await query.answer("Готово")


@router.callback_query(F.data.startswith("apr:"))
async def cb_approve_access(query: CallbackQuery, bot: Bot) -> None:
    if not _is_admin(query.from_user.id if query.from_user else None):
        await query.answer("Нет прав.", show_alert=True)
        return
    parts = (query.data or "").split(":")
    try:
        tid = int(parts[1])
    except (IndexError, ValueError):
        await query.answer("Неверные данные.", show_alert=True)
        return
    days: int | None = None
    if len(parts) >= 3 and parts[2]:
        try:
            days = int(parts[2])
        except ValueError:
            days = None

    pending = await db.get_access_request(tid)
    if pending is None:
        await query.answer("Заявка уже обработана или отозвана.", show_alert=True)
        return

    await _refresh_sub_config()
    ok, sub, expiry_time_ms, err = await _create_subscription_for_user(
        tid,
        pending.base_email,
        pending.device_kind,
        pending.slot_index,
        days=days,
    )
    if not ok or sub is None or expiry_time_ms is None:
        msg = (err or "Ошибка")[:180]
        await query.answer(msg, show_alert=True)
        return

    await db.delete_access_request(tid)
    await query.answer("Доступ выдан.")

    links = _all_links(sub)
    user_text = _subscription_message_text(
        _device_subscription_label_from_parts(
            pending.device_kind,
            pending.slot_index,
        ),
        expiry_time_ms,
        links,
    )
    delivered = False
    try:
        await bot.send_message(
            tid,
            user_text,
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="📱 Мои подписки", callback_data="menu_my_subs")]
                ]
            )
        )
        delivered = True
    except Exception:
        logger.exception("Не удалось написать пользователю %s", tid)

    if query.message:
        status_text = (
            f"\n\n✅ <b>Выдано пользователю <code>{tid}</code></b>"
            if delivered
            else f"\n\n⚠️ <b>Создано для <code>{tid}</code>, но не доставлено в ЛС (нужен чат с ботом)</b>\n"
                 + "\n".join(f"{n}: {l}" for n, l in links)
        )
        with suppress(Exception):
            await query.message.edit_text(
                text=(query.message.html_text or "") + status_text,
                reply_markup=None,
                parse_mode="HTML"
            )


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
        status_text = f"\n\n❌ <b>Заявка пользователя <code>{tid}</code> отклонена</b>"
        with suppress(Exception):
            await query.message.edit_text(
                text=(query.message.html_text or "") + status_text,
                reply_markup=None,
                parse_mode="HTML"
            )


@router.message(Command("stats"))
async def cmd_stats(message: Message) -> None:
    if not _is_admin(message.from_user.id if message.from_user else None):
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
    if SUBSCRIPTION_PORTAL_BASE:
        logger.info(
            "Ссылки на подписку: HTML-портал %s?name=<sub_token>",
            SUBSCRIPTION_PORTAL_BASE,
        )
    elif master:
        logger.info(
            "Ссылки на подписку: мультиподписка с мастер-панели #%s (subId из 3x-ui)",
            master.index,
        )

    await db.init_db()
    await _refresh_sub_config()
    if _require_approval() and not _admin_ids():
        logger.warning(
            "REQUIRE_APPROVAL=1, но не заданы ADMINS/ADMIN_ID — заявки некому подтверждать"
        )
    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher()
    dp.include_router(router)
    reminder_task = asyncio.create_task(_subscription_reminder_worker(bot))
    logger.info("Бот запущен (panel_api build=%s)", PANEL_API_BUILD)
    try:
        await dp.start_polling(bot)
    finally:
        reminder_task.cancel()
        with suppress(asyncio.CancelledError):
            await reminder_task


if __name__ == "__main__":
    asyncio.run(main())
