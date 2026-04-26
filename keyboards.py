"""Все клавиатуры (inline + reply) в одном месте."""

from __future__ import annotations

from urllib.parse import quote

from aiogram.types import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardMarkup,
)

from config import (
    APPROVAL_DURATION_CHOICES,
    SUBSCRIPTION_AGGREGATOR_BASE,
    SUBSCRIPTION_PORTAL_BASE,
    primary_admin_id,
    support_username,
)


# --- Главное меню ---------------------------------------------------------------------


def main_reply_keyboard(is_admin_user: bool = False) -> ReplyKeyboardMarkup:
    rows: list[list[KeyboardButton]] = [
        [KeyboardButton(text="Личный кабинет"), KeyboardButton(text="Помощь")],
    ]
    if is_admin_user:
        rows.append([KeyboardButton(text="Админ-панель")])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)


# --- Личный кабинет -------------------------------------------------------------------


def cabinet_keyboard(has_devices: bool, has_pending_request: bool) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    if has_devices:
        rows.append(
            [
                InlineKeyboardButton(
                    text="🔗 Открыть подписки", callback_data="cab:links"
                )
            ]
        )
    if not has_pending_request:
        rows.append(
            [
                InlineKeyboardButton(
                    text=("➕ Добавить устройство" if has_devices else "➕ Получить доступ"),
                    callback_data="cab:get_access",
                )
            ]
        )
    rows.append(
        [
            InlineKeyboardButton(
                text="💬 Связаться с админом", callback_data="cab:support"
            ),
            InlineKeyboardButton(text="❓ FAQ", callback_data="cab:faq"),
        ]
    )
    rows.append(
        [
            InlineKeyboardButton(
                text="📄 Правила сервиса", callback_data="agr:show"
            )
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def back_to_cabinet_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="⬅️ В личный кабинет", callback_data="cab:home"
                )
            ]
        ]
    )


# --- Поддержка ------------------------------------------------------------------------


def support_keyboard() -> InlineKeyboardMarkup:
    """Ссылка на админа: предпочитаем @username (https://t.me/...), иначе tg://user?id="""
    username = support_username()
    rows: list[list[InlineKeyboardButton]] = []
    if username:
        rows.append(
            [
                InlineKeyboardButton(
                    text="✉️ Написать админу", url=f"https://t.me/{quote(username)}"
                )
            ]
        )
    else:
        admin_id = primary_admin_id()
        if admin_id is not None:
            rows.append(
                [
                    InlineKeyboardButton(
                        text="✉️ Написать админу",
                        url=f"tg://user?id={admin_id}",
                    )
                ]
            )
    rows.append(
        [
            InlineKeyboardButton(
                text="⬅️ Назад", callback_data="cab:home"
            )
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


# --- Выбор устройства -----------------------------------------------------------------


def device_selection_keyboard() -> InlineKeyboardMarkup:
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
                InlineKeyboardButton(
                    text="⬅️ Отмена", callback_data="cab:home"
                )
            ],
        ]
    )


# --- Юридическое согласие -------------------------------------------------------------


def agreement_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="📄 Открыть правила", callback_data="agr:show"
                )
            ],
            [
                InlineKeyboardButton(
                    text="✅ Принимаю", callback_data="agr:yes"
                ),
                InlineKeyboardButton(
                    text="❌ Не согласен", callback_data="agr:no"
                ),
            ],
        ]
    )


# --- Подписка пользователя: «Открыть инструкцию» / «Продлить» ------------------------


def link_keyboard(url: str, device_kind: str, slot_index: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🌐 Открыть инструкцию", url=url)],
            [
                InlineKeyboardButton(
                    text="🔁 Продлить",
                    callback_data=f"rnw_req:{device_kind}:{slot_index}",
                )
            ],
        ]
    )


def renew_only_keyboard(device_kind: str, slot_index: int) -> InlineKeyboardMarkup:
    """Кнопка «🔁 Продлить» под напоминаниями."""
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


# --- Админ ---------------------------------------------------------------------------


def _duration_buttons(prefix_callback: str) -> list[list[InlineKeyboardButton]]:
    rows: list[list[InlineKeyboardButton]] = []
    row: list[InlineKeyboardButton] = []
    for days in APPROVAL_DURATION_CHOICES:
        row.append(
            InlineKeyboardButton(
                text=f"✅ {days} дн.",
                callback_data=f"{prefix_callback}:{days}",
            )
        )
        if len(row) == 3:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    return rows


def access_review_keyboard(target_telegram_id: int) -> InlineKeyboardMarkup:
    rows = _duration_buttons(f"apr:{target_telegram_id}")
    rows.append(
        [
            InlineKeyboardButton(
                text="❌ Отклонить",
                callback_data=f"rej:{target_telegram_id}",
            ),
            InlineKeyboardButton(
                text="✉️ Написать", url=f"tg://user?id={target_telegram_id}"
            ),
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def renewal_review_keyboard(
    tid: int, device_kind: str, slot_index: int
) -> InlineKeyboardMarkup:
    rows = _duration_buttons(f"rnw_apr:{tid}:{device_kind}:{slot_index}")
    rows.append(
        [
            InlineKeyboardButton(
                text="❌ Отклонить",
                callback_data=f"rnw_rej:{tid}:{device_kind}:{slot_index}",
            ),
            InlineKeyboardButton(text="✉️ Написать", url=f"tg://user?id={tid}"),
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def admin_dashboard_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="📋 Очередь заявок", callback_data="adm:pending"
                )
            ],
            [
                InlineKeyboardButton(
                    text="👥 Пользователи", callback_data="adm:users"
                ),
                InlineKeyboardButton(
                    text="📊 Статистика", callback_data="adm:stats"
                ),
            ],
        ]
    )


# --- Вспомогательное ------------------------------------------------------------------


def has_link_targets() -> bool:
    """True — настроен хоть один источник ссылок (портал, агрегатор или панели)."""
    return bool(SUBSCRIPTION_PORTAL_BASE or SUBSCRIPTION_AGGREGATOR_BASE)
