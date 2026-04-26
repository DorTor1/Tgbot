"""Конфигурация бота: чтение .env, описание панелей, константы."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from datetime import timedelta, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

logger = logging.getLogger(__name__)

load_dotenv(Path(__file__).resolve().parent / ".env")

# --- Базовые секреты/идентификаторы --------------------------------------------------

BOT_TOKEN: str = os.getenv("BOT_TOKEN", "").strip()


def _env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def _admin_id() -> int | None:
    raw = _env("ADMIN_ID", "0")
    try:
        v = int(raw)
        return v if v else None
    except ValueError:
        return None


def admin_ids() -> set[int]:
    """Все Telegram-id админов (ADMINS приоритетнее ADMIN_ID)."""
    ids: set[int] = set()
    raw = _env("ADMINS")
    if raw:
        for part in raw.split(","):
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


def is_admin(user_id: int | None) -> bool:
    if user_id is None:
        return False
    return user_id in admin_ids()


def support_username() -> str:
    """@username админа для кнопки «Написать админу» (без @)."""
    return _env("SUPPORT_USERNAME").lstrip("@")


def primary_admin_id() -> int | None:
    """Первый админ из набора — для deep-link tg://user?id=… когда нет @username."""
    ids = admin_ids()
    return min(ids) if ids else None


def require_approval() -> bool:
    v = _env("REQUIRE_APPROVAL", "1").lower()
    return v not in ("0", "false", "no", "off", "")


# --- Подписка / агрегатор / портал ---------------------------------------------------

SUBSCRIPTION_AGGREGATOR_BASE: str = _env("SUBSCRIPTION_AGGREGATOR_BASE").rstrip("/")
SUBSCRIPTION_PORTAL_BASE: str = _env("SUBSCRIPTION_PORTAL_BASE").rstrip("/")


# --- Тариф (для отображения пользователю) -------------------------------------------

DEFAULT_TARIFF_TEXT: str = _env("TARIFF_TEXT", "80 ₽ за 30 дней")


# --- Панели 3x-ui ---------------------------------------------------------------------


@dataclass
class PanelConfig:
    """Конфиг одной 3x-ui панели (одного VPS)."""

    index: int
    name: str
    base_url: str
    login: str
    password: str
    sub_base_url: str = ""
    sub_path: str = ""
    sub_query_param: str = "name"
    sub_config_cache: dict[str, Any] = field(default_factory=dict)

    @property
    def configured(self) -> bool:
        return bool(self.base_url and self.login and self.password)


def _load_panels() -> list[PanelConfig]:
    """Считывает панели из .env. Поддерживает суффиксы _1.._20 + legacy без суффикса."""
    panels: list[PanelConfig] = []
    for i in range(1, 21):
        base_url = _env(f"PANEL_BASE_URL_{i}").rstrip("/")
        if not base_url:
            continue
        panels.append(
            PanelConfig(
                index=i,
                name=_env(f"PANEL_NAME_{i}") or f"Сервер {i}",
                base_url=base_url,
                login=_env(f"PANEL_LOGIN_{i}"),
                password=_env(f"PANEL_PASSWORD_{i}"),
                sub_base_url=_env(f"SUBSCRIPTION_BASE_URL_{i}").rstrip("/"),
                sub_path=_env(f"SUBSCRIPTION_PATH_{i}"),
                sub_query_param=_env(f"SUBSCRIPTION_QUERY_PARAM_{i}") or "name",
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
            sub_base_url=_env("SUBSCRIPTION_BASE_URL").rstrip("/"),
            sub_path=_env("SUBSCRIPTION_PATH"),
            sub_query_param=_env("SUBSCRIPTION_QUERY_PARAM") or "name",
        )
    ]


PANELS: list[PanelConfig] = _load_panels()


def panels_configured() -> bool:
    return any(p.configured for p in PANELS)


# --- Устройства -----------------------------------------------------------------------

# Тип устройства → префикс в email панели (до _nick и номера слота).
EMAIL_PREFIX: dict[str, str] = {
    "phone": "phone",
    "laptop": "laptop",
    "pc": "pc",
    "other": "other",
}
DEVICE_LABEL_RU: dict[str, str] = {
    "phone": "Смартфон",
    "laptop": "Ноутбук",
    "pc": "ПК",
    "other": "Другое устройство",
}
DEVICE_EMOJI: dict[str, str] = {
    "phone": "📱",
    "laptop": "💻",
    "pc": "🖥",
    "other": "📟",
}


# --- Сроки и напоминания --------------------------------------------------------------

# Варианты срока подписки (в днях), которые админ выбирает кнопкой.
APPROVAL_DURATION_CHOICES: tuple[int, ...] = (7, 30, 90, 180, 365)

REMINDER_3D_MS: int = 3 * 24 * 60 * 60 * 1000
REMINDER_1D_MS: int = 1 * 24 * 60 * 60 * 1000
REMINDER_CHECK_INTERVAL_SECONDS: int = 3600

MSK_TZ = timezone(timedelta(hours=3), "МСК")
