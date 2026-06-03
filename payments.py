"""Обёртка над ЮKassa: создание платежа, проверка статуса, refund.

Зависимости: yookassa (pip install yookassa)
"""
from __future__ import annotations

import logging
import os
import uuid
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

logger = logging.getLogger(__name__)

# Состояния платежа из ЮKassa
PENDING = "pending"
WAITING_FOR_CAPTURE = "waiting_for_capture"
SUCCEEDED = "succeeded"
CANCELED = "canceled"

TERMINAL_STATUSES = {SUCCEEDED, CANCELED}

# Срок жизни счёта в ЮKassa (по договору максимум 7 дней, делаем 30 минут для UX)
PAYMENT_EXPIRES_SECONDS = 30 * 60


@dataclass(slots=True)
class PlanPrice:
    days: int
    amount_rub: int  # в рублях, без копеек


def _shop_creds() -> tuple[str, str]:
    shop_id = os.getenv("YOOKASSA_SHOP_ID", "").strip()
    secret = os.getenv("YOOKASSA_SECRET_KEY", "").strip()
    return shop_id, secret


def is_configured() -> bool:
    """True, если заданы оба ключа ЮKassa."""
    shop_id, secret = _shop_creds()
    return bool(shop_id) and bool(secret)


def get_return_url() -> str:
    """Куда ЮKassa вернёт пользователя после оплаты (return_url)."""
    return os.getenv("YOOKASSA_RETURN_URL", "").strip() or "https://t.me/"


# --- Цены тарифов ---------------------------------------------------------
# Берутся из .env: PLAN_PRICE_7=350, PLAN_PRICE_30=800, ...
# Если в .env не задано — берётся дефолт из DEFAULT_PLAN_PRICES ниже.

DEFAULT_PLAN_PRICES: dict[int, int] = {
    7: 30,
    30: 80,
    90: 240,
    180: 480,
    365: 960,
}


def load_plan_prices() -> dict[int, int]:
    """Читает цены тарифов из ENV. Не задано — дефолт."""
    out: dict[int, int] = {}
    for days in DEFAULT_PLAN_PRICES:
        raw = os.getenv(f"PLAN_PRICE_{days}")
        if raw and raw.strip().isdigit():
            out[days] = int(raw.strip())
        else:
            out[days] = DEFAULT_PLAN_PRICES[days]
    return out


def plan_amount(days: int) -> int:
    prices = load_plan_prices()
    if days not in prices:
        raise ValueError(f"Тариф {days} дн. не настроен")
    return prices[days]


# --- SDK lazy import -------------------------------------------------------

def _sdk():
    """Импорт SDK внутри функции, чтобы модуль грузился только при необходимости."""
    try:
        from yookassa import Configuration  # type: ignore
    except ImportError as e:
        raise RuntimeError(
            "Не установлен пакет yookassa. Добавьте 'yookassa' в requirements.txt"
        ) from e
    shop_id, secret = _shop_creds()
    if not shop_id or not secret:
        raise RuntimeError("Не заданы YOOKASSA_SHOP_ID / YOOKASSA_SECRET_KEY")
    Configuration.account_id = shop_id
    Configuration.secret_key = secret
    return Configuration


# --- Создание платежа ------------------------------------------------------


def _receipt_item(days: int, amount: int, *, email: str | None) -> dict[str, Any]:
    item: dict[str, Any] = {
        "description": f"Предоставление защищённого канала связи (VPN) на {days} дней",
        "quantity": "1.00",
        "amount": {"value": f"{amount}.00", "currency": "RUB"},
        "vat_code": 1,            # Без НДС
        "payment_mode": "FULL_PAYMENT",
        "payment_subject": "SERVICE",
    }
    if email:
        item["receipt_item_id"] = None  # оставим без id, допустимо
    return item


def _send_receipt() -> bool:
    """Отправлять ли чек 54-ФЗ в API ЮKassa.

    - "1" — ИП/ООО с онлайн-кассой: чек обязателен, нужно передавать `customer`.
      В этом случае email юзера должен быть в `create_payment(email=...)`.
    - "0" (по умолчанию) — самозанятый: чеки выдаются через «Мой налог»,
      в API `receipt` не передаём.
    """
    return os.getenv("YOOKASSA_SEND_RECEIPT", "0").strip() == "1"


def create_payment(
    *,
    days: int,
    amount_rub: int,
    telegram_id: int,
    device_kind: str,
    slot_index: int,
    email: str | None = None,
    idempotence_key: str | None = None,
) -> dict[str, Any]:
    """Создаёт платёж в ЮKassa.

    Возвращает dict с полями:
        id, status, confirmation_url, amount, expires_at, raw
    Бросает RuntimeError при ошибке SDK.
    """
    from yookassa import Payment  # type: ignore

    _sdk()
    if not idempotence_key:
        idempotence_key = f"tg-{telegram_id}-{device_kind}-{slot_index}-{days}-{uuid.uuid4().hex[:8]}"

    description = f"Подписка Vibecode VPN на {days} дней"
    return_url = get_return_url()

    payload: dict[str, Any] = {
        "amount": {"value": f"{amount_rub}.00", "currency": "RUB"},
        "confirmation": {
            "type": "redirect",
            "return_url": return_url,
        },
        "capture": True,
        "description": description,
        "metadata": {
            "telegram_id": str(telegram_id),
            "device_kind": device_kind,
            "slot_index": str(slot_index),
            "days": str(days),
        },
    }

    if _send_receipt():
        if not email:
            raise RuntimeError(
                "YOOKASSA_SEND_RECEIPT=1 требует email юзера "
                "(передайте email= в create_payment)"
            )
        payload["receipt"] = {
            "customer": {"email": email},
            "items": [_receipt_item(days, amount_rub, email=email)],
        }

    try:
        payment = Payment.create(payload, idempotence_key)
    except Exception as e:
        logger.exception("ЮKassa: ошибка создания платежа: %s", e)
        raise RuntimeError(f"ЮKassa: не удалось создать платёж: {e}") from e

    data = {
        "id": payment.id,
        "status": payment.status,
        "amount": amount_rub,
        "confirmation_url": payment.confirmation.confirmation_url
        if payment.confirmation
        else None,
        "expires_at": getattr(payment, "expires_at", None),
        "raw": payment,
    }
    return data


def get_payment_status(payment_id: str) -> dict[str, Any]:
    """Возвращает актуальный статус платежа из ЮKassa."""
    from yookassa import Payment  # type: ignore

    _sdk()
    try:
        payment = Payment.find_one(payment_id)
    except Exception as e:
        logger.exception("ЮKassa: ошибка запроса статуса %s: %s", payment_id, e)
        raise RuntimeError(f"ЮKassa: не удалось получить статус: {e}") from e
    return {
        "id": payment.id,
        "status": payment.status,
        "paid": payment.paid,
        "amount": Decimal(payment.amount.value) if payment.amount else None,
        "raw": payment,
    }


def cancel_payment(payment_id: str) -> bool:
    """Отменяет pending-платёж. Возвращает True при успехе."""
    from yookassa import Payment  # type: ignore

    _sdk()
    try:
        payment = Payment.cancel(payment_id)
        return payment.status == CANCELED
    except Exception:
        logger.exception("ЮKassa: не удалось отменить платёж %s", payment_id)
        return False


def create_refund(payment_id: str, amount_rub: int | None = None) -> str:
    """Возвращает ID refund'а. Если amount=None — полный возврат."""
    from yookassa import Refund  # type: ignore

    _sdk()
    payload: dict[str, Any] = {
        "payment_id": payment_id,
    }
    if amount_rub is not None:
        payload["amount"] = {"value": f"{amount_rub}.00", "currency": "RUB"}
    refund = Refund.create(payload)
    return refund.id
