"""Высокоуровневые операции над подписками: создание и продление.

Атомарность мульти-панельной регистрации:
- Регистрируем клиента на каждой сконфигурированной панели последовательно.
- Если хоть одна панель упала — пытаемся откатить регистрацию на всех уже успешных
  панелях (`delete_client_from_all_inbounds`), чтобы не оставлять «полу-доступ».
- Только после полного успеха пишем в БД (`user_devices`).
"""

from __future__ import annotations

import logging
import secrets
import string
import uuid as uuid_mod

import db
from config import EMAIL_PREFIX, PANELS, panels_configured
from panel_api import (
    PanelAPI,
    PanelAPIError,
    expiry_time_ms_for_days,
    subscription_expiry_time_ms,
)

logger = logging.getLogger(__name__)


def panel_base_email(nick: str, device_kind: str, slot_index: int) -> str:
    """Префикс для панели: phone_nick, второй смартфон — phone_nick2 (далее _1.._4 — inbound)."""
    p = EMAIL_PREFIX.get(device_kind, "other")
    nick = nick.strip()[:40] or "user"
    if slot_index <= 1:
        return f"{p}_{nick}"
    return f"{p}_{nick}{slot_index}"


def _new_sub_token() -> str:
    alphabet = string.ascii_letters + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(10))


async def _rollback_on_panels(
    succeeded_panels: list, client_uuid: str
) -> None:
    """Удаляет клиента со всех уже зарегистрированных панелей (best-effort)."""
    for panel in succeeded_panels:
        try:
            async with PanelAPI(panel.base_url, panel.login, panel.password) as api:
                ok = await api.delete_client_from_all_inbounds(client_uuid)
                logger.info(
                    "Откат регистрации: панель %s (%s) — удалено инбаундов: %d",
                    panel.index,
                    panel.name,
                    ok,
                )
        except Exception:
            logger.exception(
                "Откат регистрации: панель %s (%s) — не удалось",
                panel.index,
                panel.name,
            )


async def create_subscription_for_user(
    tid: int,
    base_email: str,
    device_kind: str,
    slot_index: int,
    days: int | None = None,
) -> tuple[bool, str | None, int | None, str]:
    """Регистрирует клиента на всех панелях и пишет в БД.

    Возвращает (успех, sub_token, expiry_time_ms, текст ошибки для пользователя).
    При частичной регистрации — откатывает уже успешные панели.
    """
    if not panels_configured():
        return False, None, None, "Сейчас выдать ссылку нельзя. Напишите администратору."

    client_uuid = str(uuid_mod.uuid4())
    sub = _new_sub_token()
    expiry_time_ms = (
        expiry_time_ms_for_days(days) if days is not None else subscription_expiry_time_ms()
    )

    succeeded: list = []
    for panel in PANELS:
        if not panel.configured:
            continue
        try:
            async with PanelAPI(panel.base_url, panel.login, panel.password) as api:
                await api.register_user_on_all_inbounds(
                    base_email,
                    client_uuid,
                    sub,
                    expiry_time_ms,
                )
            succeeded.append(panel)
        except PanelAPIError as e:
            logger.warning(
                "Регистрация на панели %s (%s) упала для tg_id=%s: %s",
                panel.index,
                panel.name,
                tid,
                e,
            )
            # Откатываем как уже успешные панели, так и текущую: на ней могли
            # успеть зарегистрировать клиента в части inbound до сбоя.
            await _rollback_on_panels(succeeded + [panel], client_uuid)
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
            await _rollback_on_panels(succeeded + [panel], client_uuid)
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
    logger.info(
        "Создана подписка tg_id=%s device=%s/%s expiry_ms=%s",
        tid,
        device_kind,
        slot_index,
        expiry_time_ms,
    )
    return True, sub, expiry_time_ms, ""


async def extend_subscription_for_user(
    tid: int,
    device_kind: str,
    slot_index: int,
    days: int,
) -> tuple[bool, int | None, str]:
    """Продлевает подписку на всех доступных панелях/инбаундах. Best-effort.

    Если хотя бы один инбаунд на любой панели обновился — операция считается успешной,
    и в БД сохраняется новый срок (это поведение совпадает со старой версией, но при
    PanelAPIError на одной панели мы больше не откатываем продление целиком, чтобы
    транзиентные сетевые ошибки не блокировали продление). Срок в БД — источник истины
    для напоминаний; при следующей попытке продления панель доберётся.
    """
    device = await db.get_user_device(tid, device_kind, slot_index)
    if device is None:
        return False, None, "Подписка пользователя не найдена в БД."
    if not panels_configured():
        return False, None, "Нет сконфигурированных панелей."

    new_expiry_ms = expiry_time_ms_for_days(days)
    updated_inbounds = 0
    panel_errors: list[str] = []
    for panel in PANELS:
        if not panel.configured:
            continue
        try:
            async with PanelAPI(panel.base_url, panel.login, panel.password) as api:
                n = await api.update_user_on_all_inbounds(
                    device.base_email,
                    device.uuid,
                    device.sub_token,
                    new_expiry_ms,
                )
                updated_inbounds += n
                if n == 0:
                    logger.info(
                        "Продление: на панели %s (%s) для tg_id=%s ни один inbound не обновлён.",
                        panel.index,
                        panel.name,
                        tid,
                    )
        except PanelAPIError as e:
            logger.warning(
                "Продление: ошибка панели %s (%s) для tg_id=%s: %s",
                panel.index,
                panel.name,
                tid,
                e,
            )
            panel_errors.append(f"{panel.name}: {e}")
        except Exception:
            logger.exception(
                "Продление: неожиданная ошибка на панели %s для tg_id=%s",
                panel.index,
                tid,
            )
            panel_errors.append(f"{panel.name}: внутренняя ошибка")

    if updated_inbounds == 0:
        if panel_errors:
            return False, None, "Ошибка панелей: " + "; ".join(panel_errors)
        return (
            False,
            None,
            "Не удалось продлить: на доступных панелях нет клиента ни в одном из inbound.",
        )

    await db.extend_device_expiry(tid, device_kind, slot_index, new_expiry_ms)
    return True, new_expiry_ms, ""
