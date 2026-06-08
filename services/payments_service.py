"""Async-обёртки вокруг синхронного SDK ЮKassa.

Sync-функции `payments.create_payment`, `get_payment_status`, `cancel_payment`
выполняют блокирующие HTTP-запросы. Если вызывать их напрямую из async-кода
aiogram — event loop «замерзает» на время ответа ЮKassa (200-2000 мс), что
приводит к таймаутам long-polling и потере обновлений.

Эти обёртки выносят блокирующий вызов в `asyncio.to_thread`, оставляя
event loop свободным.
"""
from __future__ import annotations

import asyncio
from typing import Any

import payments as _sync


async def create_payment_async(**kwargs: Any) -> dict[str, Any]:
    return await asyncio.to_thread(_sync.create_payment, **kwargs)


async def get_payment_status_async(payment_id: str) -> dict[str, Any]:
    return await asyncio.to_thread(_sync.get_payment_status, payment_id)


async def cancel_payment_async(payment_id: str) -> bool:
    return await asyncio.to_thread(_sync.cancel_payment, payment_id)
