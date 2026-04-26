"""Сборка всех роутеров в один."""

from __future__ import annotations

from aiogram import Router

from . import access, admin, agreement, cabinet, renewal, start


def build_router() -> Router:
    root = Router()
    # Порядок: специфичные хендлеры (callback'и + явные F.text) идут раньше,
    # чтобы их не перехватили общие.
    root.include_router(start.router)
    root.include_router(cabinet.router)
    root.include_router(agreement.router)
    root.include_router(access.router)
    root.include_router(renewal.router)
    root.include_router(admin.router)
    return root
