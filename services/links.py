"""Сборка ссылок подписки: портал, агрегатор или per-panel sub URL."""

from __future__ import annotations

import logging
from urllib.parse import quote, urlparse

from config import (
    PANELS,
    SUBSCRIPTION_AGGREGATOR_BASE,
    SUBSCRIPTION_PORTAL_BASE,
    PanelConfig,
)
from panel_api import PanelAPI, PanelAPIError

logger = logging.getLogger(__name__)


def _subscription_link_base_from_panel(panel: PanelConfig) -> str:
    """Собирает базу ссылки по subURI/subPath/… из настроек панели."""
    cfg = panel.sub_config_cache
    sub_uri = (cfg.get("subURI") or "").strip() if isinstance(cfg, dict) else ""
    if sub_uri:
        return sub_uri.rstrip("/")

    u = urlparse(panel.base_url)
    sub_path = (cfg.get("subPath") if isinstance(cfg, dict) else None) or "/sub/"
    sub_path = "/" + sub_path.strip("/")
    sub_domain = (cfg.get("subDomain") if isinstance(cfg, dict) else None) or ""
    sub_port = cfg.get("subPort") if isinstance(cfg, dict) else None
    has_tls = bool(
        (cfg.get("subKeyFile") if isinstance(cfg, dict) else None)
        and (cfg.get("subCertFile") if isinstance(cfg, dict) else None)
    )

    host = sub_domain.strip() or (u.hostname or "")
    if sub_domain:
        scheme = "https" if has_tls else "http"
    else:
        scheme = u.scheme or ("https" if has_tls else "http")

    try:
        port_int = int(sub_port) if sub_port is not None else 0
    except (TypeError, ValueError):
        port_int = 0

    netloc = host
    if port_int and port_int not in (80, 443):
        netloc = f"{host}:{port_int}"

    return f"{scheme}://{netloc}{sub_path}".rstrip("/")


def _subscription_link_base(panel: PanelConfig) -> str:
    """Override из .env сильнее, чем автодетект из панели."""
    if panel.sub_base_url and panel.sub_path:
        root = panel.sub_base_url.rstrip("/")
        path = panel.sub_path.strip("/")
        return f"{root}/{path}" if path else root
    if panel.sub_base_url and not panel.sub_path:
        return panel.sub_base_url.rstrip("/")
    if panel.sub_path:
        u = urlparse(panel.base_url)
        root = (
            f"{u.scheme}://{u.netloc}".rstrip("/")
            if u.scheme and u.netloc
            else panel.base_url
        )
        return f"{root}/{panel.sub_path.strip('/')}"
    return _subscription_link_base_from_panel(panel)


def _instruction_link(panel: PanelConfig, sub_token: str) -> str:
    base = _subscription_link_base(panel).rstrip("/")
    enc = quote(sub_token, safe="")
    q = panel.sub_query_param.lower()
    if q in ("bare", "legacy", "none"):
        return f"{base}?{enc}"
    if q in ("path", "slash"):
        return f"{base}/{enc}"
    return f"{base}?{quote(panel.sub_query_param, safe='')}={enc}"


def subscription_portal_link(sub_token: str) -> str:
    enc = quote(sub_token, safe="")
    base = SUBSCRIPTION_PORTAL_BASE
    return f"{base}&name={enc}" if "?" in base else f"{base}?name={enc}"


def all_links(sub_token: str) -> list[tuple[str, str]]:
    """Возвращает (имя_источника, ссылка). Портал > агрегатор > per-panel."""
    if SUBSCRIPTION_PORTAL_BASE:
        return [("Инструкция и подписка", subscription_portal_link(sub_token))]
    if SUBSCRIPTION_AGGREGATOR_BASE:
        enc = quote(sub_token, safe="")
        return [("Все серверы", f"{SUBSCRIPTION_AGGREGATOR_BASE}/{enc}")]
    return [(p.name, _instruction_link(p, sub_token)) for p in PANELS]


async def refresh_sub_config() -> None:
    """Тянет subURI/subPath/… с каждой панели и кладёт в её кэш."""
    for panel in PANELS:
        if not panel.configured:
            continue
        try:
            async with PanelAPI(panel.base_url, panel.login, panel.password) as api:
                await api.login()
                cfg = await api.get_sub_config()
            if cfg:
                panel.sub_config_cache = cfg
                logger.info(
                    "Панель %s (%s): настройки подписки %s",
                    panel.index,
                    panel.name,
                    cfg,
                )
        except PanelAPIError as e:
            logger.warning(
                "Панель %s (%s): не удалось получить настройки подписки: %s",
                panel.index,
                panel.name,
                e,
            )
        except Exception:
            logger.exception(
                "Панель %s (%s): неожиданная ошибка при чтении настроек",
                panel.index,
                panel.name,
            )
