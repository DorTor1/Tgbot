"""Клиент 3x-ui: логин и добавление клиента во inbound."""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

logger = logging.getLogger(__name__)

INBOUND_IDS = (1, 2, 3, 4)
def vless_flow_inbound_id() -> int:
    try:
        return int(os.getenv("VLESS_FLOW_INBOUND_ID", "1"))
    except ValueError:
        return 1


def vless_flow_value() -> str:
    v = os.getenv("VLESS_FLOW_VISION", "xtls-rprx-vision").strip()
    return v or "xtls-rprx-vision"


def subscription_days() -> int:
    try:
        v = int(os.getenv("SUBSCRIPTION_DAYS", "30"))
        return max(1, min(v, 3650))
    except ValueError:
        return 30


def subscription_expiry_time_ms() -> int:
    """Момент окончания подписки для 3x-ui (expiryTime в миллисекундах, UTC)."""
    return expiry_time_ms_for_days(subscription_days())


def expiry_time_ms_for_days(days: int) -> int:
    """expiryTime в миллисекундах (UTC) на указанное количество дней от сейчас."""
    days = max(1, min(int(days), 3650))
    end = datetime.now(timezone.utc) + timedelta(days=days)
    return int(end.timestamp() * 1000)


class PanelAPIError(Exception):
    """Ошибка API панели или сети."""


class PanelAPI:
    """Клиент 3x-ui. Новые версии (Vue 3 / CSRF): токен с GET /csrf-token, заголовок X-CSRF-Token для POST."""

    # Совпадает с web/session/csrf.go в 3x-ui
    _CSRF_HEADER = "X-CSRF-Token"

    def __init__(
        self,
        base_url: str,
        username: str,
        password: str,
        timeout: float = 45.0,
    ) -> None:
        self._base = base_url.rstrip("/")
        self._username = username
        self._password = password
        self._timeout = timeout
        self._client: httpx.AsyncClient | None = None
        self._csrf_token: str | None = None

    def _abs_url(self, path: str) -> str:
        """Полный URL относительно корня панели (учитывает webBasePath в PANEL_BASE_URL)."""
        p = path.lstrip("/")
        return f"{self._base}/{p}"

    def _panel_api_urls(self, path: str) -> tuple[str, str]:
        """Варианты URL для API панели: со слэшем (новые 3x-ui) и без (старые)."""
        p = path.lstrip("/").rstrip("/")
        return f"{self._base}/{p}/", f"{self._base}/{p}"

    def _csrf_headers(self) -> dict[str, str]:
        if not self._csrf_token:
            return {}
        return {self._CSRF_HEADER: self._csrf_token}

    async def _fetch_public_csrf_token(self) -> None:
        """Публичный GET /csrf-token до логина (нужен для POST /login на новых панелях)."""
        client = self._require_client()
        url = self._abs_url("csrf-token")
        try:
            r = await client.get(url)
        except httpx.RequestError as e:
            logger.debug("csrf-token (public): сеть %s", e)
            return
        if r.status_code != 200:
            logger.debug("csrf-token (public): HTTP %s", r.status_code)
            return
        try:
            body = r.json()
        except json.JSONDecodeError:
            return
        if not body.get("success"):
            return
        obj = body.get("obj")
        if isinstance(obj, str) and obj.strip():
            self._csrf_token = obj.strip()
            logger.info("Получен CSRF-токен (публичный endpoint)")

    async def _fetch_panel_csrf_token(self) -> None:
        """После логина: GET /panel/csrf-token (как SPA) — на случай смены токена в сессии."""
        client = self._require_client()
        url = self._abs_url("panel/csrf-token")
        try:
            r = await client.get(url)
        except httpx.RequestError as e:
            logger.debug("panel/csrf-token: сеть %s", e)
            return
        if r.status_code != 200:
            return
        try:
            body = r.json()
        except json.JSONDecodeError:
            return
        if not body.get("success"):
            return
        obj = body.get("obj")
        if isinstance(obj, str) and obj.strip():
            self._csrf_token = obj.strip()
            logger.info("Обновлён CSRF-токен (panel/csrf-token)")

    async def __aenter__(self) -> PanelAPI:
        self._client = httpx.AsyncClient(
            base_url=self._base,
            timeout=self._timeout,
            follow_redirects=True,
        )
        self._csrf_token = None
        return self

    async def __aexit__(self, *args: Any) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    def _require_client(self) -> httpx.AsyncClient:
        if self._client is None:
            raise RuntimeError("PanelAPI используйте через async with")
        return self._client

    async def login(self) -> None:
        client = self._require_client()
        await self._fetch_public_csrf_token()
        login_url = self._abs_url("login")
        try:
            r = await client.post(
                login_url,
                data={"username": self._username, "password": self._password},
                headers=self._csrf_headers(),
            )
        except httpx.RequestError as e:
            logger.exception("Панель недоступна при логине: %s", e)
            raise PanelAPIError("Панель недоступна. Попробуйте позже.") from e

        logger.info("POST /login status=%s", r.status_code)
        if r.status_code == 403:
            logger.warning("Логин 403 (часто CSRF). Ответ: %s", r.text[:500])
            raise PanelAPIError(
                "Доступ к панели отклонён (403). Обновите бота или проверьте версию 3x-ui / CSRF."
            )
        if r.status_code != 200:
            logger.warning("Ответ логина: %s", r.text[:500])
            raise PanelAPIError("Не удалось войти в панель (проверьте логин/пароль).")

        try:
            body = r.json()
        except json.JSONDecodeError:
            logger.warning("Логин: не JSON, тело: %s", r.text[:300])
            raise PanelAPIError("Некорректный ответ панели при входе.")

        if not body.get("success", True):
            msg = body.get("msg", "unknown")
            logger.error("Логин отклонён: %s", msg)
            raise PanelAPIError("Вход в панель отклонён.")

        await self._fetch_panel_csrf_token()

    async def get_sub_config(self) -> dict[str, Any]:
        """Читает настройки подписки из панели.

        Новые 3x-ui (Vue 3): POST /panel/setting/all.
        Старые: POST /panel/api/setting/all.

        Возвращает словарь с полями subURI, subPath, subDomain, subPort,
        subKeyFile, subCertFile, subEnable и т.п. Пустой словарь — если не удалось.
        """
        client = self._require_client()
        slash, plain = self._panel_api_urls("panel/setting/all")
        legacy_slash, legacy_plain = self._panel_api_urls("panel/api/setting/all")
        urls = (slash, plain, legacy_slash, legacy_plain)
        last_status = 0
        r: httpx.Response | None = None
        for url in urls:
            try:
                r = await client.post(url, headers=self._csrf_headers())
            except httpx.RequestError as e:
                logger.exception("setting/all: %s", e)
                raise PanelAPIError("Не удалось получить настройки панели.") from e
            last_status = r.status_code
            if r.status_code == 404:
                continue
            break
        if r is None:
            raise PanelAPIError("setting/all: нет ответа.")
        if r.status_code == 403:
            raise PanelAPIError("setting/all: 403 — сессия или CSRF (обновите бота).")
        if r.status_code != 200:
            raise PanelAPIError(f"setting/all: HTTP {r.status_code}.")
        try:
            body = r.json()
        except json.JSONDecodeError as e:
            raise PanelAPIError("setting/all: не JSON.") from e
        if not body.get("success"):
            raise PanelAPIError("setting/all: отказ панели.")
        obj = body.get("obj")
        if not isinstance(obj, dict):
            return {}
        keys = (
            "subEnable",
            "subURI",
            "subPath",
            "subDomain",
            "subPort",
            "subKeyFile",
            "subCertFile",
        )
        return {k: obj.get(k) for k in keys if k in obj}

    async def _inbound_protocol_map(self) -> dict[int, str]:
        """id inbound → protocol (как в панели: vless, trojan, shadowsocks, …)."""
        client = self._require_client()
        r: httpx.Response | None = None
        try:
            for url in self._panel_api_urls("panel/api/inbounds/list"):
                r = await client.get(url)
                if r.status_code != 404:
                    break
        except httpx.RequestError as e:
            logger.exception("inbounds/list: %s", e)
            raise PanelAPIError("Не удалось получить список inbound.") from e
        if r is None:
            raise PanelAPIError("Список inbound: нет ответа панели.")
        if r.status_code != 200:
            raise PanelAPIError(f"Список inbound: HTTP {r.status_code}.")
        try:
            body = r.json()
        except json.JSONDecodeError as e:
            raise PanelAPIError("Список inbound: не JSON.") from e
        if not body.get("success"):
            raise PanelAPIError("Список inbound: отказ панели.")
        raw = body.get("obj")
        if not isinstance(raw, list):
            return {}
        out: dict[int, str] = {}
        for row in raw:
            if not isinstance(row, dict):
                continue
            try:
                iid = int(row["id"])
            except (KeyError, TypeError, ValueError):
                continue
            proto = row.get("protocol")
            out[iid] = proto if isinstance(proto, str) else ""
        logger.info("Протоколы inbound: %s", out)
        return out

    @staticmethod
    def _client_json_for_protocol(
        protocol: str,
        client_uuid: str,
        email: str,
        sub_id: str,
        expiry_time_ms: int,
        inbound_id: int,
    ) -> dict[str, Any]:
        """
        В 3x-ui для trojan валидируется поле password, для vless/vmess — id.
        См. AddInboundClient в web/service/inbound.go (switch oldInbound.Protocol).
        expiryTime — срок клиента в панели (мс, модель database/model.Client).
        """
        proto = (protocol or "").strip().lower()
        common = {
            "email": email,
            "subId": sub_id,
            "enable": True,
            "expiryTime": expiry_time_ms,
        }
        if proto == "trojan":
            # В панели для trojan проверяется password, а не id (inbound.go AddInboundClient).
            return {"password": client_uuid, **common}
        row: dict[str, Any] = {"id": client_uuid, **common}
        if inbound_id == vless_flow_inbound_id() and proto == "vless":
            row["flow"] = vless_flow_value()
        return row

    async def add_client(
        self,
        inbound_id: int,
        client_uuid: str,
        email: str,
        sub_id: str,
        protocol: str = "",
        expiry_time_ms: int = 0,
    ) -> None:
        client = self._require_client()
        client_row = self._client_json_for_protocol(
            protocol, client_uuid, email, sub_id, expiry_time_ms, inbound_id
        )
        settings_obj = {"clients": [client_row]}
        payload = {
            "id": inbound_id,
            "settings": json.dumps(settings_obj, separators=(",", ":")),
        }
        r: httpx.Response | None = None
        try:
            for url in self._panel_api_urls("panel/api/inbounds/addClient"):
                r = await client.post(
                    url,
                    json=payload,
                    headers=self._csrf_headers(),
                )
                if r.status_code != 404:
                    break
        except httpx.RequestError as e:
            logger.exception("addClient inbound=%s: %s", inbound_id, e)
            raise PanelAPIError("Сеть: не удалось связаться с панелью.") from e
        if r is None:
            raise PanelAPIError("addClient: нет ответа панели.")

        logger.info(
            "addClient inbound=%s status=%s body=%s",
            inbound_id,
            r.status_code,
            (r.text[:400] + "…") if len(r.text) > 400 else r.text,
        )

        if r.status_code != 200:
            raise PanelAPIError(f"Панель вернула HTTP {r.status_code} для inbound {inbound_id}.")

        try:
            body = r.json()
        except json.JSONDecodeError:
            raise PanelAPIError(f"Некорректный JSON ответа addClient (inbound {inbound_id}).")

        if not body.get("success", False):
            msg = body.get("msg", str(body))
            logger.error("addClient failed inbound=%s: %s", inbound_id, msg)
            raise PanelAPIError(f"Панель не создала клиента (inbound {inbound_id}): {msg}")

    async def update_client(
        self,
        inbound_id: int,
        client_uuid: str,
        email: str,
        sub_id: str,
        protocol: str = "",
        expiry_time_ms: int = 0,
    ) -> None:
        """Обновляет существующего клиента во inbound (меняет expiryTime и т.п.).

        Эндпоинт 3x-ui: POST /panel/api/inbounds/updateClient/<clientId>,
        где clientId — UUID для vless/vmess или password для trojan (у нас это одно и то же,
        см. _client_json_for_protocol → add_client).
        """
        client = self._require_client()
        client_row = self._client_json_for_protocol(
            protocol, client_uuid, email, sub_id, expiry_time_ms, inbound_id
        )
        settings_obj = {"clients": [client_row]}
        payload = {
            "id": inbound_id,
            "settings": json.dumps(settings_obj, separators=(",", ":")),
        }
        path = f"panel/api/inbounds/updateClient/{client_uuid}"
        r: httpx.Response | None = None
        try:
            for url in self._panel_api_urls(path):
                r = await client.post(
                    url,
                    json=payload,
                    headers=self._csrf_headers(),
                )
                if r.status_code != 404:
                    break
        except httpx.RequestError as e:
            logger.exception("updateClient inbound=%s: %s", inbound_id, e)
            raise PanelAPIError("Сеть: не удалось связаться с панелью.") from e
        if r is None:
            raise PanelAPIError("updateClient: нет ответа панели.")

        logger.info(
            "updateClient inbound=%s status=%s body=%s",
            inbound_id,
            r.status_code,
            (r.text[:400] + "…") if len(r.text) > 400 else r.text,
        )

        if r.status_code != 200:
            raise PanelAPIError(f"Панель вернула HTTP {r.status_code} для inbound {inbound_id}.")
        try:
            body = r.json()
        except json.JSONDecodeError:
            raise PanelAPIError(f"Некорректный JSON ответа updateClient (inbound {inbound_id}).")
        if not body.get("success", False):
            msg = body.get("msg", str(body))
            logger.error("updateClient failed inbound=%s: %s", inbound_id, msg)
            raise PanelAPIError(f"Панель не обновила клиента (inbound {inbound_id}): {msg}")

    async def update_user_on_all_inbounds(
        self,
        base_email: str,
        client_uuid: str,
        sub_id: str,
        expiry_time_ms: int,
    ) -> int:
        """Продлевает клиента на инбаундах, где он есть на этой панели.

        Пропускает отсутствующие на панели id из INBOUND_IDS и ошибки
        update по отдельному inbound (нет клиента / отказ панели по одному id).

        Возвращает число успешных update_client.
        """
        await self.login()
        proto_map = await self._inbound_protocol_map()
        ok = 0
        for iid in INBOUND_IDS:
            if iid not in proto_map:
                logger.debug("Продление: inbound %s нет на панели, пропуск.", iid)
                continue
            email = f"{base_email}_{iid}"
            try:
                await self.update_client(
                    iid,
                    client_uuid,
                    email,
                    sub_id,
                    proto_map.get(iid, ""),
                    expiry_time_ms,
                )
                ok += 1
            except PanelAPIError as e:
                logger.warning(
                    "Продление: не обновлён inbound %s на панели (%s): %s",
                    iid,
                    self._base,
                    e,
                )
        return ok

    async def register_user_on_all_inbounds(
        self,
        base_email: str,
        client_uuid: str,
        sub_id: str,
        expiry_time_ms: int | None = None,
    ) -> None:
        await self.login()
        expiry_ms = expiry_time_ms if expiry_time_ms is not None else subscription_expiry_time_ms()
        proto_map = await self._inbound_protocol_map()
        for iid in INBOUND_IDS:
            email = f"{base_email}_{iid}"
            await self.add_client(
                iid,
                client_uuid,
                email,
                sub_id,
                proto_map.get(iid, ""),
                expiry_ms,
            )
