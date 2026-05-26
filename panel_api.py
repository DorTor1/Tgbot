"""Клиент 3x-ui 3.x: Bearer или сессия, /panel/api/clients/*, настройки подписки."""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import quote

import httpx

logger = logging.getLogger(__name__)

# Маркер сборки — ищите в journalctl после рестарта; если строки нет, крутится старый код.
PANEL_API_BUILD = "v3-bearer-renew-2026-05-26"

_DEFAULT_INBOUND_IDS = (1, 2, 3, 4)


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


def panel_api_mode() -> str:
    """Режим API: v3 (3x-ui 3.x, по умолчанию), legacy (2.x), auto (определить)."""
    v = os.getenv("PANEL_API_MODE", "v3").strip().lower()
    if v in ("v3", "legacy", "auto"):
        return v
    return "v3"


def inbound_ids_config() -> tuple[int, ...]:
    """Inbound id для привязки клиента (пересекаются с тем, что есть на панели)."""
    raw = os.getenv("PANEL_INBOUND_IDS", "").strip()
    if not raw:
        return _DEFAULT_INBOUND_IDS
    out: list[int] = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            out.append(int(part))
        except ValueError:
            continue
    return tuple(out) if out else _DEFAULT_INBOUND_IDS


def panel_client_email(base_email: str, inbound_ids: list[int] | None = None) -> str:
    """Канонический email клиента в 3x-ui v3 (один на все inbound)."""
    ids = inbound_ids or list(inbound_ids_config())
    if not ids:
        return base_email
    return f"{base_email}_{min(ids)}"


class PanelAPIError(Exception):
    """Ошибка API панели или сети."""


class PanelAPI:
    """Клиент 3x-ui 3.x: Bearer API token (предпочтительно) или login+CSRF."""

    _CSRF_HEADER = "X-CSRF-Token"

    def __init__(
        self,
        base_url: str,
        username: str = "",
        password: str = "",
        *,
        api_token: str = "",
        timeout: float = 45.0,
    ) -> None:
        self._base = base_url.rstrip("/")
        self._username = username
        self._password = password
        self._api_token = api_token.strip()
        self._timeout = timeout
        self._client: httpx.AsyncClient | None = None
        self._csrf_token: str | None = None
        self._clients_api_v3: bool | None = None
        self._logged_in = False

    def _abs_url(self, path: str) -> str:
        p = path.lstrip("/")
        return f"{self._base}/{p}"

    def _panel_api_urls(self, path: str) -> tuple[str, str]:
        p = path.lstrip("/").rstrip("/")
        return f"{self._base}/{p}/", f"{self._base}/{p}"

    def _uses_bearer(self) -> bool:
        return bool(self._api_token)

    def _api_headers(self) -> dict[str, str]:
        h: dict[str, str] = {
            "X-Requested-With": "XMLHttpRequest",
            "Accept": "application/json",
        }
        if self._uses_bearer():
            h["Authorization"] = f"Bearer {self._api_token}"
        else:
            h.update(self._csrf_headers())
        return h

    def _csrf_headers(self) -> dict[str, str]:
        if not self._csrf_token:
            return {}
        return {self._CSRF_HEADER: self._csrf_token}

    async def _fetch_public_csrf_token(self) -> None:
        client = self._require_client()
        url = self._abs_url("csrf-token")
        try:
            r = await client.get(url, headers={"X-Requested-With": "XMLHttpRequest"})
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
        client = self._require_client()
        url = self._abs_url("panel/csrf-token")
        try:
            r = await client.get(url, headers=self._api_headers())
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
        self._clients_api_v3 = None
        self._logged_in = False
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
                headers=self._api_headers(),
            )
        except httpx.RequestError as e:
            logger.exception("Панель недоступна при логине: %s", e)
            raise PanelAPIError("Панель недоступна. Попробуйте позже.") from e

        logger.info("POST /login status=%s", r.status_code)
        if r.status_code == 403:
            logger.warning("Логин 403 (часто CSRF). Ответ: %s", r.text[:500])
            raise PanelAPIError(
                "Доступ к панели отклонён (403). Проверьте CSRF и версию 3x-ui."
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
        self._logged_in = True

    async def _ensure_auth(self) -> None:
        """Bearer-токен не требует login; иначе — сессия и CSRF."""
        if self._uses_bearer():
            return
        if not self._logged_in:
            if not self._username or not self._password:
                raise PanelAPIError(
                    "Задайте PANEL_API_TOKEN или пару PANEL_LOGIN / PANEL_PASSWORD."
                )
            await self.login()

    async def _request_panel(
        self,
        method: str,
        path: str,
        *,
        json_body: dict[str, Any] | None = None,
    ) -> httpx.Response:
        client = self._require_client()
        r: httpx.Response | None = None
        for url in self._panel_api_urls(path):
            try:
                if method == "GET":
                    r = await client.get(url, headers=self._api_headers())
                else:
                    r = await client.post(
                        url,
                        json=json_body,
                        headers=self._api_headers(),
                    )
            except httpx.RequestError as e:
                logger.exception("%s %s: %s", method, path, e)
                raise PanelAPIError("Сеть: не удалось связаться с панелью.") from e
            if r.status_code != 404:
                break
        if r is None:
            raise PanelAPIError(f"{method} {path}: нет ответа панели.")
        return r

    def _check_panel_json_response(
        self, r: httpx.Response, op_name: str, detail: str = ""
    ) -> None:
        suffix = f" ({detail})" if detail else ""
        logger.info(
            "%s status=%s body=%s",
            op_name,
            r.status_code,
            (r.text[:400] + "…") if len(r.text) > 400 else r.text,
        )
        if r.status_code != 200:
            raise PanelAPIError(f"{op_name}: HTTP {r.status_code}{suffix}.")
        try:
            body = r.json()
        except json.JSONDecodeError:
            raise PanelAPIError(f"{op_name}: не JSON{suffix}.")
        if not body.get("success", False):
            msg = body.get("msg", str(body))
            raise PanelAPIError(f"{op_name}: {msg}{suffix}")

    async def get_sub_config(self) -> dict[str, Any]:
        """Настройки подписки: POST /panel/setting/all (3x-ui 3.x)."""
        await self._ensure_auth()
        r: httpx.Response | None = None
        last_status = 0
        for path in ("panel/setting/all", "panel/api/setting/all"):
            try:
                candidate = await self._request_panel("POST", path)
            except PanelAPIError:
                continue
            last_status = candidate.status_code
            if candidate.status_code == 404:
                continue
            r = candidate
            break
        if r is None:
            raise PanelAPIError(
                f"setting/all: не удалось получить настройки (HTTP {last_status})."
            )
        if r.status_code == 403:
            raise PanelAPIError("setting/all: 403 — сессия или CSRF.")
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
        """id inbound → protocol. Сначала /inbounds/options, затем /list."""
        out: dict[int, str] = {}
        for path in ("panel/api/inbounds/options", "panel/api/inbounds/list"):
            try:
                r = await self._request_panel("GET", path)
            except PanelAPIError:
                continue
            if r.status_code != 200:
                continue
            try:
                body = r.json()
            except json.JSONDecodeError:
                continue
            if not body.get("success"):
                continue
            raw = body.get("obj")
            if not isinstance(raw, list):
                continue
            for row in raw:
                if not isinstance(row, dict):
                    continue
                try:
                    iid = int(row["id"])
                except (KeyError, TypeError, ValueError):
                    continue
                proto = row.get("protocol")
                out[iid] = proto if isinstance(proto, str) else ""
            if out:
                break
        if not out:
            raise PanelAPIError("Не удалось получить список inbound с панели.")
        logger.info("Протоколы inbound: %s", out)
        return out

    def _target_inbound_ids(self, proto_map: dict[int, str]) -> list[int]:
        configured = set(inbound_ids_config())
        available = set(proto_map.keys())
        chosen = sorted(configured & available)
        if chosen:
            return chosen
        return sorted(available)

    async def _uses_clients_api_v3(self) -> bool:
        if self._clients_api_v3 is not None:
            return self._clients_api_v3
        mode = panel_api_mode()
        if mode == "v3":
            self._clients_api_v3 = True
            logger.info("Панель %s: режим API v3 (3x-ui 3.x)", self._base)
            return True
        if mode == "legacy":
            self._clients_api_v3 = False
            logger.info("Панель %s: режим legacy API (2.x)", self._base)
            return False
        try:
            r = await self._request_panel("GET", "panel/api/clients/list")
            body = r.json()
            found = r.status_code == 200 and bool(body.get("success"))
        except (PanelAPIError, json.JSONDecodeError):
            found = False
        self._clients_api_v3 = found
        logger.info(
            "Панель %s: %s",
            self._base,
            "API v3 (/panel/api/clients/*)"
            if found
            else "legacy (/panel/api/inbounds/addClient)",
        )
        return found

    def _v3_client_body(
        self,
        email: str,
        client_uuid: str,
        sub_id: str,
        expiry_time_ms: int,
        inbound_ids: list[int],
        proto_map: dict[int, str],
        *,
        telegram_id: int | None = None,
    ) -> dict[str, Any]:
        row: dict[str, Any] = {
            "email": email,
            "subId": sub_id,
            "enable": True,
            "expiryTime": expiry_time_ms,
            "limitIp": 0,
            "totalGB": 0,
        }
        if telegram_id is not None and telegram_id > 0:
            row["tgId"] = telegram_id
        protos = {(proto_map.get(i) or "").lower() for i in inbound_ids}
        if "trojan" in protos:
            row["password"] = client_uuid
        if "trojan" not in protos or len(protos) > 1:
            row["id"] = client_uuid
        flow_iid = vless_flow_inbound_id()
        if flow_iid in inbound_ids and (proto_map.get(flow_iid) or "").lower() == "vless":
            row["flow"] = vless_flow_value()
        return row

    async def _add_client_v3(
        self,
        base_email: str,
        client_uuid: str,
        sub_id: str,
        expiry_time_ms: int,
        inbound_ids: list[int],
        proto_map: dict[int, str],
        *,
        telegram_id: int | None = None,
    ) -> None:
        email = panel_client_email(base_email, inbound_ids)
        payload = {
            "client": self._v3_client_body(
                email,
                client_uuid,
                sub_id,
                expiry_time_ms,
                inbound_ids,
                proto_map,
                telegram_id=telegram_id,
            ),
            "inboundIds": inbound_ids,
        }
        r = await self._request_panel(
            "POST", "panel/api/clients/add", json_body=payload
        )
        self._check_panel_json_response(
            r, "clients/add", f"email={email} inbounds={inbound_ids}"
        )

    async def _update_client_v3(
        self,
        email: str,
        client_uuid: str,
        sub_id: str,
        expiry_time_ms: int,
        inbound_ids: list[int],
        proto_map: dict[int, str],
        *,
        telegram_id: int | None = None,
    ) -> None:
        enc = quote(email, safe="")
        path = f"panel/api/clients/update/{enc}"
        payload = self._v3_client_body(
            email,
            client_uuid,
            sub_id,
            expiry_time_ms,
            inbound_ids,
            proto_map,
            telegram_id=telegram_id,
        )
        r = await self._request_panel("POST", path, json_body=payload)
        self._check_panel_json_response(r, "clients/update", email)

    async def _get_client_v3(
        self, email: str
    ) -> tuple[dict[str, Any], list[int]] | None:
        enc = quote(email, safe="")
        r = await self._request_panel("GET", f"panel/api/clients/get/{enc}")
        if r.status_code == 404:
            return None
        self._check_panel_json_response(r, "clients/get", email)
        try:
            body = r.json()
        except json.JSONDecodeError:
            return None
        obj = body.get("obj")
        if not isinstance(obj, dict):
            return None
        client = obj.get("client")
        if not isinstance(client, dict):
            client = obj
        raw_ids = obj.get("inboundIds")
        inbound_ids: list[int] = []
        if isinstance(raw_ids, list):
            for x in raw_ids:
                try:
                    inbound_ids.append(int(x))
                except (TypeError, ValueError):
                    continue
        return client, inbound_ids

    async def _find_client_email_by_sub_id(self, sub_id: str) -> str | None:
        q = quote(sub_id, safe="")
        r = await self._request_panel(
            "GET", f"panel/api/clients/list/paged?search={q}&pageSize=50"
        )
        if r.status_code != 200:
            return None
        try:
            body = r.json()
        except json.JSONDecodeError:
            return None
        if not body.get("success"):
            return None
        obj = body.get("obj")
        if not isinstance(obj, dict):
            return None
        items = obj.get("items")
        if not isinstance(items, list):
            return None
        for row in items:
            if not isinstance(row, dict):
                continue
            if row.get("subId") == sub_id:
                em = row.get("email")
                if isinstance(em, str) and em.strip():
                    return em.strip()
        return None

    async def _attach_client_v3(self, email: str, inbound_ids: list[int]) -> None:
        if not inbound_ids:
            return
        enc = quote(email, safe="")
        path = f"panel/api/clients/{enc}/attach"
        r = await self._request_panel(
            "POST", path, json_body={"inboundIds": inbound_ids}
        )
        self._check_panel_json_response(
            r, "clients/attach", f"email={email} inbounds={inbound_ids}"
        )

    async def _renew_client_v3(
        self,
        base_email: str,
        client_uuid: str,
        sub_id: str,
        expiry_time_ms: int,
        inbound_ids: list[int],
        proto_map: dict[int, str],
        *,
        telegram_id: int | None = None,
    ) -> set[int]:
        """Продление через API v3: attach недостающих inbound + update."""
        updated: set[int] = set()
        candidates: list[str] = []
        canonical = panel_client_email(base_email, inbound_ids)
        candidates.append(canonical)
        by_sub = await self._find_client_email_by_sub_id(sub_id)
        if by_sub and by_sub not in candidates:
            candidates.append(by_sub)

        for email in candidates:
            found = await self._get_client_v3(email)
            if found is None:
                continue
            _, attached = found
            missing = [i for i in inbound_ids if i not in attached]
            if missing:
                logger.info(
                    "Продление v3: привязка %s к inbound %s на %s",
                    email,
                    missing,
                    self._base,
                )
                await self._attach_client_v3(email, missing)
            await self._update_client_v3(
                email,
                client_uuid,
                sub_id,
                expiry_time_ms,
                inbound_ids,
                proto_map,
                telegram_id=telegram_id,
            )
            updated.update(inbound_ids)
            logger.info(
                "Продление v3: обновлён %s (все целевые inbound %s) на %s",
                email,
                inbound_ids,
                self._base,
            )
            break
        return updated

    @staticmethod
    def _client_json_for_protocol(
        protocol: str,
        client_uuid: str,
        email: str,
        sub_id: str,
        expiry_time_ms: int,
        inbound_id: int,
    ) -> dict[str, Any]:
        proto = (protocol or "").strip().lower()
        common = {
            "email": email,
            "subId": sub_id,
            "enable": True,
            "expiryTime": expiry_time_ms,
        }
        if proto == "trojan":
            return {"password": client_uuid, **common}
        row: dict[str, Any] = {"id": client_uuid, **common}
        if inbound_id == vless_flow_inbound_id() and proto == "vless":
            row["flow"] = vless_flow_value()
        return row

    async def _add_client_legacy(
        self,
        inbound_id: int,
        client_uuid: str,
        email: str,
        sub_id: str,
        protocol: str,
        expiry_time_ms: int,
    ) -> None:
        client_row = self._client_json_for_protocol(
            protocol, client_uuid, email, sub_id, expiry_time_ms, inbound_id
        )
        settings_obj = {"clients": [client_row]}
        payload = {
            "id": inbound_id,
            "settings": json.dumps(settings_obj, separators=(",", ":")),
        }
        r = await self._request_panel(
            "POST", "panel/api/inbounds/addClient", json_body=payload
        )
        self._check_panel_json_response(
            r, "inbounds/addClient", f"inbound={inbound_id}"
        )

    async def _update_client_legacy(
        self,
        inbound_id: int,
        client_uuid: str,
        email: str,
        sub_id: str,
        protocol: str,
        expiry_time_ms: int,
    ) -> None:
        client_row = self._client_json_for_protocol(
            protocol, client_uuid, email, sub_id, expiry_time_ms, inbound_id
        )
        settings_obj = {"clients": [client_row]}
        payload = {
            "id": inbound_id,
            "settings": json.dumps(settings_obj, separators=(",", ":")),
        }
        path = f"panel/api/inbounds/updateClient/{client_uuid}"
        r = await self._request_panel("POST", path, json_body=payload)
        self._check_panel_json_response(
            r, "inbounds/updateClient", f"inbound={inbound_id}"
        )

    async def update_user_on_all_inbounds(
        self,
        base_email: str,
        client_uuid: str,
        sub_id: str,
        expiry_time_ms: int,
        *,
        telegram_id: int | None = None,
    ) -> int:
        """Продлевает клиента на всех inbound (v3 + legacy-записи со старых выдач)."""
        await self._ensure_auth()
        proto_map = await self._inbound_protocol_map()
        inbound_ids = self._target_inbound_ids(proto_map)
        updated_inbound_ids: set[int] = set()

        if await self._uses_clients_api_v3():
            try:
                v3_done = await self._renew_client_v3(
                    base_email,
                    client_uuid,
                    sub_id,
                    expiry_time_ms,
                    inbound_ids,
                    proto_map,
                    telegram_id=telegram_id,
                )
                updated_inbound_ids |= v3_done
            except PanelAPIError as e:
                logger.warning(
                    "Продление v3 на %s: %s — продолжаем legacy по inbound",
                    self._base,
                    e,
                )

        for iid in inbound_ids:
            legacy_email = f"{base_email}_{iid}"
            try:
                await self._update_client_legacy(
                    iid,
                    client_uuid,
                    legacy_email,
                    sub_id,
                    proto_map.get(iid, ""),
                    expiry_time_ms,
                )
                updated_inbound_ids.add(iid)
                logger.info(
                    "Продление legacy: inbound %s (%s) на %s",
                    iid,
                    legacy_email,
                    self._base,
                )
            except PanelAPIError as e:
                logger.debug(
                    "Продление legacy: inbound %s (%s) на %s: %s",
                    iid,
                    legacy_email,
                    self._base,
                    e,
                )

        logger.info(
            "Продление итог на %s: обновлены inbound %s из %s",
            self._base,
            sorted(updated_inbound_ids),
            inbound_ids,
        )
        return len(updated_inbound_ids)

    async def register_user_on_all_inbounds(
        self,
        base_email: str,
        client_uuid: str,
        sub_id: str,
        expiry_time_ms: int | None = None,
        *,
        telegram_id: int | None = None,
    ) -> None:
        await self._ensure_auth()
        expiry_ms = (
            expiry_time_ms
            if expiry_time_ms is not None
            else subscription_expiry_time_ms()
        )
        proto_map = await self._inbound_protocol_map()
        inbound_ids = self._target_inbound_ids(proto_map)
        if not inbound_ids:
            raise PanelAPIError("На панели нет inbound для выдачи доступа.")

        use_v3 = await self._uses_clients_api_v3()
        logger.info(
            "register_user_on_all_inbounds: build=%s mode=%s v3=%s base=%s inbounds=%s",
            PANEL_API_BUILD,
            panel_api_mode(),
            use_v3,
            self._base,
            inbound_ids,
        )
        if use_v3:
            await self._add_client_v3(
                base_email,
                client_uuid,
                sub_id,
                expiry_ms,
                inbound_ids,
                proto_map,
                telegram_id=telegram_id,
            )
            return

        logger.warning(
            "register_user: legacy addClient (проверьте PANEL_API_MODE и что на сервере актуальный panel_api.py)"
        )
        for iid in inbound_ids:
            await self._add_client_legacy(
                iid,
                client_uuid,
                f"{base_email}_{iid}",
                sub_id,
                proto_map.get(iid, ""),
                expiry_ms,
            )
