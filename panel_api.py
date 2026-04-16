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
    end = datetime.now(timezone.utc) + timedelta(days=subscription_days())
    return int(end.timestamp() * 1000)


class PanelAPIError(Exception):
    """Ошибка API панели или сети."""


class PanelAPI:
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

    async def __aenter__(self) -> PanelAPI:
        self._client = httpx.AsyncClient(
            base_url=self._base,
            timeout=self._timeout,
            follow_redirects=True,
        )
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
        try:
            r = await client.post(
                "/login",
                data={"username": self._username, "password": self._password},
            )
        except httpx.RequestError as e:
            logger.exception("Панель недоступна при логине: %s", e)
            raise PanelAPIError("Панель недоступна. Попробуйте позже.") from e

        logger.info("POST /login status=%s", r.status_code)
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

    async def _inbound_protocol_map(self) -> dict[int, str]:
        """id inbound → protocol (как в панели: vless, trojan, shadowsocks, …)."""
        client = self._require_client()
        try:
            r = await client.get("/panel/api/inbounds/list")
        except httpx.RequestError as e:
            logger.exception("inbounds/list: %s", e)
            raise PanelAPIError("Не удалось получить список inbound.") from e
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
        try:
            r = await client.post("/panel/api/inbounds/addClient", json=payload)
        except httpx.RequestError as e:
            logger.exception("addClient inbound=%s: %s", inbound_id, e)
            raise PanelAPIError("Сеть: не удалось связаться с панелью.") from e

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

    async def register_user_on_all_inbounds(
        self,
        base_email: str,
        client_uuid: str,
        sub_id: str,
    ) -> None:
        await self.login()
        expiry_ms = subscription_expiry_time_ms()
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
