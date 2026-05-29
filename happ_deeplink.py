"""Deep link Happ: happ://add/<url_подписки>#<имя_профиля>

Стандартный импорт подписки на iOS и Android.
Документация: https://www.happ.su/main/faq/adding-configuration-subscription
"""

from __future__ import annotations

import os
from urllib.parse import quote


def happ_deeplink_enabled() -> bool:
    v = os.getenv("HAPP_DEEPLINK", "1").strip().lower()
    return v not in ("0", "false", "no", "off")


def build_happ_deeplink(subscription_url: str, profile_name: str = "") -> str | None:
    """happ://add/… — для синей ссылки «Открыть в Happ» в сообщении."""
    sub = subscription_url.strip()
    if not sub or not happ_deeplink_enabled():
        return None
    deeplink = f"happ://add/{sub}"
    name = (profile_name or "").strip()
    if name:
        deeplink += f"#{quote(name, safe='')}"
    return deeplink
