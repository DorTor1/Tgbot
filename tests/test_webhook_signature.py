"""Тесты безопасности webhook (C4 + C5).

Покрывает чистые функции из webhook.py:
  1. test_parse_content_signature_header — разбор `Content-Signature: value=<hex>`
     (формат ЮKassa по https://yookassa.ru/developers/using-api/webhooks).
  2. test_verify_signature_ok — корректная подпись → True.
  3. test_verify_signature_bad — изменённое тело или поддельный secret → False.
  4. test_verify_signature_empty_secret — пустой secret → False.
  5. test_verify_signature_empty_header — пустой/None заголовок → False.
  6. test_ip_allowed_in_whitelist — IP из подсети ЮKassa → True.
  7. test_ip_allowed_outside_whitelist — чужой IP → False.
  8. test_ip_allowed_trust_proxy_xff — YOOKASSA_TRUST_PROXY=1 + X-Forwarded-For
     берёт первый IP из XFF.
  9. test_ip_allowed_trust_proxy_disabled — YOOKASSA_TRUST_PROXY=0 берёт
     request.remote, игнорирует XFF.
 10. test_ip_allowed_single_host — 77.75.156.11 / 77.75.156.35 (из доки
     ЮKassa — одиночные IP без префикса) тоже проходят.

Запускается без pytest: `python tests/test_webhook_signature.py`.
"""
from __future__ import annotations

import hashlib
import hmac
import sys
import unittest
from pathlib import Path
from unittest import IsolatedAsyncioTestCase

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def _make_signature(body: bytes, secret: str) -> str:
    """Эталонная HMAC-SHA256 от body с secret (как считает ЮKassa)."""
    return hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()


class TestParseContentSignatureHeader(unittest.TestCase):
    def test_value_prefix(self) -> None:
        from webhook import parse_content_signature_header

        self.assertEqual(
            parse_content_signature_header("value=abc123def"),
            "abc123def",
        )

    def test_bare_hex(self) -> None:
        from webhook import parse_content_signature_header

        self.assertEqual(
            parse_content_signature_header("abc123def"),
            "abc123def",
        )

    def test_value_prefix_with_whitespace(self) -> None:
        from webhook import parse_content_signature_header

        self.assertEqual(
            parse_content_signature_header("value= abc123 "),
            "abc123",
        )

    def test_empty(self) -> None:
        from webhook import parse_content_signature_header

        self.assertIsNone(parse_content_signature_header(""))
        self.assertIsNone(parse_content_signature_header(None))
        self.assertIsNone(parse_content_signature_header("value="))

    def test_invalid_hex_chars_kept_for_compare(self) -> None:
        """Хедер с мусором — возвращаем как есть, compare_digest() отклонит."""
        from webhook import parse_content_signature_header

        self.assertEqual(
            parse_content_signature_header("value=zzz"),
            "zzz",
        )


class TestVerifySignature(IsolatedAsyncioTestCase):
    async def test_ok(self) -> None:
        from webhook import verify_yookassa_signature

        body = b'{"event":"payment.succeeded","object":{"id":"abc"}}'
        secret = "test-secret-123"
        sig = _make_signature(body, secret)
        header = f"value={sig}"
        self.assertTrue(verify_yookassa_signature(body, header, secret))

    async def test_bare_header_ok(self) -> None:
        """Без префикса `value=` тоже работает (для совместимости)."""
        from webhook import verify_yookassa_signature

        body = b"some body"
        secret = "s"
        sig = _make_signature(body, secret)
        self.assertTrue(verify_yookassa_signature(body, sig, secret))

    async def test_bad_body(self) -> None:
        from webhook import verify_yookassa_signature

        secret = "test-secret"
        body = b"original body"
        sig = _make_signature(body, secret)
        self.assertFalse(
            verify_yookassa_signature(b"tampered body", f"value={sig}", secret)
        )

    async def test_bad_secret(self) -> None:
        from webhook import verify_yookassa_signature

        body = b"x"
        sig = _make_signature(body, "real-secret")
        self.assertFalse(
            verify_yookassa_signature(body, f"value={sig}", "wrong-secret")
        )

    async def test_empty_secret(self) -> None:
        from webhook import verify_yookassa_signature

        body = b"x"
        sig = _make_signature(body, "real")
        self.assertFalse(verify_yookassa_signature(body, f"value={sig}", ""))

    async def test_empty_header(self) -> None:
        from webhook import verify_yookassa_signature

        self.assertFalse(verify_yookassa_signature(b"x", "", "secret"))
        self.assertFalse(verify_yookassa_signature(b"x", None, "secret"))


class TestIpAllowed(unittest.TestCase):
    def setUp(self) -> None:
        from webhook import YOOKASSA_IP_NETWORKS

        self.networks = YOOKASSA_IP_NETWORKS

    def _ip(self, remote: str, trust_proxy: bool, xff: str | None) -> bool:
        from webhook import ip_allowed

        return ip_allowed(
            remote=remote,
            trust_proxy=trust_proxy,
            x_forwarded_for=xff,
            networks=self.networks,
        )

    def test_in_whitelist_27(self) -> None:
        # 185.71.76.0/27 — диапазон 185.71.76.0 .. 185.71.76.31
        self.assertTrue(self._ip("185.71.76.5", False, None))
        self.assertTrue(self._ip("185.71.76.31", False, None))
        self.assertTrue(self._ip("185.71.77.15", False, None))
        # 185.71.76.32 — за пределами /27
        self.assertFalse(self._ip("185.71.76.32", False, None))

    def test_in_whitelist_25(self) -> None:
        # 77.75.153.0/25 — 77.75.153.0 .. 77.75.153.127
        self.assertTrue(self._ip("77.75.153.0", False, None))
        self.assertTrue(self._ip("77.75.153.127", False, None))
        self.assertTrue(self._ip("77.75.154.200", False, None))
        # 77.75.153.128 — за пределами /25
        self.assertFalse(self._ip("77.75.153.128", False, None))

    def test_single_hosts(self) -> None:
        """77.75.156.11 и 77.75.156.35 — одиночные IP (без префикса) по доке."""
        self.assertTrue(self._ip("77.75.156.11", False, None))
        self.assertTrue(self._ip("77.75.156.35", False, None))
        # Соседний IP — не из whitelist'а
        self.assertFalse(self._ip("77.75.156.10", False, None))
        self.assertFalse(self._ip("77.75.156.12", False, None))

    def test_outside_whitelist(self) -> None:
        self.assertFalse(self._ip("8.8.8.8", False, None))
        self.assertFalse(self._ip("185.71.78.1", False, None))  # соседняя подсеть
        self.assertFalse(self._ip("127.0.0.1", False, None))

    def test_trust_proxy_xff(self) -> None:
        # trust_proxy=True: берём первый IP из XFF, игнорируем remote.
        self.assertTrue(self._ip("127.0.0.1", True, "185.71.76.5, 10.0.0.1"))
        self.assertFalse(self._ip("127.0.0.1", True, "8.8.8.8, 10.0.0.1"))

    def test_trust_proxy_disabled(self) -> None:
        # trust_proxy=False: игнорируем XFF, берём remote.
        # remote — 127.0.0.1 (nginx на той же машине) — отлуп.
        self.assertFalse(self._ip("127.0.0.1", False, "185.71.76.5, 10.0.0.1"))
        # remote — IP ЮKassa напрямую (без nginx) — ок.
        self.assertTrue(self._ip("185.71.76.5", False, "8.8.8.8"))

    def test_invalid_ip_string(self) -> None:
        self.assertFalse(self._ip("not-an-ip", False, None))
        self.assertFalse(self._ip("", False, None))

    def test_ipv6_whitelist(self) -> None:
        # 2a02:5180::/32 — большой блок IPv6
        self.assertTrue(self._ip("2a02:5180::1", False, None))
        # За пределами /32
        self.assertFalse(self._ip("2a02:5181::1", False, None))


class TestKillSwitchBehavior(unittest.TestCase):
    """Проверяем логику kill-switch: при YOOKASSA_REQUIRE_SIGNATURE=0
    middleware должен пропускать любой запрос без проверок.
    """

    def test_kill_switch_disabled(self) -> None:
        import os

        os.environ["YOOKASSA_REQUIRE_SIGNATURE"] = "0"
        # Импортируем ПОСЛЕ установки env, чтобы функция-конфиг
        # увидела актуальное значение.
        import importlib
        import webhook

        importlib.reload(webhook)
        # Теперь проверим: при require_signature=False ip_allowed всё равно
        # работает (его дёргает middleware), но middleware его НЕ вызывает —
        # пропускает как есть. Проверим через флаг модуля.
        self.assertFalse(webhook._signature_required())


def main() -> None:
    unittest.main(verbosity=2, exit=True)


if __name__ == "__main__":
    main()
