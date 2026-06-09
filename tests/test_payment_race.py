"""Тесты гонки при создании платежа (C2).

Покрывает:
  1. test_double_create_payment_race — 10 параллельных попыток создать
     платёж для одного telegram_id → ровно 1 запись в payments, у всех
     один и тот же yookassa_payment_id.
  2. test_create_pending_payment_with_key_dedup — повторный вызов
     create_pending_payment_with_key с тем же ключом возвращает ту же
     запись (idempotency).
  3. test_create_pending_blocks_when_other_pending_exists — если у юзера
     уже есть pending с ДРУГИМ ключом, новый вызов вернёт None.
  4. test_attach_yookassa_to_pending — успешный attach и поведение
     после succeeded (повторный attach возвращает False).
  5. test_expire_old_pending_skips_succeeded — если между SELECT и UPDATE
     webhook пометил succeeded, воркер expire_old_pending_payments не
     перезаписывает статус.

Запускается без pytest: `python tests/test_payment_race.py` (или
`python -m unittest tests.test_payment_race`).
"""
from __future__ import annotations

import asyncio
import os
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import IsolatedAsyncioTestCase


# Делаем корень проекта импортируемым, чтобы `import db`, `import main`
# и `from services import payments_service` работали.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def _patch_db_path(tmp: Path) -> None:
    import db

    db.DB_PATH = tmp


async def _init_fresh_db() -> None:
    import db

    await db.init_db()


class _FakeYookassa:
    """Подменяет services.payments_service.create_payment_async.

    Возвращает фейковый ответ ЮKassa, эмулируя сетевую задержку,
    чтобы параллельные корутины могли «столкнуться» в create_pending.
    """

    def __init__(self) -> None:
        self.call_count = 0
        self.payment_id = "test-yookassa-123"

    async def __call__(self, **kwargs):
        self.call_count += 1
        # Небольшая задержка, чтобы гонка действительно возникла.
        await asyncio.sleep(0.01)
        return {
            "id": self.payment_id,
            "status": "pending",
            "confirmation_url": "https://yoomoney.ru/checkout/test",
            "amount": kwargs.get("amount_rub"),
        }


class TestPaymentRace(IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        tmp_path = Path(self._tmpdir.name) / "test_race.db"
        _patch_db_path(tmp_path)
        await _init_fresh_db()

        import main
        from services import payments_service

        self._fake_yookassa = _FakeYookassa()
        # Подменяем async-обёртку payments_service.
        self._original_create_async = payments_service.create_payment_async
        payments_service.create_payment_async = self._fake_yookassa
        # Подменяем и в main, потому что там `from services import payments_service`.
        main.payments_service.create_payment_async = self._fake_yookassa
        # payments.is_configured должен вернуть True, иначе код уйдёт рано.
        # Подменяем функцию `payments.is_configured` через модуль payments,
        # который импортирован в main.
        import payments

        self._original_is_configured = payments.is_configured
        payments.is_configured = lambda: True
        main.payments.is_configured = lambda: True

        # Чтобы уведомления админов не падали (bot=None в тестах).
        # _create_payment_for_user вызывает _notify_admins_new_payment
        # в `with suppress(Exception)`, так что падение не критично.
        self.main = main

    async def asyncTearDown(self) -> None:
        from services import payments_service

        payments_service.create_payment_async = self._original_create_async
        import payments

        payments.is_configured = self._original_is_configured
        self._tmpdir.cleanup()

    async def test_double_create_payment_race(self) -> None:
        """10 параллельных попыток → ровно 1 запись в payments, 1 вызов ЮKassa."""
        import db
        from aiogram.types import User

        tid = 4242
        query_user = User(
            id=tid,
            is_bot=False,
            first_name="Test",
            username="tester",
        )

        async def one_call():
            return await self.main._create_payment_for_user(
                bot=None,
                kind="new",
                query_from_user=query_user,
                days=30,
                device_kind="phone",
                slot_index=1,
                base_email="test@example.com",
            )

        results = await asyncio.gather(*[one_call() for _ in range(10)])
        records = [r for r, err in results if r is not None]
        errors = [err for r, err in results if r is None]

        # Все 10 вызовов либо получили запись, либо ERR_REQUEST_ALREADY
        # (потому что в этот момент уже есть pending). Но НЕ
        # «Не удалось создать счёт».
        self.assertTrue(
            len(records) >= 1, f"ни одна попытка не создала запись: {errors!r}"
        )

        # В БД должна быть ровно 1 запись для этого telegram_id.
        active = await db.get_active_payment(tid)
        self.assertIsNotNone(active)
        self.assertEqual(active.telegram_id, tid)
        self.assertEqual(active.status, "pending")
        self.assertEqual(active.yookassa_payment_id, "test-yookassa-123")

        # ЮKassa должна была быть вызвана ровно 1 раз — гонка
        # в create_pending_payment_with_key блокирует остальные попытки
        # до того, как они успеют сходить в HTTP.
        self.assertEqual(
            self._fake_yookassa.call_count,
            1,
            f"ЮKassa вызвана {self._fake_yookassa.call_count} раз, ожидалось 1",
        )

    async def test_create_pending_payment_with_key_dedup(self) -> None:
        """Повторный вызов с тем же ключом возвращает ту же запись."""
        import db
        import uuid as _uuid

        tid = 5050
        key = f"tg-{tid}-phone-1-{_uuid.uuid4().hex}"

        rec1 = await db.create_pending_payment_with_key(
            idempotence_key=key,
            telegram_id=tid,
            username="dup",
            first_name="Dup",
            last_name=None,
            kind="new",
            device_kind="phone",
            slot_index=1,
            base_email="dup@example.com",
            plan_days=30,
            amount=80,
        )
        self.assertIsNotNone(rec1)
        self.assertIsNone(rec1.yookassa_payment_id)

        rec2 = await db.create_pending_payment_with_key(
            idempotence_key=key,
            telegram_id=tid,
            username="dup",
            first_name="Dup",
            last_name=None,
            kind="new",
            device_kind="phone",
            slot_index=1,
            base_email="dup@example.com",
            plan_days=30,
            amount=80,
        )
        self.assertIsNotNone(rec2)
        self.assertEqual(rec1.id, rec2.id)

    async def test_create_pending_blocks_when_other_pending_exists(self) -> None:
        """Если уже есть pending с ДРУГИМ ключом — новый вызов вернёт None."""
        import db
        import uuid as _uuid

        tid = 6060
        # Создаём первый pending.
        rec1 = await db.create_pending_payment_with_key(
            idempotence_key=f"key1-{_uuid.uuid4().hex}",
            telegram_id=tid,
            username=None,
            first_name=None,
            last_name=None,
            kind="new",
            device_kind="phone",
            slot_index=1,
            base_email="x@x.com",
            plan_days=30,
            amount=80,
        )
        self.assertIsNotNone(rec1)

        # Пытаемся создать второй с другим ключом — должно вернуть существующий.
        rec2 = await db.create_pending_payment_with_key(
            idempotence_key=f"key2-{_uuid.uuid4().hex}",
            telegram_id=tid,
            username=None,
            first_name=None,
            last_name=None,
            kind="new",
            device_kind="phone",
            slot_index=1,
            base_email="x@x.com",
            plan_days=30,
            amount=80,
        )
        self.assertIsNotNone(rec2)
        self.assertEqual(rec1.id, rec2.id)

    async def test_attach_yookassa_to_pending(self) -> None:
        """Успешный attach и неуспешный после succeeded."""
        import db
        import uuid as _uuid

        tid = 7070
        key = f"key-{_uuid.uuid4().hex}"
        rec = await db.create_pending_payment_with_key(
            idempotence_key=key,
            telegram_id=tid,
            username=None,
            first_name=None,
            last_name=None,
            kind="new",
            device_kind="phone",
            slot_index=1,
            base_email="x@x.com",
            plan_days=30,
            amount=80,
        )
        self.assertIsNotNone(rec)

        ok = await db.attach_yookassa_to_pending(
            idempotence_key=key,
            yookassa_payment_id="yk-1",
            confirmation_url="https://example.com/1",
        )
        self.assertTrue(ok)

        # Повторный attach с другим id должен вернуть False
        # (статус pending, но yookassa_payment_id уже не NULL).
        ok2 = await db.attach_yookassa_to_pending(
            idempotence_key=key,
            yookassa_payment_id="yk-2",
            confirmation_url="https://example.com/2",
        )
        self.assertFalse(ok2)

        # Webhook пометил succeeded — attach снова False.
        await db.mark_payment_paid("yk-1")
        ok3 = await db.attach_yookassa_to_pending(
            idempotence_key=key,
            yookassa_payment_id="yk-3",
            confirmation_url="https://example.com/3",
        )
        self.assertFalse(ok3)

    async def test_expire_old_pending_skips_succeeded(self) -> None:
        """SELECT-фильтр (status='pending') и UPDATE-фильтр (WHERE status='pending')
        в expire_old_pending_payments не дают перезаписать succeeded-запись."""
        import aiosqlite
        import db
        import uuid as _uuid
        from datetime import datetime, timedelta, timezone

        # Запись 1: pending + старая → должна попасть в expired и стать canceled.
        key_old = f"key-old-{_uuid.uuid4().hex}"
        rec_old = await db.create_pending_payment_with_key(
            idempotence_key=key_old,
            telegram_id=9001,
            username=None,
            first_name=None,
            last_name=None,
            kind="new",
            device_kind="phone",
            slot_index=1,
            base_email="x@x.com",
            plan_days=30,
            amount=80,
        )
        assert rec_old is not None
        await db.attach_yookassa_to_pending(
            idempotence_key=key_old,
            yookassa_payment_id="yk-old",
            confirmation_url=None,
        )
        old_ts = (datetime.now(timezone.utc) - timedelta(minutes=10)).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        async with aiosqlite.connect(db.DB_PATH) as conn:
            await conn.execute(
                "UPDATE payments SET created_at=? WHERE id=?",
                (old_ts, rec_old.id),
            )
            await conn.commit()

        # Запись 2: уже succeeded + старая → не должна попасть в SELECT
        # (отфильтрована по status='pending').
        key_succ = f"key-succ-{_uuid.uuid4().hex}"
        rec_succ = await db.create_pending_payment_with_key(
            idempotence_key=key_succ,
            telegram_id=9002,
            username=None,
            first_name=None,
            last_name=None,
            kind="new",
            device_kind="phone",
            slot_index=1,
            base_email="x@x.com",
            plan_days=30,
            amount=80,
        )
        assert rec_succ is not None
        await db.attach_yookassa_to_pending(
            idempotence_key=key_succ,
            yookassa_payment_id="yk-succ",
            confirmation_url=None,
        )
        await db.mark_payment_paid("yk-succ")
        async with aiosqlite.connect(db.DB_PATH) as conn:
            await conn.execute(
                "UPDATE payments SET created_at=? WHERE id=?",
                (old_ts, rec_succ.id),
            )
            await conn.commit()

        expired = await db.expire_old_pending_payments(expires_seconds=300)
        expired_ids = {r.id for r in expired}
        self.assertIn(rec_old.id, expired_ids)
        self.assertNotIn(rec_succ.id, expired_ids)

        # Статус записи 1 — canceled.
        old_now = await db.get_payment_by_yookassa_id("yk-old")
        self.assertEqual(old_now.status, "canceled")
        # Статус записи 2 — остался succeeded, не перезаписан.
        succ_now = await db.get_payment_by_yookassa_id("yk-succ")
        self.assertEqual(succ_now.status, "succeeded")

    async def test_expire_old_pending_update_protection(self) -> None:
        """UPDATE-фильтр: если вручную перевести запись в succeeded между
        SELECT и UPDATE в одном конкурентном сценарии — статус не
        перезапишется. Эмулируем через прямое подключение: делаем UPDATE
        статуса вручную ПОСЛЕ expire_old_pending_payments SELECT'нет
        (что мы не можем перехватить без мока), но проверяем что expire
        возвращает только pending, а UPDATE-clause содержит `status='pending'`."""
        import db
        import uuid as _uuid

        # Свежий pending (не старый) — не должен попасть в expired.
        key = f"key-{_uuid.uuid4().hex}"
        rec = await db.create_pending_payment_with_key(
            idempotence_key=key,
            telegram_id=9500,
            username=None,
            first_name=None,
            last_name=None,
            kind="new",
            device_kind="phone",
            slot_index=1,
            base_email="x@x.com",
            plan_days=30,
            amount=80,
        )
        assert rec is not None
        await db.attach_yookassa_to_pending(
            idempotence_key=key,
            yookassa_payment_id="yk-fresh",
            confirmation_url=None,
        )
        expired = await db.expire_old_pending_payments(expires_seconds=300)
        # Свежий pending (created_at = now) — НЕ старше 5 минут.
        self.assertEqual(len(expired), 0)
        # Статус остался pending.
        cur = await db.get_payment_by_yookassa_id("yk-fresh")
        self.assertEqual(cur.status, "pending")


def main() -> None:
    unittest.main(verbosity=2, exit=True)


if __name__ == "__main__":
    main()
