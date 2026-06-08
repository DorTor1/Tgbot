"""SQLite: устройства пользователей, заявки на доступ."""

import aiosqlite
import logging
from dataclasses import dataclass
from pathlib import Path

from panel_api import subscription_days

logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).resolve().parent / "bot.db"


@dataclass(frozen=True)
class UserDeviceRecord:
    telegram_id: int
    device_kind: str
    slot_index: int
    base_email: str
    uuid: str
    sub_token: str
    expiry_time_ms: int | None
    reminder_3d_sent_at: str | None
    reminder_1d_sent_at: str | None
    expired_notified_at: str | None


@dataclass(frozen=True)
class AccessRequestRecord:
    telegram_id: int
    username: str | None
    first_name: str | None
    last_name: str | None
    base_email: str
    device_kind: str
    slot_index: int


@dataclass(frozen=True)
class PaymentRecord:
    id: int
    telegram_id: int
    username: str | None
    first_name: str | None
    last_name: str | None
    kind: str  # "new" | "renewal"
    device_kind: str
    slot_index: int
    base_email: str
    plan_days: int
    amount: int
    yookassa_payment_id: str
    confirmation_url: str | None
    status: str  # "pending" | "succeeded" | "canceled"
    created_at: str


async def _migrate_schema(conn: aiosqlite.Connection) -> None:
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS user_devices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_id INTEGER NOT NULL,
            device_kind TEXT NOT NULL,
            slot_index INTEGER NOT NULL,
            base_email TEXT NOT NULL,
            uuid TEXT NOT NULL,
            sub_token TEXT NOT NULL,
            created_at TEXT DEFAULT (datetime('now')),
            UNIQUE (telegram_id, device_kind, slot_index)
        )
        """
    )
    cur = await conn.execute("PRAGMA table_info(user_devices)")
    user_device_cols = {r[1] for r in await cur.fetchall()}
    if "expiry_time_ms" not in user_device_cols:
        await conn.execute("ALTER TABLE user_devices ADD COLUMN expiry_time_ms INTEGER")
    if "reminder_3d_sent_at" not in user_device_cols:
        await conn.execute(
            "ALTER TABLE user_devices ADD COLUMN reminder_3d_sent_at TEXT"
        )
    if "reminder_1d_sent_at" not in user_device_cols:
        await conn.execute(
            "ALTER TABLE user_devices ADD COLUMN reminder_1d_sent_at TEXT"
        )
    if "expired_notified_at" not in user_device_cols:
        await conn.execute(
            "ALTER TABLE user_devices ADD COLUMN expired_notified_at TEXT"
        )
    await conn.execute(
        """
        UPDATE user_devices
        SET expiry_time_ms = CAST(strftime('%s', created_at) AS INTEGER) * 1000 + (? * 86400000)
        WHERE expiry_time_ms IS NULL AND created_at IS NOT NULL
        """,
        (subscription_days(),),
    )
    cur = await conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='users'"
    )
    if await cur.fetchone():
        await conn.execute(
            """
            INSERT OR IGNORE INTO user_devices
            (telegram_id, device_kind, slot_index, base_email, uuid, sub_token)
            SELECT telegram_id, 'other', 1,
                   'legacy_' || CAST(telegram_id AS TEXT), uuid, sub_token
            FROM users
            """
        )
        await conn.execute(
            """
            UPDATE user_devices
            SET expiry_time_ms = CAST(strftime('%s', created_at) AS INTEGER) * 1000 + (? * 86400000)
            WHERE expiry_time_ms IS NULL AND created_at IS NOT NULL
            """,
            (subscription_days(),),
        )

    cur = await conn.execute("PRAGMA table_info(access_requests)")
    cols = {r[1] for r in await cur.fetchall()}
    if "device_kind" not in cols:
        await conn.execute(
            "ALTER TABLE access_requests ADD COLUMN device_kind TEXT NOT NULL DEFAULT 'other'"
        )
    if "slot_index" not in cols:
        await conn.execute(
            "ALTER TABLE access_requests ADD COLUMN slot_index INTEGER NOT NULL DEFAULT 1"
        )

    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS renewal_requests (
            telegram_id INTEGER NOT NULL,
            device_kind TEXT NOT NULL,
            slot_index INTEGER NOT NULL,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            PRIMARY KEY (telegram_id, device_kind, slot_index)
        )
        """
    )

    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS terms_acceptance (
            telegram_id INTEGER PRIMARY KEY,
            accepted_at TEXT DEFAULT (datetime('now'))
        )
        """
    )
    cur = await conn.execute("PRAGMA table_info(terms_acceptance)")
    ta_cols = {r[1] for r in await cur.fetchall()}
    if ta_cols and "agreement_accepted_at" not in ta_cols:
        await conn.execute(
            "ALTER TABLE terms_acceptance ADD COLUMN agreement_accepted_at TEXT"
        )
        await conn.execute(
            """
            UPDATE terms_acceptance
            SET agreement_accepted_at = accepted_at
            WHERE agreement_accepted_at IS NULL AND accepted_at IS NOT NULL
            """
        )
    await conn.execute(
        """
        INSERT OR IGNORE INTO terms_acceptance (telegram_id, accepted_at)
        SELECT DISTINCT telegram_id, datetime('now') FROM user_devices
        """
    )
    await conn.execute(
        """
        UPDATE terms_acceptance
        SET agreement_accepted_at = accepted_at
        WHERE agreement_accepted_at IS NULL AND accepted_at IS NOT NULL
        """
    )

    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS payments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_id INTEGER NOT NULL,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            kind TEXT NOT NULL,
            device_kind TEXT NOT NULL,
            slot_index INTEGER NOT NULL,
            base_email TEXT NOT NULL,
            plan_days INTEGER NOT NULL,
            amount INTEGER NOT NULL,
            yookassa_payment_id TEXT UNIQUE,
            confirmation_url TEXT,
            status TEXT NOT NULL DEFAULT 'pending',
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now')),
            idempotence_key TEXT
        )
        """
    )

    cur = await conn.execute("PRAGMA table_info(payments)")
    payments_cols = {r[1]: r for r in await cur.fetchall()}

    if "idempotence_key" not in payments_cols:
        await conn.execute("ALTER TABLE payments ADD COLUMN idempotence_key TEXT")

    yookassa_col = payments_cols.get("yookassa_payment_id")
    if yookassa_col is not None and yookassa_col[3] == 1:
        await _rebuild_payments_table_drop_notnull(conn)

    await conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_payments_idempotence_key "
        "ON payments(idempotence_key)"
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_payments_tid_status ON payments(telegram_id, status)"
    )


async def _rebuild_payments_table_drop_notnull(conn: aiosqlite.Connection) -> None:
    """Пересоздаёт payments, убирая NOT NULL с yookassa_payment_id (нужно для C2).

    В SQLite ALTER TABLE ... DROP NOT NULL поддержан с 3.35.0, но проект
    может крутиться на более старых сборках — поэтому используем безопасный
    12-шаговый rebuild. Внутри одной транзакции: RENAME → CREATE → COPY → DROP.
    При ошибке — ROLLBACK и попытка вернуть имя таблицы.
    """
    try:
        await conn.execute("BEGIN")
        await conn.execute("ALTER TABLE payments RENAME TO payments__migrating")
        await conn.execute(
            """
            CREATE TABLE payments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                telegram_id INTEGER NOT NULL,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                kind TEXT NOT NULL,
                device_kind TEXT NOT NULL,
                slot_index INTEGER NOT NULL,
                base_email TEXT NOT NULL,
                plan_days INTEGER NOT NULL,
                amount INTEGER NOT NULL,
                yookassa_payment_id TEXT UNIQUE,
                confirmation_url TEXT,
                status TEXT NOT NULL DEFAULT 'pending',
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now')),
                idempotence_key TEXT
            )
            """
        )
        await conn.execute(
            """
            INSERT INTO payments (
                id, telegram_id, username, first_name, last_name, kind,
                device_kind, slot_index, base_email, plan_days, amount,
                yookassa_payment_id, confirmation_url,
                status, created_at, updated_at
            )
            SELECT
                id, telegram_id, username, first_name, last_name, kind,
                device_kind, slot_index, base_email, plan_days, amount,
                yookassa_payment_id, confirmation_url,
                status,
                COALESCE(created_at, datetime('now')),
                COALESCE(updated_at, datetime('now'))
            FROM payments__migrating
            """
        )
        await conn.execute("DROP TABLE payments__migrating")
        await conn.execute("COMMIT")
        logger.info("Миграция payments: убран NOT NULL с yookassa_payment_id")
    except Exception:
        await conn.execute("ROLLBACK")
        try:
            cur = await conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='payments__migrating'"
            )
            if await cur.fetchone():
                await conn.execute("ALTER TABLE payments__migrating RENAME TO payments")
        except Exception:
            logger.exception(
                "Не удалось восстановить payments после неудачной миграции"
            )
        raise


async def init_db() -> None:
    # N11: WAL + busy_timeout в ОТДЕЛЬНОМ соединении (PRAGMA synchronous
    # нельзя менять внутри транзакции, а PRAGMA journal_mode в сочетании
    # с synchronous тоже требует «чистого» состояния).
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("PRAGMA busy_timeout=5000")
        await db.commit()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                telegram_id INTEGER PRIMARY KEY,
                uuid TEXT NOT NULL,
                sub_token TEXT NOT NULL,
                created_at TEXT DEFAULT (datetime('now'))
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS access_requests (
                telegram_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                base_email TEXT NOT NULL,
                created_at TEXT DEFAULT (datetime('now'))
            )
            """
        )
        await _migrate_schema(db)
        await db.commit()
    logger.info("База данных готова: %s", DB_PATH)


async def count_device_slots(telegram_id: int, device_kind: str) -> int:
    """Сколько уже выданных конфигов этого типа устройства (для слота и суффикса email)."""
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT COUNT(*) FROM user_devices WHERE telegram_id = ? AND device_kind = ?",
            (telegram_id, device_kind),
        )
        row = await cur.fetchone()
    return int(row[0]) if row else 0


async def count_user_devices(telegram_id: int) -> int:
    """Сколько всего устройств (любых типов) у пользователя."""
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT COUNT(*) FROM user_devices WHERE telegram_id = ?",
            (telegram_id,),
        )
        row = await cur.fetchone()
    return int(row[0]) if row else 0


async def get_user_global_slot_index(
    telegram_id: int, device_kind: str, slot_index: int
) -> int | None:
    """Глобальный порядковый номер слота у пользователя (1, 2, 3, ...).

    Считается как «сколько у пользователя записей с id <= моего».
    Слоты 1, 11, 21, ... — «ведущие» (платные) в десятках по 10.
    """
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT id FROM user_devices "
            "WHERE telegram_id = ? AND device_kind = ? AND slot_index = ?",
            (telegram_id, device_kind, slot_index),
        )
        row = await cur.fetchone()
        if row is None:
            return None
        local_id = row[0]
        cur = await db.execute(
            "SELECT COUNT(*) FROM user_devices WHERE telegram_id = ? AND id <= ?",
            (telegram_id, local_id),
        )
        row = await cur.fetchone()
    return int(row[0]) if row else None


async def get_user_device_by_global_slot(
    telegram_id: int, global_slot: int
) -> UserDeviceRecord | None:
    """Возвращает user_device пользователя по его глобальному номеру слота."""
    all_devices = await list_user_devices(telegram_id)
    if not all_devices or global_slot < 1 or global_slot > len(all_devices):
        return None
    return all_devices[global_slot - 1]


async def list_user_devices_in_group(
    telegram_id: int, lead_global_slot: int, group_size: int = 10
) -> list[UserDeviceRecord]:
    """Все user_devices пользователя, чей глобальный слот в одной группе
    с lead_global_slot. Размер группы — group_size (по умолчанию 10),
    должен совпадать с GROUP_SIZE из main.py, иначе группы «протекают»
    в соседние. Например, для lead=1, group_size=10 вернёт слоты 1..10."""
    group_start = lead_global_slot
    group_end = lead_global_slot + group_size - 1
    all_devices = await list_user_devices(telegram_id)
    if not all_devices:
        return []
    if group_start > len(all_devices):
        return []
    upper = min(group_end, len(all_devices))
    return all_devices[group_start - 1 : upper]


async def create_user_device(
    telegram_id: int,
    device_kind: str,
    slot_index: int,
    base_email: str,
    uuid_val: str,
    sub_token: str,
    expiry_time_ms: int,
) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            INSERT INTO user_devices
            (telegram_id, device_kind, slot_index, base_email, uuid, sub_token, expiry_time_ms)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                telegram_id,
                device_kind,
                slot_index,
                base_email,
                uuid_val,
                sub_token,
                expiry_time_ms,
            ),
        )
        await db.commit()


async def list_user_devices(telegram_id: int) -> list[UserDeviceRecord]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            """
            SELECT telegram_id, device_kind, slot_index, base_email, uuid, sub_token,
                   expiry_time_ms, reminder_3d_sent_at, reminder_1d_sent_at,
                   expired_notified_at
            FROM user_devices WHERE telegram_id = ?
            ORDER BY id ASC
            """,
            (telegram_id,),
        )
        rows = await cur.fetchall()
    return [
        UserDeviceRecord(
            telegram_id=r["telegram_id"],
            device_kind=r["device_kind"],
            slot_index=r["slot_index"],
            base_email=r["base_email"],
            uuid=r["uuid"],
            sub_token=r["sub_token"],
            expiry_time_ms=r["expiry_time_ms"],
            reminder_3d_sent_at=r["reminder_3d_sent_at"],
            reminder_1d_sent_at=r["reminder_1d_sent_at"],
            expired_notified_at=r["expired_notified_at"],
        )
        for r in rows
    ]


async def list_all_user_devices() -> list[UserDeviceRecord]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            """
            SELECT telegram_id, device_kind, slot_index, base_email, uuid, sub_token,
                   expiry_time_ms, reminder_3d_sent_at, reminder_1d_sent_at,
                   expired_notified_at
            FROM user_devices
            ORDER BY id ASC
            """
        )
        rows = await cur.fetchall()
    return [
        UserDeviceRecord(
            telegram_id=r["telegram_id"],
            device_kind=r["device_kind"],
            slot_index=r["slot_index"],
            base_email=r["base_email"],
            uuid=r["uuid"],
            sub_token=r["sub_token"],
            expiry_time_ms=r["expiry_time_ms"],
            reminder_3d_sent_at=r["reminder_3d_sent_at"],
            reminder_1d_sent_at=r["reminder_1d_sent_at"],
            expired_notified_at=r["expired_notified_at"],
        )
        for r in rows
    ]


async def mark_subscription_notice_sent(
    telegram_id: int,
    device_kind: str,
    slot_index: int,
    stage: str,
) -> None:
    columns = {
        "3d": "reminder_3d_sent_at",
        "1d": "reminder_1d_sent_at",
        "expired": "expired_notified_at",
    }
    column = columns.get(stage)
    if column is None:
        raise ValueError(f"Unknown reminder stage: {stage}")
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            f"""
            UPDATE user_devices
            SET {column} = datetime('now')
            WHERE telegram_id = ? AND device_kind = ? AND slot_index = ? AND {column} IS NULL
            """,
            (telegram_id, device_kind, slot_index),
        )
        await db.commit()


async def count_distinct_subscribers() -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT COUNT(DISTINCT telegram_id) FROM user_devices")
        row = await cur.fetchone()
    return int(row[0]) if row else 0


async def list_users_legal_status() -> list[dict]:
    """Возвращает список всех пользователей с их статусом принятия правил и ником."""
    async with aiosqlite.connect(DB_PATH) as db:
        # Собираем всех, кто есть в user_devices или terms_acceptance
        # username берём из access_requests
        cur = await db.execute(
            """
            SELECT
                u.telegram_id,
                t.accepted_at,
                t.agreement_accepted_at,
                (SELECT COUNT(*) FROM user_devices WHERE telegram_id = u.telegram_id) as device_count,
                (SELECT username FROM access_requests WHERE telegram_id = u.telegram_id LIMIT 1) as username
            FROM (
                SELECT telegram_id FROM user_devices
                UNION
                SELECT telegram_id FROM terms_acceptance
            ) u
            LEFT JOIN terms_acceptance t ON u.telegram_id = t.telegram_id
            """
        )
        rows = await cur.fetchall()
        return [
            {
                "tid": r[0],
                "accepted": r[1] is not None and r[2] is not None,
                "devices": r[3],
                "username": r[4],
            }
            for r in rows
        ]


async def count_devices() -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT COUNT(*) FROM user_devices")
        row = await cur.fetchone()
    return int(row[0]) if row else 0


async def has_accepted_user_agreement(telegram_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            """
            SELECT 1 FROM terms_acceptance
            WHERE telegram_id = ? AND agreement_accepted_at IS NOT NULL
            """,
            (telegram_id,),
        )
        return await cur.fetchone() is not None


async def set_rules_accepted(telegram_id: int) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            INSERT INTO terms_acceptance
                (telegram_id, accepted_at, agreement_accepted_at)
            VALUES (?, datetime('now'), NULL)
            ON CONFLICT(telegram_id) DO UPDATE SET
                accepted_at = excluded.accepted_at
            """,
            (telegram_id,),
        )
        await db.commit()


async def set_agreement_accepted(telegram_id: int) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            UPDATE terms_acceptance
            SET agreement_accepted_at = datetime('now')
            WHERE telegram_id = ?
            """,
            (telegram_id,),
        )
        await db.commit()


async def get_user_device(
    telegram_id: int, device_kind: str, slot_index: int
) -> UserDeviceRecord | None:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            """
            SELECT telegram_id, device_kind, slot_index, base_email, uuid, sub_token,
                   expiry_time_ms, reminder_3d_sent_at, reminder_1d_sent_at,
                   expired_notified_at
            FROM user_devices
            WHERE telegram_id = ? AND device_kind = ? AND slot_index = ?
            """,
            (telegram_id, device_kind, slot_index),
        )
        row = await cur.fetchone()
    if row is None:
        return None
    return UserDeviceRecord(
        telegram_id=row["telegram_id"],
        device_kind=row["device_kind"],
        slot_index=row["slot_index"],
        base_email=row["base_email"],
        uuid=row["uuid"],
        sub_token=row["sub_token"],
        expiry_time_ms=row["expiry_time_ms"],
        reminder_3d_sent_at=row["reminder_3d_sent_at"],
        reminder_1d_sent_at=row["reminder_1d_sent_at"],
        expired_notified_at=row["expired_notified_at"],
    )


async def extend_device_expiry(
    telegram_id: int, device_kind: str, slot_index: int, new_expiry_time_ms: int
) -> None:
    """Обновляет expiry_time_ms и сбрасывает флаги напоминаний (чтобы перед следующим
    окончанием они снова сработали)."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            UPDATE user_devices
            SET expiry_time_ms = ?,
                reminder_3d_sent_at = NULL,
                reminder_1d_sent_at = NULL,
                expired_notified_at = NULL
            WHERE telegram_id = ? AND device_kind = ? AND slot_index = ?
            """,
            (new_expiry_time_ms, telegram_id, device_kind, slot_index),
        )
        await db.commit()


def _row_to_payment(row) -> PaymentRecord:
    return PaymentRecord(
        id=row["id"],
        telegram_id=row["telegram_id"],
        username=row["username"],
        first_name=row["first_name"],
        last_name=row["last_name"],
        kind=row["kind"],
        device_kind=row["device_kind"],
        slot_index=row["slot_index"],
        base_email=row["base_email"],
        plan_days=row["plan_days"],
        amount=row["amount"],
        yookassa_payment_id=row["yookassa_payment_id"],
        confirmation_url=row["confirmation_url"],
        status=row["status"],
        created_at=row["created_at"],
    )


async def create_pending_payment_with_key(
    *,
    idempotence_key: str,
    telegram_id: int,
    username: str | None,
    first_name: str | None,
    last_name: str | None,
    kind: str,
    device_kind: str,
    slot_index: int,
    base_email: str,
    plan_days: int,
    amount: int,
) -> PaymentRecord | None:
    """Создаёт pending-платёж с уникальным idempotence_key (C2: race-safe).

    Алгоритм:
      1. BEGIN IMMEDIATE — атомарно проверяем, что у пользователя нет pending.
      2. INSERT с уникальным idempotence_key. Если конфликт (другой запрос
         с тем же ключом уже создал запись) — возвращаем существующую.
      3. Если у пользователя УЖЕ есть pending (с другим ключом) — None.

    Возвращает:
      - новую запись со status='pending' и yookassa_payment_id=None;
      - существующую запись по idempotence_key (если дубликат);
      - None, если у пользователя есть активный pending с другим ключом.
    """
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        await db.execute("BEGIN IMMEDIATE")
        cur = await db.execute(
            "SELECT * FROM payments WHERE telegram_id=? AND status='pending'",
            (telegram_id,),
        )
        existing = await cur.fetchone()
        if existing is not None:
            await db.rollback()
            return _row_to_payment(existing)

        cur = await db.execute(
            """
            INSERT INTO payments (
                telegram_id, username, first_name, last_name, kind,
                device_kind, slot_index, base_email,
                plan_days, amount, idempotence_key,
                yookassa_payment_id, confirmation_url, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, 'pending')
            ON CONFLICT(idempotence_key) DO NOTHING
            """,
            (
                telegram_id,
                username,
                first_name,
                last_name,
                kind,
                device_kind,
                slot_index,
                base_email,
                plan_days,
                amount,
                idempotence_key,
            ),
        )
        if not cur.lastrowid:
            await db.rollback()
            cur = await db.execute(
                "SELECT * FROM payments WHERE idempotence_key=?",
                (idempotence_key,),
            )
            row = await cur.fetchone()
            if row is None:
                return None
            return _row_to_payment(row)
        new_id = cur.lastrowid
        await db.commit()
        cur = await db.execute("SELECT * FROM payments WHERE id=?", (new_id,))
        row = await cur.fetchone()
        if row is None:
            return None
        return _row_to_payment(row)


async def attach_yookassa_to_pending(
    *,
    idempotence_key: str,
    yookassa_payment_id: str,
    confirmation_url: str | None,
) -> bool:
    """Привязывает yookassa_payment_id к pending-записи по idempotence_key (C2).

    UPDATE срабатывает только если status всё ещё 'pending' — если параллельный
    webhook уже пометил succeeded, обновление не произойдёт, и вызвавший
    код получит False (нужно отдать существующий счёт, а не плодить второй).
    """
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            """
            UPDATE payments
            SET yookassa_payment_id=?, confirmation_url=?, updated_at=datetime('now')
            WHERE idempotence_key=? AND status='pending' AND yookassa_payment_id IS NULL
            """,
            (yookassa_payment_id, confirmation_url, idempotence_key),
        )
        await db.commit()
        return cur.rowcount > 0


async def get_payment_by_idempotence_key(
    idempotence_key: str,
) -> PaymentRecord | None:
    """Возвращает pending-запись по idempotence_key (для C2: после гонки)."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT * FROM payments WHERE idempotence_key=?",
            (idempotence_key,),
        )
        row = await cur.fetchone()
    if row is None:
        return None
    return _row_to_payment(row)


async def get_payment_by_yookassa_id(yookassa_payment_id: str) -> PaymentRecord | None:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT * FROM payments WHERE yookassa_payment_id = ?",
            (yookassa_payment_id,),
        )
        row = await cur.fetchone()
    if row is None:
        return None
    return _row_to_payment(row)


async def get_active_payment(telegram_id: int) -> PaymentRecord | None:
    """Возвращает текущий pending-платёж пользователя или None."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            """
            SELECT * FROM payments
            WHERE telegram_id = ? AND status = 'pending'
            ORDER BY id DESC LIMIT 1
            """,
            (telegram_id,),
        )
        row = await cur.fetchone()
    if row is None:
        return None
    return _row_to_payment(row)


async def mark_payment_paid(yookassa_payment_id: str) -> int:
    """Помечает платёж как succeeded. Возвращает rowcount.

    rowcount == 0 означает, что либо платёж не найден, либо он уже не pending
    (т.е. другой webhook уже обработал его). Используется для защиты от
    двойной выдачи подписки при дублированных webhook'ах (N2).
    """
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            """
            UPDATE payments
            SET status = 'succeeded', updated_at = datetime('now')
            WHERE yookassa_payment_id = ? AND status = 'pending'
            """,
            (yookassa_payment_id,),
        )
        await db.commit()
        return cur.rowcount


async def mark_payment_canceled(yookassa_payment_id: str) -> int:
    """Помечает платёж как canceled. Возвращает rowcount (см. mark_payment_paid)."""
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            """
            UPDATE payments
            SET status = 'canceled', updated_at = datetime('now')
            WHERE yookassa_payment_id = ? AND status = 'pending'
            """,
            (yookassa_payment_id,),
        )
        await db.commit()
        return cur.rowcount


async def expire_old_pending_payments(expires_seconds: int) -> list:
    """Помечает просроченные pending-платежи как canceled, возвращает их.

    Фоновая задача вызывает это раз в минуту, чтобы зависшие
    (юзер создал счёт и ушёл) не блокировали get_active_payment.

    C2: гонка с webhook. Если между SELECT и UPDATE webhook уже поставил
    'succeeded' — UPDATE с `WHERE status='pending'` эту строку не тронет.
    Запись останется succeeded, воркер увидит это через
    get_payment_by_yookassa_id и не будет отменять оплаченный счёт.
    """
    from datetime import datetime, timedelta, timezone

    cutoff = (datetime.now(timezone.utc) - timedelta(seconds=expires_seconds)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        await db.execute("BEGIN IMMEDIATE")
        cur = await db.execute(
            """
            SELECT * FROM payments
            WHERE status = 'pending' AND created_at < ?
            """,
            (cutoff,),
        )
        rows = await cur.fetchall()
        if rows:
            ids = [r["id"] for r in rows]
            placeholders = ",".join("?" * len(ids))
            await db.execute(
                f"""
                UPDATE payments
                SET status = 'canceled', updated_at = datetime('now')
                WHERE id IN ({placeholders}) AND status = 'pending'
                """,
                ids,
            )
        await db.commit()
    return [_row_to_payment(r) for r in rows]


async def count_pending_payments() -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT COUNT(*) FROM payments WHERE status = 'pending'")
        row = await cur.fetchone()
    return int(row[0]) if row else 0
