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
class RenewalRequestRecord:
    telegram_id: int
    username: str | None
    first_name: str | None
    last_name: str | None
    device_kind: str
    slot_index: int
    current_expiry_time_ms: int | None


@dataclass(frozen=True)
class PendingAccessRequestRecord:
    telegram_id: int
    username: str | None
    first_name: str | None
    last_name: str | None
    base_email: str
    device_kind: str
    slot_index: int
    created_at: str | None


@dataclass(frozen=True)
class PendingRenewalRequestRecord:
    telegram_id: int
    username: str | None
    first_name: str | None
    last_name: str | None
    device_kind: str
    slot_index: int
    current_expiry_time_ms: int | None
    created_at: str | None


@dataclass(frozen=True)
class AccessRequestRecord:
    telegram_id: int
    username: str | None
    first_name: str | None
    last_name: str | None
    base_email: str
    device_kind: str
    slot_index: int


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
        await conn.execute(
            "ALTER TABLE user_devices ADD COLUMN expiry_time_ms INTEGER"
        )
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
    cur = await conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='users'")
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


async def init_db() -> None:
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
        cur = await db.execute(
            "SELECT COUNT(DISTINCT telegram_id) FROM user_devices"
        )
        row = await cur.fetchone()
    return int(row[0]) if row else 0


async def list_users_legal_status() -> list[dict]:
    """Возвращает список всех пользователей с их статусом принятия правил и ником."""
    async with aiosqlite.connect(DB_PATH) as db:
        # Собираем всех, кто есть в user_devices или terms_acceptance
        # Пытаемся достать username из renewal_requests или access_requests
        cur = await db.execute(
            """
            SELECT 
                u.telegram_id, 
                t.accepted_at, 
                t.agreement_accepted_at,
                (SELECT COUNT(*) FROM user_devices WHERE telegram_id = u.telegram_id) as device_count,
                COALESCE(
                    (SELECT username FROM renewal_requests WHERE telegram_id = u.telegram_id LIMIT 1),
                    (SELECT username FROM access_requests WHERE telegram_id = u.telegram_id LIMIT 1)
                ) as username
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


async def count_pending_requests() -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT COUNT(*) FROM access_requests")
        row = await cur.fetchone()
    return int(row[0]) if row else 0


async def get_access_request(telegram_id: int) -> AccessRequestRecord | None:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            """
            SELECT telegram_id, username, first_name, last_name, base_email,
                   device_kind, slot_index
            FROM access_requests WHERE telegram_id = ?
            """,
            (telegram_id,),
        )
        row = await cur.fetchone()
    if row is None:
        return None
    return AccessRequestRecord(
        telegram_id=row["telegram_id"],
        username=row["username"],
        first_name=row["first_name"],
        last_name=row["last_name"],
        base_email=row["base_email"],
        device_kind=row["device_kind"] or "other",
        slot_index=int(row["slot_index"] or 1),
    )


async def list_pending_access_requests() -> list[PendingAccessRequestRecord]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            """
            SELECT telegram_id, username, first_name, last_name, base_email,
                   device_kind, slot_index, created_at
            FROM access_requests
            ORDER BY created_at ASC, telegram_id ASC
            """
        )
        rows = await cur.fetchall()
    return [
        PendingAccessRequestRecord(
            telegram_id=r["telegram_id"],
            username=r["username"],
            first_name=r["first_name"],
            last_name=r["last_name"],
            base_email=r["base_email"],
            device_kind=r["device_kind"] or "other",
            slot_index=int(r["slot_index"] or 1),
            created_at=r["created_at"],
        )
        for r in rows
    ]


async def try_insert_access_request(
    telegram_id: int,
    username: str | None,
    first_name: str | None,
    last_name: str | None,
    base_email: str,
    device_kind: str,
    slot_index: int,
) -> bool:
    """True — новая заявка. False — уже есть активная заявка от этого пользователя."""
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT 1 FROM access_requests WHERE telegram_id = ?",
            (telegram_id,),
        )
        if await cur.fetchone():
            return False
        await db.execute(
            """
            INSERT INTO access_requests
            (telegram_id, username, first_name, last_name, base_email, device_kind, slot_index)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                telegram_id,
                username,
                first_name,
                last_name,
                base_email,
                device_kind,
                slot_index,
            ),
        )
        await db.commit()
    return True


async def delete_access_request(telegram_id: int) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "DELETE FROM access_requests WHERE telegram_id = ?",
            (telegram_id,),
        )
        await db.commit()


async def has_accepted_usage_rules(telegram_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            """
            SELECT 1 FROM terms_acceptance
            WHERE telegram_id = ? AND accepted_at IS NOT NULL
            """,
            (telegram_id,),
        )
        return await cur.fetchone() is not None


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


async def reset_all_legal_acceptances() -> None:
    """Сбрасывает согласие с правилами для всех пользователей."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE terms_acceptance SET agreement_accepted_at = NULL, accepted_at = NULL"
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


async def try_insert_renewal_request(
    telegram_id: int,
    device_kind: str,
    slot_index: int,
    username: str | None,
    first_name: str | None,
    last_name: str | None,
) -> bool:
    """True — новая заявка на продление. False — уже есть активная."""
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            """
            SELECT 1 FROM renewal_requests
            WHERE telegram_id = ? AND device_kind = ? AND slot_index = ?
            """,
            (telegram_id, device_kind, slot_index),
        )
        if await cur.fetchone():
            return False
        await db.execute(
            """
            INSERT INTO renewal_requests
                (telegram_id, device_kind, slot_index, username, first_name, last_name)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (telegram_id, device_kind, slot_index, username, first_name, last_name),
        )
        await db.commit()
    return True


async def get_renewal_request(
    telegram_id: int, device_kind: str, slot_index: int
) -> RenewalRequestRecord | None:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            """
            SELECT r.telegram_id, r.username, r.first_name, r.last_name,
                   r.device_kind, r.slot_index, d.expiry_time_ms
            FROM renewal_requests r
            LEFT JOIN user_devices d
              ON d.telegram_id = r.telegram_id
             AND d.device_kind = r.device_kind
             AND d.slot_index = r.slot_index
            WHERE r.telegram_id = ? AND r.device_kind = ? AND r.slot_index = ?
            """,
            (telegram_id, device_kind, slot_index),
        )
        row = await cur.fetchone()
    if row is None:
        return None
    return RenewalRequestRecord(
        telegram_id=row["telegram_id"],
        username=row["username"],
        first_name=row["first_name"],
        last_name=row["last_name"],
        device_kind=row["device_kind"],
        slot_index=row["slot_index"],
        current_expiry_time_ms=row["expiry_time_ms"],
    )


async def list_pending_renewal_requests() -> list[PendingRenewalRequestRecord]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            """
            SELECT r.telegram_id, r.username, r.first_name, r.last_name,
                   r.device_kind, r.slot_index, r.created_at, d.expiry_time_ms
            FROM renewal_requests r
            LEFT JOIN user_devices d
              ON d.telegram_id = r.telegram_id
             AND d.device_kind = r.device_kind
             AND d.slot_index = r.slot_index
            ORDER BY r.created_at ASC, r.telegram_id ASC
            """
        )
        rows = await cur.fetchall()
    return [
        PendingRenewalRequestRecord(
            telegram_id=r["telegram_id"],
            username=r["username"],
            first_name=r["first_name"],
            last_name=r["last_name"],
            device_kind=r["device_kind"],
            slot_index=r["slot_index"],
            current_expiry_time_ms=r["expiry_time_ms"],
            created_at=r["created_at"],
        )
        for r in rows
    ]


async def delete_renewal_request(
    telegram_id: int, device_kind: str, slot_index: int
) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            DELETE FROM renewal_requests
            WHERE telegram_id = ? AND device_kind = ? AND slot_index = ?
            """,
            (telegram_id, device_kind, slot_index),
        )
        await db.commit()


async def count_pending_renewals() -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT COUNT(*) FROM renewal_requests")
        row = await cur.fetchone()
    return int(row[0]) if row else 0
