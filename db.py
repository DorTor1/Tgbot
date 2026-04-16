"""SQLite: устройства пользователей, заявки на доступ."""

import aiosqlite
import logging
from dataclasses import dataclass
from pathlib import Path

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
) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            INSERT INTO user_devices
            (telegram_id, device_kind, slot_index, base_email, uuid, sub_token)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (telegram_id, device_kind, slot_index, base_email, uuid_val, sub_token),
        )
        await db.commit()


async def list_user_devices(telegram_id: int) -> list[UserDeviceRecord]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            """
            SELECT telegram_id, device_kind, slot_index, base_email, uuid, sub_token
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
        )
        for r in rows
    ]


async def count_distinct_subscribers() -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT COUNT(DISTINCT telegram_id) FROM user_devices"
        )
        row = await cur.fetchone()
    return int(row[0]) if row else 0


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
