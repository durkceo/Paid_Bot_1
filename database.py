import os
import aiosqlite
from contextlib import asynccontextmanager
from datetime import datetime

DB_DIR = os.path.dirname(os.path.abspath(__file__))
DB_NAME = os.path.join(DB_DIR, "durkpro.db")

@asynccontextmanager
async def _connect():
    """Opens a connection tuned for high-volume concurrent traffic:
    - WAL mode: Readers never block writers, writers never block readers.
    - busy_timeout=30000: Waits up to 30 seconds instead of 'database is locked'.
    - synchronous=NORMAL: Maximizes write performance safely in WAL mode.
    - cache_size=-64000: 64MB memory page cache.
    - temp_store=MEMORY: In-memory temporary tables and sorts.
    - mmap_size=268435456: Memory-mapped I/O (256MB) for instant reads.
    """
    db = await aiosqlite.connect(DB_NAME)
    try:
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("PRAGMA synchronous=NORMAL")
        await db.execute("PRAGMA busy_timeout=30000")
        await db.execute("PRAGMA cache_size=-64000")
        await db.execute("PRAGMA temp_store=MEMORY")
        await db.execute("PRAGMA mmap_size=268435456")
        yield db
    finally:
        await db.close()

async def init_db():
    async with _connect() as db:
        await db.execute("CREATE TABLE IF NOT EXISTS users (user_id INTEGER PRIMARY KEY, first_name TEXT, username TEXT, is_blocked INTEGER DEFAULT 0, joined_date TEXT, key_attempts INTEGER DEFAULT 0)")
        await db.execute("CREATE TABLE IF NOT EXISTS purchases (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, package_id TEXT, voucher_code TEXT UNIQUE, amount REAL, method TEXT, purchase_date TEXT)")
        await db.execute("CREATE TABLE IF NOT EXISTS tickets (id TEXT PRIMARY KEY, user_id INTEGER, subject TEXT, status TEXT, created_at TEXT)")
        await db.execute("CREATE TABLE IF NOT EXISTS pending_payments (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, package_id TEXT, code TEXT, status TEXT DEFAULT 'PENDING', created_at TEXT)")
        # Older DBs created before key_attempts existed won't get the column from CREATE TABLE IF NOT EXISTS.
        try:
            await db.execute("ALTER TABLE users ADD COLUMN key_attempts INTEGER DEFAULT 0")
        except Exception:
            pass

        # High-concurrency performance indexes
        await db.execute("CREATE INDEX IF NOT EXISTS idx_users_joined_date ON users (joined_date)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_users_is_blocked ON users (is_blocked)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_purchases_user_id ON purchases (user_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_purchases_purchase_date ON purchases (purchase_date)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_tickets_user_status ON tickets (user_id, status)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_tickets_status ON tickets (status)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_tickets_created_at ON tickets (created_at)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_pending_payments_user_id ON pending_payments (user_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_pending_payments_status ON pending_payments (status)")
        await db.commit()

async def get_admin_stats() -> dict:
    today = datetime.now().strftime("%Y-%m-%d")
    async with _connect() as db:
        async with db.execute("SELECT COUNT(*) FROM users") as cursor:
            total_users = (await cursor.fetchone())[0]
        async with db.execute("SELECT COUNT(DISTINCT user_id) FROM purchases") as cursor:
            paid_users = (await cursor.fetchone())[0]
        async with db.execute("SELECT COUNT(*) FROM purchases WHERE purchase_date LIKE ?", (f"{today}%",)) as cursor:
            today_total = (await cursor.fetchone())[0]
        async with db.execute("SELECT COUNT(*) FROM purchases WHERE purchase_date LIKE ? AND method = 'API'", (f"{today}%",)) as cursor:
            today_api = (await cursor.fetchone())[0]

    api_rate = round((today_api / today_total * 100)) if today_total > 0 else 100
    return {"total_users": total_users, "paid_users": paid_users, "api_rate": api_rate}

async def get_sales_stats() -> dict:
    today = datetime.now().strftime("%Y-%m-%d")
    async with _connect() as db:
        async with db.execute("SELECT COUNT(*) FROM users WHERE joined_date LIKE ?", (f"{today}%",)) as cursor:
            today_users = (await cursor.fetchone())[0]
        async with db.execute("SELECT COUNT(*) FROM users") as cursor:
            monthly_users = (await cursor.fetchone())[0]
        async with db.execute("SELECT SUM(amount) FROM purchases WHERE purchase_date LIKE ?", (f"{today}%",)) as cursor:
            today_sell = (await cursor.fetchone())[0] or 0
        async with db.execute("SELECT SUM(amount) FROM purchases") as cursor:
            monthly_sell = (await cursor.fetchone())[0] or 0
        async with db.execute("SELECT SUM(amount) FROM purchases") as cursor:
            total_earning = (await cursor.fetchone())[0] or 0

    return {"today_users": today_users, "monthly_users": monthly_users, "today_sell": today_sell, "weekly_sell": today_sell, "monthly_sell": monthly_sell, "total_earning": total_earning}

async def get_all_users() -> list:
    async with _connect() as db:
        async with db.execute("SELECT user_id FROM users") as cursor:
            return [row[0] for row in await cursor.fetchall()]

async def get_recent_users(limit: int = 10) -> list:
    async with _connect() as db:
        async with db.execute(
            "SELECT user_id, first_name, username, joined_date FROM users ORDER BY joined_date DESC LIMIT ?",
            (limit,),
        ) as cursor:
            return await cursor.fetchall()

async def get_banned_users() -> list:
    async with _connect() as db:
        async with db.execute("SELECT user_id, first_name FROM users WHERE is_blocked = 1 LIMIT 10") as cursor:
            return await cursor.fetchall()

async def unblock_user(user_id: int) -> bool:
    async with _connect() as db:
        await db.execute("UPDATE users SET is_blocked = 0 WHERE user_id = ?", (user_id,))
        await db.commit()
        return db.total_changes > 0

async def get_active_tickets() -> list:
    async with _connect() as db:
        async with db.execute("SELECT id, user_id, subject FROM tickets WHERE status = 'OPEN' LIMIT 10") as cursor:
            return await cursor.fetchall()

async def get_ticket_user(ticket_id: str):
    async with _connect() as db:
        async with db.execute("SELECT user_id FROM tickets WHERE id = ?", (ticket_id,)) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else None

async def close_ticket(ticket_id: str):
    async with _connect() as db:
        await db.execute("UPDATE tickets SET status = 'CLOSED' WHERE id = ?", (ticket_id,))
        await db.commit()

async def add_user(user_id: int, first_name: str, username: str):
    date_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    async with _connect() as db:
        await db.execute(
            "INSERT OR IGNORE INTO users (user_id, first_name, username, joined_date) VALUES (?, ?, ?, ?)",
            (user_id, first_name, username, date_now),
        )
        await db.commit()

async def get_user(user_id: int):
    async with _connect() as db:
        async with db.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)) as cursor:
            return await cursor.fetchone()

async def is_user_blocked(user_id: int) -> bool:
    async with _connect() as db:
        async with db.execute("SELECT is_blocked FROM users WHERE user_id = ?", (user_id,)) as cursor:
            row = await cursor.fetchone()
            return bool(row and row[0])

async def add_purchase(user_id: int, package_id: str, voucher_code: str, amount: float, method: str):
    date_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    async with _connect() as db:
        await db.execute(
            "INSERT INTO purchases (user_id, package_id, voucher_code, amount, method, purchase_date) VALUES (?, ?, ?, ?, ?, ?)",
            (user_id, package_id, voucher_code, amount, method, date_now),
        )
        await db.commit()

async def get_user_purchases(user_id: int) -> list:
    async with _connect() as db:
        async with db.execute(
            "SELECT package_id, amount, method, purchase_date FROM purchases WHERE user_id = ? ORDER BY id DESC",
            (user_id,),
        ) as cursor:
            return await cursor.fetchall()

async def create_ticket(ticket_id: str, user_id: int, subject: str):
    date_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    async with _connect() as db:
        await db.execute(
            "INSERT INTO tickets (id, user_id, subject, status, created_at) VALUES (?, ?, ?, 'OPEN', ?)",
            (ticket_id, user_id, subject, date_now),
        )
        await db.commit()

async def get_active_ticket_for_user(user_id: int):
    async with _connect() as db:
        async with db.execute(
            "SELECT id FROM tickets WHERE user_id = ? AND status = 'OPEN'", (user_id,)
        ) as cursor:
            return await cursor.fetchone()

async def get_stale_high_priority_tickets(hours: int = 5) -> list:
    async with _connect() as db:
        async with db.execute(
            "SELECT id, user_id, created_at FROM tickets "
            "WHERE status = 'OPEN' AND subject LIKE '[HIGH PRIORITY]%' "
            "AND (julianday('now') - julianday(created_at)) * 24 >= ?",
            (hours,),
        ) as cursor:
            return await cursor.fetchall()

async def is_voucher_used(code: str) -> bool:
    async with _connect() as db:
        async with db.execute("SELECT 1 FROM purchases WHERE voucher_code = ?", (code,)) as cursor:
            return (await cursor.fetchone()) is not None

async def increment_key_attempts(user_id: int) -> int:
    async with _connect() as db:
        await db.execute("UPDATE users SET key_attempts = key_attempts + 1 WHERE user_id = ?", (user_id,))
        await db.commit()
        async with db.execute("SELECT key_attempts FROM users WHERE user_id = ?", (user_id,)) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else 0

async def reset_key_attempts(user_id: int):
    async with _connect() as db:
        await db.execute("UPDATE users SET key_attempts = 0 WHERE user_id = ?", (user_id,))
        await db.commit()

async def block_user(user_id: int):
    async with _connect() as db:
        await db.execute("UPDATE users SET is_blocked = 1 WHERE user_id = ?", (user_id,))
        await db.commit()

async def add_pending_payment(user_id: int, package_id: str, code: str) -> int:
    date_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    async with _connect() as db:
        cursor = await db.execute(
            "INSERT INTO pending_payments (user_id, package_id, code, status, created_at) VALUES (?, ?, ?, 'PENDING', ?)",
            (user_id, package_id, code, date_now),
        )
        await db.commit()
        return cursor.lastrowid

async def get_pending_payment(payment_id: int):
    async with _connect() as db:
        async with db.execute(
            "SELECT id, user_id, package_id, code, status FROM pending_payments WHERE id = ?", (payment_id,)
        ) as cursor:
            return await cursor.fetchone()

async def set_pending_payment_status(payment_id: int, status: str):
    async with _connect() as db:
        await db.execute("UPDATE pending_payments SET status = ? WHERE id = ?", (status, payment_id))
        await db.commit()
