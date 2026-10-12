import asyncio
import json
import logging
import os
import sys

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

# Ensure execution context is always the bot's root directory
BOT_DIR = os.path.dirname(os.path.abspath(__file__))
os.chdir(BOT_DIR)
if BOT_DIR not in sys.path:
    sys.path.insert(0, BOT_DIR)
from aiogram import Bot, Dispatcher, BaseMiddleware
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.types import ErrorEvent
from database import init_db, get_stale_high_priority_tickets
from handlers.admin import admin_main, admin_stats, admin_actions, admin_support, admin_packages
from handlers.user import user_main

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("durkpro")


class ThrottlingMiddleware(BaseMiddleware):
    def __init__(self, limit: float = 1.0):
        self.limit = limit
        self.last_seen = {}

    async def __call__(self, handler, event, data):
        uid = event.from_user.id
        now = asyncio.get_event_loop().time()
        if uid in self.last_seen and now - self.last_seen[uid] < self.limit:
            return
        self.last_seen[uid] = now
        return await handler(event, data)


async def sla_monitor(bot: Bot, admin_id: int):
    """Alerts the admin if a paid high-priority ticket has been open for 5+ hours."""
    while True:
        await asyncio.sleep(1800)
        try:
            for ticket_id, user_id, created_at in await get_stale_high_priority_tickets(hours=5):
                await bot.send_message(
                    admin_id,
                    f"🚨 SLA BREACH: High priority ticket <code>{ticket_id}</code> "
                    f"(user <code>{user_id}</code>) has been open since {created_at} — over 5 hours.",
                    parse_mode=ParseMode.HTML,
                )
        except Exception:
            logger.exception("SLA monitor error")


async def main():
    with open("config.json", "r", encoding="utf-8") as f:
        config = json.load(f)

    bot = Bot(token=config["BOT_TOKEN"], default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher()

    dp.message.middleware(ThrottlingMiddleware())
    dp.callback_query.middleware(ThrottlingMiddleware())

    @dp.error()
    async def global_error_handler(event: ErrorEvent):
        """Catches any exception an update handler raises so one bad update
        (missing file, Telegram API hiccup, network blip) can't take the bot
        down or leave the user hanging with no reply at all."""
        logger.exception("Unhandled error while processing update", exc_info=event.exception)
        try:
            update = event.update
            chat = None
            if update.message:
                chat = update.message.chat
            elif update.callback_query and update.callback_query.message:
                chat = update.callback_query.message.chat
            if chat:
                await bot.send_message(chat.id, "⚠️ Something went wrong. Please try again in a moment.")
        except Exception:
            logger.exception("Failed to notify user about the earlier error")
        try:
            await bot.send_message(config["ADMIN_ID"], f"🚨 Bot error: <code>{event.exception}</code>")
        except Exception:
            logger.exception("Failed to notify admin about the earlier error")
        return True

    await init_db()

    dp.include_router(admin_main.router)
    dp.include_router(admin_packages.router)
    dp.include_router(admin_stats.router)
    dp.include_router(admin_actions.router)
    dp.include_router(admin_support.router)
    dp.include_router(user_main.router)

    asyncio.create_task(sla_monitor(bot, config["ADMIN_ID"]))

    try:
        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(bot)
    finally:
        await bot.session.close()

if __name__ == "__main__":
    asyncio.run(main())
