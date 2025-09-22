# bot/poller.py
import asyncio
import logging
from app import dp, bot
from logsetup import setup_logging

setup_logging()
log = logging.getLogger("poller")

async def main():
    log.info("Starting bot in long polling mode")
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
