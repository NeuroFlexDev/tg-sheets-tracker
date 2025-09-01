# bot/poller.py
import asyncio
import logging
from logsetup import setup_logging
from app import dp, bot, schedule_jobs

setup_logging()
log = logging.getLogger("poller")

async def main():
    await bot.delete_webhook(drop_pending_updates=True)
    schedule_jobs()
    log.info("poller_started")
    try:
        await dp.start_polling(bot)
    finally:
        await bot.session.close()  # корректно закрываем клиент

if __name__ == "__main__":
    asyncio.run(main())
