import os

from pathlib import Path
from dotenv import load_dotenv
load_dotenv(Path(__file__).parent / ".env")


TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = int(os.getenv("TELEGRAM_CHAT_ID", "0"))
GOOGLE_SA_JSON_PATH = os.getenv("GOOGLE_SA_JSON_PATH", "sa.json")
GSHEET_NAME = os.getenv("GSHEET_NAME", "Tasks")
GSHEET_WORKSHEET = os.getenv("GSHEET_WORKSHEET", "tasks")
GSHEET_THREADS_SHEET = os.getenv("GSHEET_THREADS_SHEET", "threads")
GSHEET_ID = os.getenv("GSHEET_ID", "")
WEBHOOK_BASE = os.getenv("WEBHOOK_BASE")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")
PORT = int(os.getenv("PORT", "8080"))
TZ = os.getenv("TZ", "Europe/Moscow")
DAILY_SUMMARY_HHMM = os.getenv("DAILY_SUMMARY_HHMM", "09:00")
OVERDUE_REMINDER_HHMM = os.getenv("OVERDUE_REMINDER_HHMM", "18:00")
SUMMARY_LABEL = os.getenv("SUMMARY_LABEL", "summary")
