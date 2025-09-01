# bot/app.py
import asyncio
import logging
import time
import uuid
from fastapi import FastAPI, Request, Response, Header
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message, Update
from aiogram.client.default import DefaultBotProperties
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from datetime import datetime, date

from dateutil import parser as dtparser

import pytz

import config
import sheets
from parser import parse_freeform
from logsetup import setup_logging

from functools import wraps

setup_logging()
log = logging.getLogger("app")

bot = Bot(
    token=config.TELEGRAM_BOT_TOKEN,
    default=DefaultBotProperties(parse_mode="HTML")
)
dp = Dispatcher()
app = FastAPI()
scheduler = AsyncIOScheduler(timezone=config.TZ)

# -------------- Helpers --------------

def _now_tz():
    return datetime.now(pytz.timezone(config.TZ))

def _parse_hhmm(s: str):
    hh, mm = s.split(":")
    return int(hh), int(mm)

async def _send_chat(text: str, thread_label: str | None = None, fallback_thread_id: int | None = None, **ctx):
    thread_id = None
    if thread_label:
        thread_id = sheets.get_thread_id(thread_label)
    if thread_id is None:
        thread_id = fallback_thread_id
    log.info("send_message", extra={"thread_label": thread_label, "thread_id": thread_id, **ctx})
    await bot.send_message(chat_id=config.TELEGRAM_CHAT_ID, text=text, message_thread_id=thread_id)

def _overdue_tasks():
    tasks = sheets.list_tasks(status=None)
    res = []
    for r in tasks:
        if r.get("Status") == "done":
            continue
        due = r.get("Due")
        if due:
            try:
                d = date.fromisoformat(due)
                if d < _now_tz().date():
                    res.append(r)
            except Exception:
                log.exception("parse_due_failed", extra={"due": due, "task": r})
    return res

def with_timing(fn_name: str):
    def deco(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            t0 = time.perf_counter()
            try:
                # aiogram может передавать служебные kwargs (dispatcher, event, state и т.д.)
                # Наши хэндлеры принимают только (m: Message), поэтому НЕ передаём kwargs.
                return await func(*args)
            finally:
                dt = (time.perf_counter() - t0) * 1000
                log.info("handler_timing", extra={"handler": fn_name, "ms": round(dt, 2)})
        return wrapper
    return deco

def _to_aware(dt_naive, tzname: str):
    tz = pytz.timezone(tzname)
    return tz.localize(dt_naive) if dt_naive.tzinfo is None else dt_naive.astimezone(tz)

def parse_when(text: str, tzname: str) -> datetime:
    """
    Поддерживаем форматы:
    - Абсолютно: "2025-09-05 14:30", "2025-09-05", "2025-09-05T14:30"
    - Относительно: "+30m", "+2h", "+1d"
    - Русские краткие: "завтра 10:00", "сегодня 18:00"
    Возвращает aware-datetime в TZ.
    """
    text = text.strip().lower()

    # относительные
    if text.startswith("+"):
        num = int("".join(ch for ch in text if ch.isdigit()))
        if text.endswith("m"):
            dt = _now_tz() + timedelta(minutes=num)
        elif text.endswith("h") or text.endswith("ч"):
            dt = _now_tz() + timedelta(hours=num)
        elif text.endswith("d") or text.endswith("д"):
            dt = _now_tz() + timedelta(days=num)
        else:
            raise ValueError("Неподдержанный относительный формат (+30m|+2h|+1d)")
        return dt

    if text.startswith("завтра"):
        parts = text.split()
        base = _now_tz().date() + timedelta(days=1)
        if len(parts) > 1:
            tm = dtparser.parse(parts[1]).time()
            return _to_aware(datetime.combine(base, tm), config.TZ)
        return _to_aware(datetime.combine(base, datetime.min.time().replace(hour=9)), config.TZ)

    if text.startswith("сегодня"):
        parts = text.split()
        base = _now_tz().date()
        if len(parts) > 1:
            tm = dtparser.parse(parts[1]).time()
            return _to_aware(datetime.combine(base, tm), config.TZ)
        return _to_aware(datetime.combine(base, datetime.min.time().replace(hour=9)), config.TZ)

    # абсолютные
    dt = dtparser.parse(text, dayfirst=False)
    if dt.tzinfo is None:
        dt = _to_aware(dt, config.TZ)
    else:
        dt = dt.astimezone(pytz.timezone(config.TZ))
    return dt


# -------------- Middlewares-like simple wrapper --------------

async def _log_update(update: Update):
    # Корреляционный ID на апдейт
    cid = str(uuid.uuid4())[:8]
    u = update.model_dump(exclude_none=True)
    chat_id = None
    user_id = None
    text = None
    try:
        if "message" in u:
            msg = u["message"]
            chat_id = msg["chat"]["id"]
            user_id = msg["from"]["id"]
            text = msg.get("text")
        elif "edited_message" in u:
            msg = u["edited_message"]
            chat_id = msg["chat"]["id"]
            user_id = msg["from"]["id"]
            text = msg.get("text")
    except Exception:
        pass
    log.info("update_in", extra={"cid": cid, "chat_id": chat_id, "user_id": user_id, "text": text})
    return cid

# -------------- Commands --------------

@dp.message(Command("start", "help"))
@with_timing("help")
async def cmd_help(m: Message):
    log.info("cmd_help", extra={"from": m.from_user.id, "chat": m.chat.id})
    await m.reply(
        """
<b>Команды</b>:
/add &lt;текст&gt; — создать задачу (P0..P3, @assignee, до YYYY-MM-DD, #labels)
/list [status|@assignee|#label] — список задач
/done &lt;ID&gt; — закрыть задачу
/assign &lt;ID&gt; @user — назначить
/due &lt;ID&gt; YYYY-MM-DD — срок
/who — кто назначен на открытые задачи
/bind #label — привязать текущий тред к лейблу
/summary — сводка за сегодня

/remind <ID> <когда> — напомнить (поддержка: +30m, +2h, +1d, "сегодня 18:00", "завтра 10:00", "YYYY-MM-DD HH:MM")
/remind list <ID> — список напоминаний для задачи
/remind cancel <ID> — удалить все напоминания по задаче

        """.strip()
    )

@dp.message(Command("bind"))
@with_timing("bind")
async def cmd_bind(m: Message):
    if not m.message_thread_id:
        log.warning("bind_no_thread", extra={"from": m.from_user.id, "chat": m.chat.id})
        return await m.reply("Команда должна выполняться внутри треда (форум-топика).")
    text = m.text.partition(" ")[2].strip()
    if not text.startswith("#"):
        return await m.reply("Пример: /bind #frontend")
    label = text.lstrip("#").strip()
    sheets.bind_thread(label, m.message_thread_id)
    log.info("bind_ok", extra={"label": label, "thread_id": m.message_thread_id})
    await m.reply(f"✅ Тред привязан к лейблу <b>#{label}</b>")

@dp.message(Command("add"))
@with_timing("add")
async def cmd_add(m: Message):
    text = m.text.partition(" ")[2].strip()
    if not text:
        return await m.reply("Пример: /add Починить деплой P1 @vadim до 2025-09-05 #deploy #frontend")
    title, priority, assignee, due, labels, project = parse_freeform(text)
    thread_id = m.message_thread_id or (sheets.get_thread_id(project) if project else None)
    log.info("add_parsed", extra={
        "title": title, "priority": priority, "assignee": assignee, "due": due,
        "labels": labels, "project": project, "thread_id": thread_id
    })
    t = sheets.create_task(
        title=title, description="", assignee=assignee, priority=priority,
        due=due, labels=labels, source="tg", tg_thread_id=thread_id,
        tg_message_link=f"https://t.me/c/{str(m.chat.id).replace('-100','')}/{m.message_id}"
    )
    await bot.send_message(
        chat_id=config.TELEGRAM_CHAT_ID,
        message_thread_id=thread_id,
        text=(f"✅ <b>Создано</b>: {t.title}\n"
              f"ID: <code>{t.id}</code> | {t.priority} | {assignee or '—'}{f' | до {due}' if due else ''}\n"
              f"Labels: {', '.join(labels) if labels else '—'}")
    )
    log.info("add_created", extra={"task_id": t.id})

@dp.message(Command("list"))
@with_timing("list")
async def cmd_list(m: Message):
    arg = m.text.partition(" ")[2].strip()
    status = arg if arg in {"open","in_progress","done","blocked"} else None
    assignee = arg if arg.startswith('@') else None
    label = arg.lstrip("#") if arg.startswith("#") else None
    tasks = sheets.list_tasks(status=status, assignee=assignee, label=label)
    log.info("list_query", extra={"status": status, "assignee": assignee, "label": label, "count": len(tasks)})
    if not tasks:
        return await m.reply("Пусто")
    lines = []
    for r in tasks[:50]:
        lines.append(f"<code>{r['ID']}</code> • <b>{r['Title']}</b> • {r['Priority']} • {r.get('Assignee','') or '—'}"
                     + (f" • до {r['Due']}" if r.get('Due') else "")
                     + (f" • #{(r.get('Labels') or '').split(',')[0]}" if r.get('Labels') else ""))
    await m.reply("\n".join(lines))

@dp.message(Command("done"))
@with_timing("done")
async def cmd_done(m: Message):
    tid = m.text.partition(" ")[2].strip()
    if not tid:
        return await m.reply("Укажи ID: /done 1a2b3c4d")
    ok = sheets.update_status(tid, "done")
    log.info("done_update", extra={"task_id": tid, "ok": ok})
    await m.reply("✅ Готово" if ok else "❌ Не найдено")

@dp.message(Command("assign"))
@with_timing("assign")
async def cmd_assign(m: Message):
    _, _, rest = m.text.partition(" ")
    tid, _, user = rest.strip().partition(" ")
    if not tid or not user.startswith('@'):
        return await m.reply("Пример: /assign 1a2b3c4d @vadim")
    ok = sheets.assign_task(tid, user)
    log.info("assign_update", extra={"task_id": tid, "assignee": user, "ok": ok})
    await m.reply("✅ Назначено" if ok else "❌ Не найдено")

@dp.message(Command("due"))
@with_timing("due")
async def cmd_due(m: Message):
    _, _, rest = m.text.partition(" ")
    tid, _, due = rest.strip().partition(" ")
    if not tid or not due:
        return await m.reply("Пример: /due 1a2b3c4d 2025-09-10")
    ok = sheets.set_due(tid, due)
    log.info("due_update", extra={"task_id": tid, "due": due, "ok": ok})
    await m.reply("✅ Срок обновлён" if ok else "❌ Не найдено")

@dp.message(Command("remind"))
@with_timing("remind")
async def cmd_remind(m: Message):
    """
    /remind <ID> <когда>
    Примеры:
      /remind 1a2b3c4d +30m
      /remind 1a2b3c4d завтра 10:00
      /remind 1a2b3c4d 2025-09-05 14:30
      /remind list 1a2b3c4d
      /remind cancel 1a2b3c4d
    """
    _, _, rest = m.text.partition(" ")
    rest = rest.strip()
    if not rest:
        return await m.reply("Формат: /remind <ID> <когда> | /remind list <ID> | /remind cancel <ID>")

    # list/cancel
    if rest.startswith("list"):
        _, _, tid = rest.partition(" ")
        tid = tid.strip()
        if not tid:
            return await m.reply("Пример: /remind list 1a2b3c4d")
        rows = sheets.list_reminders(task_id=tid)
        if not rows:
            return await m.reply("Напоминаний нет.")
        lines = [f"⏰ <code>{r['ID']}</code> • {r['WhenISO']} • thread={r.get('ThreadID') or '—'}" for r in rows]
        return await m.reply("\n".join(lines))

    if rest.startswith("cancel"):
        _, _, tid = rest.partition(" ")
        tid = tid.strip()
        if not tid:
            return await m.reply("Пример: /remind cancel 1a2b3c4d")
        cnt = sheets.remove_reminders_by_task(tid)
        return await m.reply(f"🧹 Удалено напоминаний: {cnt}")

    # add
    tid, _, when_txt = rest.partition(" ")
    tid, when_txt = tid.strip(), when_txt.strip()
    if not tid or not when_txt:
        return await m.reply("Пример: /remind 1a2b3c4d +30m")

    try:
        when_dt = parse_when(when_txt, config.TZ)
    except Exception as e:
        return await m.reply(f"Не понял время: {when_txt}\nПоддержка: +30m, +2h, +1d, 'сегодня 18:00', 'завтра 10:00', 'YYYY-MM-DD HH:MM'")

    when_iso = when_dt.isoformat()
    rid = sheets.add_reminder(
        task_id=tid,
        when_iso=when_iso,
        chat_id=m.chat.id,
        thread_id=m.message_thread_id,
        created_by=(m.from_user.username and "@"+m.from_user.username) or str(m.from_user.id),
    )
    log.info("remind_set", extra={"rid": rid, "task_id": tid, "when": when_iso, "chat": m.chat.id, "thread": m.message_thread_id})
    await m.reply(f"⏰ Ок, напомню по <code>{tid}</code> в {when_dt.strftime('%Y-%m-%d %H:%M %Z')}")


@dp.message(Command("who"))
@with_timing("who")
async def cmd_who(m: Message):
    tasks = sheets.list_tasks(status="open")
    buckets = {}
    for r in tasks:
        buckets.setdefault(r.get("Assignee") or "(не назначено)", 0)
        buckets[r.get("Assignee") or "(не назначено)"] += 1
    log.info("who_counts", extra={"buckets": buckets})
    text = "\n".join(f"<b>{k}</b>: {v}" for k,v in buckets.items()) or "Пусто"
    await m.reply(text)

@dp.message(Command("summary"))
@with_timing("summary_manual")
async def cmd_summary(m: Message):
    await send_daily_summary()

# -------------- Schedules --------------

@with_timing("send_daily_summary")
async def send_daily_summary():
    open_tasks = sheets.list_tasks(status=None)
    by_assignee = {}
    overdue = 0
    for r in open_tasks:
        if r.get("Status") == "done":
            continue
        k = r.get("Assignee") or "(не назначено)"
        by_assignee[k] = by_assignee.get(k, 0) + 1
        d = r.get("Due")
        if d:
            try:
                if date.fromisoformat(d) < _now_tz().date():
                    overdue += 1
            except Exception:
                log.exception("summary_due_parse_fail", extra={"due": d, "task": r})
    log.info("daily_summary_stats", extra={"by_assignee": by_assignee, "overdue": overdue})
    lines = ["<b>Ежедневная сводка задач</b>"]
    for k,v in sorted(by_assignee.items(), key=lambda x: (-x[1], x[0].lower())):
        lines.append(f"{k}: {v}")
    lines.append(f"Просрочено: <b>{overdue}</b>")
    await _send_chat("\n".join(lines), thread_label=config.SUMMARY_LABEL)

@with_timing("send_overdue_reminders")
async def send_overdue_reminders():
    od = _overdue_tasks()
    if not od:
        log.info("overdue_none")
        return
    lines = ["<b>Просроченные задачи</b>"]
    for r in od[:50]:
        first_label = (r.get("Labels") or "").split(",")[0].strip() if r.get("Labels") else None
        lines.append(f"• <code>{r['ID']}</code> <b>{r['Title']}</b> — {r.get('Assignee') or '—'} (до {r.get('Due')})"
                     + (f" • #{first_label}" if first_label else ""))
    log.info("overdue_count", extra={"count": len(od)})
    await _send_chat("\n".join(lines), thread_label=config.SUMMARY_LABEL)

def schedule_jobs():
    hh, mm = _parse_hhmm(config.DAILY_SUMMARY_HHMM)
    scheduler.add_job(send_daily_summary, "cron", hour=hh, minute=mm, id="daily_summary", replace_existing=True)
    scheduler.add_job(tick_reminders, "interval", minutes=1, id="tick_reminders", replace_existing=True)


    hh2, mm2 = _parse_hhmm(config.OVERDUE_REMINDER_HHMM)
    scheduler.add_job(send_overdue_reminders, "cron", hour=hh2, minute=mm2, id="overdue_reminders", replace_existing=True)
    scheduler.start()
    log.info("scheduler_started", extra={
        "daily": config.DAILY_SUMMARY_HHMM,
        "overdue": config.OVERDUE_REMINDER_HHMM,
        "tz": config.TZ
    })

@with_timing("tick_reminders")
async def tick_reminders():
    now_iso = _now_tz().isoformat()
    due = sheets.due_reminders(now_iso)
    if not due:
        return
    for r in due:
        thread_id = None
        try:
            thread_id = int(str(r.get("ThreadID") or "").strip()) if r.get("ThreadID") else None
        except Exception:
            thread_id = None
        text = (f"⏰ <b>Напоминание по задаче</b>\n"
                f"ID: <code>{r['TaskID']}</code>\n"
                f"Время: {r['WhenISO']}")
        await _send_chat(text, fallback_thread_id=thread_id, reminder_id=r.get("ID"), task_id=r.get("TaskID"))
        # удалить записанное напоминание
        sheets.remove_reminder(r["ID"])


# -------------- Webhook plumbing --------------

@app.on_event("startup")
async def on_startup():
    await bot.delete_webhook(drop_pending_updates=True)
    await bot.set_webhook(url=f"{config.WEBHOOK_BASE}/tg/webhook", secret_token=config.WEBHOOK_SECRET)
    schedule_jobs()
    log.info("webhook_set", extra={"url": f"{config.WEBHOOK_BASE}/tg/webhook"})

@app.get("/health")
async def health():
    return {"ok": True, "time": _now_tz().isoformat()}

@app.post("/tg/webhook")
async def tg_webhook(request: Request, x_telegram_bot_api_secret_token: str = Header(None)):
    if config.WEBHOOK_SECRET and x_telegram_bot_api_secret_token != config.WEBHOOK_SECRET:
        log.warning("webhook_unauthorized")
        return Response(status_code=401)
    data = await request.json()
    upd = Update.model_validate(data)
    cid = await _log_update(upd)
    try:
        await dp.feed_update(bot, upd)
    except Exception:
        log.exception("update_failed", extra={"cid": cid})
        return Response(status_code=500)
    return Response(status_code=200)
