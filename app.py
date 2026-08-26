# -*- coding: utf-8 -*-
"""
Агент-помощник для контроля диспетчеров.

Что делает этот сервис:
1. Принимает вебхуки от CallRail (звонки: отвечен/пропущен, кто ответил, длительность).
2. Принимает вебхуки от CallRail по SMS (входящие/исходящие сообщения).
3. Записывает всё в Google Sheets — это ваша база данных и одновременно
   таблица, которую вы можете открыть в любой момент и посмотреть глазами.
4. Шлёт мгновенные алерты в ваш личный Telegram, если:
   - звонок пропущен и никто не перезвонил в течение N минут
   - на SMS клиента никто не ответил в течение N минут
5. По расписанию (через внешний cron) присылает ежедневную сводку:
   кто сколько звонков принял, сколько назначил встреч, сколько пропустил.

Как это работает "под капотом":
CallRail -> вебхук -> этот сервер -> Google Sheets (запись) + Telegram (алерт, если нужно)
Ваш Telegram-бот -> вебхук -> этот сервер -> Google Sheets (запись ответа диспетчера)
Внешний cron (cron-job.org, бесплатно) раз в день дёргает /daily-summary,
раз в 15 минут дёргает /check-pending — это и создаёт "фоновый мониторинг",
без необходимости держать отдельный процесс.

ВАЖНО ДЛЯ ТОГО, КТО БУДЕТ ПОДКЛЮЧАТЬ:
Точные названия полей в вебхуке CallRail (answered, agent_email, customer_phone_number
и т.д.) взяты из официальной документации CallRail API v3. Если в вашем аккаунте
включены кастомные поля вебхука — их нужно свериться в настройках CallRail
(Settings -> Integrations -> Webhooks) и поправить строки, помеченные # СВЕРИТЬ.
"""

import os
import hmac
import hashlib
import datetime
from flask import Flask, request, jsonify
import requests
import gspread
from google.oauth2.service_account import Credentials

app = Flask(__name__)

# ---------- НАСТРОЙКИ (берутся из переменных окружения — см. .env.example) ----------
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
OWNER_CHAT_ID = os.environ.get("OWNER_CHAT_ID")  # ваш личный chat_id, куда шлются алерты владельцу
CALLRAIL_WEBHOOK_SECRET = os.environ.get("CALLRAIL_WEBHOOK_SECRET")  # из настроек CallRail
GOOGLE_SHEET_ID = os.environ.get("GOOGLE_SHEET_ID")
GOOGLE_CREDENTIALS_FILE = os.environ.get("GOOGLE_CREDENTIALS_FILE", "google_credentials.json")
HOUSECALLPRO_WEBHOOK_SECRET = os.environ.get("HOUSECALLPRO_WEBHOOK_SECRET")  # если HCP поддерживает подпись, см. README

MISSED_CALL_ALERT_MINUTES = int(os.environ.get("MISSED_CALL_ALERT_MINUTES", "15"))
SMS_NO_RESPONSE_ALERT_MINUTES = int(os.environ.get("SMS_NO_RESPONSE_ALERT_MINUTES", "20"))

# Соответствие email диспетчера в CallRail -> человеческое имя (для читаемых отчётов)
# СВЕРИТЬ: впишите сюда реальные email и имена ваших диспетчеров
DISPATCHER_NAMES = {
    "dispatcher1@example.com": "Диспетчер 1",
    "dispatcher2@example.com": "Диспетчер 2",
}

# ---------- Google Sheets ----------
def get_sheet():
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(GOOGLE_CREDENTIALS_FILE, scopes=scopes)
    client = gspread.authorize(creds)
    return client.open_by_key(GOOGLE_SHEET_ID)


def ensure_worksheets(sh):
    """Создаёт листы 'Calls' и 'Messages' с заголовками, если их ещё нет."""
    existing = [ws.title for ws in sh.worksheets()]
    if "Calls" not in existing:
        ws = sh.add_worksheet(title="Calls", rows=2000, cols=10)
        ws.append_row([
            "call_id", "timestamp", "direction", "customer_name", "customer_phone",
            "agent_email", "answered", "duration_sec", "voicemail", "alert_sent"
        ])
    if "Messages" not in existing:
        ws = sh.add_worksheet(title="Messages", rows=2000, cols=8)
        ws.append_row([
            "message_id", "timestamp", "direction", "from", "to",
            "agent_email", "responded", "alert_sent"
        ])
    if "Appointments" not in existing:
        ws = sh.add_worksheet(title="Appointments", rows=2000, cols=8)
        ws.append_row([
            "job_id", "timestamp_created", "scheduled_start", "customer_name",
            "dispatcher", "event_type", "job_status"
        ])


# ---------- Telegram ----------
def send_telegram(chat_id, text):
    if not TELEGRAM_BOT_TOKEN or not chat_id:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        requests.post(url, json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"}, timeout=10)
    except requests.RequestException as e:
        print("Ошибка отправки в Telegram:", e)


# ---------- Проверка подлинности вебхука CallRail ----------
def verify_callrail_signature(request_obj):
    """CallRail подписывает тело запроса HMAC-SHA256. Если секрет не задан — пропускаем проверку (для теста)."""
    if not CALLRAIL_WEBHOOK_SECRET:
        return True
    signature = request_obj.headers.get("X-CallRail-Signature", "")
    expected = hmac.new(
        CALLRAIL_WEBHOOK_SECRET.encode(), request_obj.get_data(), hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(signature, expected)


# ---------- Роут 1: вебхук звонков от CallRail ----------
@app.route("/webhook/callrail/call", methods=["POST"])
def callrail_call_webhook():
    if not verify_callrail_signature(request):
        return jsonify({"error": "неверная подпись"}), 403

    data = request.get_json(force=True, silent=True) or {}

    call_id = data.get("id")                                   # СВЕРИТЬ
    answered = data.get("answered", False)                     # СВЕРИТЬ
    direction = data.get("direction", "inbound")                # СВЕРИТЬ
    customer_name = data.get("customer_name", "")                # СВЕРИТЬ
    customer_phone = data.get("customer_phone_number", "")       # СВЕРИТЬ
    agent_email = data.get("agent_email", "")                    # СВЕРИТЬ
    duration = data.get("duration", 0) or 0
    voicemail = data.get("voicemail", False)
    start_time = data.get("start_time", datetime.datetime.utcnow().isoformat())

    sh = get_sheet()
    ensure_worksheets(sh)
    ws = sh.worksheet("Calls")
    alert_sent = False

    if not answered:
        dispatcher_name = DISPATCHER_NAMES.get(agent_email, agent_email or "неизвестно")
        text = (
            f"⚠️ Пропущенный звонок\n"
            f"От: {customer_name or customer_phone}\n"
            f"Телефон: {customer_phone}\n"
            f"Время: {start_time}\n"
            f"Нужно перезвонить в течение {MISSED_CALL_ALERT_MINUTES} мин."
        )
        send_telegram(OWNER_CHAT_ID, text)
        alert_sent = True

    ws.append_row([
        call_id, start_time, direction, customer_name, customer_phone,
        agent_email, str(answered), duration, str(voicemail), str(alert_sent)
    ])

    return jsonify({"status": "ok"}), 200


# ---------- Роут 2: вебхук SMS от CallRail ----------
@app.route("/webhook/callrail/sms", methods=["POST"])
def callrail_sms_webhook():
    if not verify_callrail_signature(request):
        return jsonify({"error": "неверная подпись"}), 403

    data = request.get_json(force=True, silent=True) or {}

    message_id = data.get("id")                       # СВЕРИТЬ
    direction = data.get("direction", "inbound")        # СВЕРИТЬ
    from_number = data.get("customer_phone_number", "")  # СВЕРИТЬ
    to_number = data.get("tracking_phone_number", "")    # СВЕРИТЬ
    agent_email = data.get("agent_email", "")
    timestamp = data.get("created_at", datetime.datetime.utcnow().isoformat())  # СВЕРИТЬ

    sh = get_sheet()
    ensure_worksheets(sh)
    ws = sh.worksheet("Messages")

    responded = direction == "outbound"  # исходящее сообщение = ответ диспетчера

    ws.append_row([
        message_id, timestamp, direction, from_number, to_number,
        agent_email, str(responded), "False"
    ])

    return jsonify({"status": "ok"}), 200


# ---------- Роут: вебхук Housecall Pro (назначенные встречи) ----------
# Подключается в Housecall Pro: My Apps -> App Store -> Webhooks (доступно на тарифе MAX).
# Подписаться нужно на события: job.appointment.scheduled, job.appointment.rescheduled
@app.route("/webhook/housecallpro/appointment", methods=["POST"])
def housecallpro_appointment_webhook():
    data = request.get_json(force=True, silent=True) or {}

    # Housecall Pro оборачивает данные в разные форматы в зависимости от события —
    # СВЕРИТЬ реальную структуру через "Send test event" в настройках вебхука HCP
    # и поправить пути ниже при необходимости.
    event_type = data.get("event", "job.appointment.scheduled")
    job = data.get("job", data)  # иногда сама job лежит в корне, иногда во вложенном ключе

    job_id = job.get("id", "")
    customer_name = ""
    customer = job.get("customer", {})
    if isinstance(customer, dict):
        customer_name = (customer.get("first_name", "") + " " + customer.get("last_name", "")).strip()

    scheduled_start = job.get("schedule", {}).get("scheduled_start", "") if isinstance(job.get("schedule"), dict) else ""
    job_status = job.get("work_status", "")

    # Диспетчер = тот, кто назначил встречу. В HCP это обычно "assigned_employees" или "dispatcher"
    dispatcher_raw = ""
    assigned = job.get("assigned_employees", [])
    if isinstance(assigned, list) and assigned:
        first = assigned[0]
        dispatcher_raw = first.get("email", first.get("first_name", "")) if isinstance(first, dict) else str(first)

    dispatcher_name = DISPATCHER_NAMES.get(dispatcher_raw, dispatcher_raw or "не указан")

    sh = get_sheet()
    ensure_worksheets(sh)
    ws = sh.worksheet("Appointments")
    ws.append_row([
        job_id, datetime.datetime.utcnow().isoformat(), scheduled_start,
        customer_name, dispatcher_name, event_type, job_status
    ])

    return jsonify({"status": "ok"}), 200


# ---------- Роут 5: сравнение диспетчеров — кто активен, кто отвечает, кто "пропадает" ----------
# Можно вызывать вручную (открыть ссылку в браузере) или добавить в cron
# рядом с /daily-summary. По умолчанию считает за сегодня; ?period=week — за 7 дней.
NO_ACTIVITY_ALERT_HOURS = int(os.environ.get("NO_ACTIVITY_ALERT_HOURS", "2"))
WORKDAY_START_HOUR = int(os.environ.get("WORKDAY_START_HOUR", "8"))   # начало рабочего дня, UTC
WORKDAY_END_HOUR = int(os.environ.get("WORKDAY_END_HOUR", "22"))      # конец рабочего дня, UTC


@app.route("/performance-report", methods=["POST", "GET"])
def performance_report():
    period = request.args.get("period", "today")
    sh = get_sheet()
    ensure_worksheets(sh)
    now = datetime.datetime.utcnow()

    if period == "week":
        cutoff = now - datetime.timedelta(days=7)
        label = "за последние 7 дней"
    else:
        cutoff = now.replace(hour=0, minute=0, second=0, microsecond=0)
        label = "за сегодня"

    def in_period(ts_str):
        try:
            ts = datetime.datetime.fromisoformat(str(ts_str).replace("Z", ""))
        except (ValueError, TypeError):
            return False
        return ts >= cutoff

    calls = [c for c in sh.worksheet("Calls").get_all_records() if in_period(c["timestamp"])]
    messages = [m for m in sh.worksheet("Messages").get_all_records() if in_period(m["timestamp"])]
    appointments = [a for a in sh.worksheet("Appointments").get_all_records() if in_period(a["timestamp_created"])]

    agents = {}

    def get_agent(name):
        agents.setdefault(name, {
            "calls_total": 0, "calls_answered": 0,
            "sms_total": 0, "sms_answered": 0,
            "appointments": 0, "last_activity": None,
        })
        return agents[name]

    for c in calls:
        name = DISPATCHER_NAMES.get(c["agent_email"], c["agent_email"] or "не назначен")
        a = get_agent(name)
        a["calls_total"] += 1
        if str(c["answered"]) == "True":
            a["calls_answered"] += 1
        _update_last_activity(a, c["timestamp"])

    for m in messages:
        # считаем только исходящие (ответы диспетчера) как "активность"
        name = DISPATCHER_NAMES.get(m["agent_email"], m["agent_email"] or "не назначен")
        a = get_agent(name)
        if m["direction"] == "inbound":
            a["sms_total"] += 1
            if str(m["responded"]) == "True":
                a["sms_answered"] += 1
        _update_last_activity(a, m["timestamp"])

    for ap in appointments:
        name = ap["dispatcher"] or "не указан"
        a = get_agent(name)
        a["appointments"] += 1
        _update_last_activity(a, ap["timestamp_created"])

    # Считаем проценты и статус "на месте / нет активности"
    rows = []
    is_workday_now = WORKDAY_START_HOUR <= now.hour < WORKDAY_END_HOUR
    for name, a in agents.items():
        answer_rate = round(100 * a["calls_answered"] / a["calls_total"]) if a["calls_total"] else None
        sms_rate = round(100 * a["sms_answered"] / a["sms_total"]) if a["sms_total"] else None
        idle_hours = None
        status = "нет данных"
        if a["last_activity"]:
            idle_hours = round((now - a["last_activity"]).total_seconds() / 3600, 1)
            if is_workday_now and idle_hours >= NO_ACTIVITY_ALERT_HOURS:
                status = f"⚠️ нет активности {idle_hours} ч."
            else:
                status = "на месте"
        rows.append({
            "name": name, "answer_rate": answer_rate, "sms_rate": sms_rate,
            "calls_total": a["calls_total"], "sms_total": a["sms_total"],
            "appointments": a["appointments"], "idle_hours": idle_hours, "status": status,
        })

    # Сортируем по проценту отвеченных звонков — от худшего к лучшему,
    # чтобы проблемные сразу были видны сверху
    rows.sort(key=lambda r: (r["answer_rate"] is None, r["answer_rate"]))

    lines = [f"📈 Сравнение диспетчеров {label}\n"]
    for r in rows:
        ar = f"{r['answer_rate']}%" if r["answer_rate"] is not None else "—"
        sr = f"{r['sms_rate']}%" if r["sms_rate"] is not None else "—"
        lines.append(
            f"— {r['name']}: звонки {ar} ({r['calls_total']}), "
            f"SMS {sr} ({r['sms_total']}), встреч {r['appointments']} — {r['status']}"
        )
    if not rows:
        lines.append("Данных пока нет.")

    send_telegram(OWNER_CHAT_ID, "\n".join(lines))
    return jsonify({"status": "ok", "report": rows}), 200


def _update_last_activity(agent_dict, ts_str):
    try:
        ts = datetime.datetime.fromisoformat(str(ts_str).replace("Z", ""))
    except (ValueError, TypeError):
        return
    if agent_dict["last_activity"] is None or ts > agent_dict["last_activity"]:
        agent_dict["last_activity"] = ts


# ---------- Роут 3: проверка "зависших" звонков/сообщений без ответа ----------
# Вызывается внешним планировщиком (cron-job.org) каждые 15 минут
@app.route("/check-pending", methods=["POST", "GET"])
def check_pending():
    sh = get_sheet()
    ensure_worksheets(sh)
    now = datetime.datetime.utcnow()
    alerts_sent = 0

    ws = sh.worksheet("Messages")
    records = ws.get_all_records()
    for idx, row in enumerate(records, start=2):  # строка 1 — заголовки
        if row["direction"] == "inbound" and str(row["responded"]) == "False" and str(row["alert_sent"]) == "False":
            try:
                msg_time = datetime.datetime.fromisoformat(str(row["timestamp"]).replace("Z", ""))
            except ValueError:
                continue
            minutes_passed = (now - msg_time).total_seconds() / 60
            if minutes_passed >= SMS_NO_RESPONSE_ALERT_MINUTES:
                text = (
                    f"⚠️ Нет ответа на SMS клиента\n"
                    f"От: {row['from']}\n"
                    f"Прошло: {int(minutes_passed)} мин.\n"
                )
                send_telegram(OWNER_CHAT_ID, text)
                ws.update_cell(idx, 8, "True")  # колонка alert_sent
                alerts_sent += 1

    return jsonify({"status": "ok", "alerts_sent": alerts_sent}), 200


# ---------- Роут 4: ежедневная сводка ----------
# Вызывается внешним планировщиком раз в день (например, в 20:00)
@app.route("/daily-summary", methods=["POST", "GET"])
def daily_summary():
    sh = get_sheet()
    ensure_worksheets(sh)
    today = datetime.datetime.utcnow().date().isoformat()

    calls = sh.worksheet("Calls").get_all_records()
    messages = sh.worksheet("Messages").get_all_records()
    appointments = sh.worksheet("Appointments").get_all_records()

    today_calls = [c for c in calls if str(c["timestamp"]).startswith(today)]
    today_messages = [m for m in messages if str(m["timestamp"]).startswith(today)]
    today_appointments = [a for a in appointments if str(a["timestamp_created"]).startswith(today)]

    stats = {}
    for c in today_calls:
        agent = DISPATCHER_NAMES.get(c["agent_email"], c["agent_email"] or "не назначен")
        stats.setdefault(agent, {"answered": 0, "missed": 0, "sms_answered": 0, "sms_missed": 0, "appointments": 0})
        if str(c["answered"]) == "True":
            stats[agent]["answered"] += 1
        else:
            stats[agent]["missed"] += 1

    for m in today_messages:
        agent = DISPATCHER_NAMES.get(m["agent_email"], m["agent_email"] or "не назначен")
        stats.setdefault(agent, {"answered": 0, "missed": 0, "sms_answered": 0, "sms_missed": 0, "appointments": 0})
        if str(m["responded"]) == "True":
            stats[agent]["sms_answered"] += 1
        else:
            stats[agent]["sms_missed"] += 1

    for a in today_appointments:
        agent = a["dispatcher"] or "не указан"
        stats.setdefault(agent, {"answered": 0, "missed": 0, "sms_answered": 0, "sms_missed": 0, "appointments": 0})
        stats[agent]["appointments"] += 1

    lines = [f"📊 Сводка за {today}\n"]
    for agent, s in stats.items():
        lines.append(
            f"— {agent}: звонков принято {s['answered']}, пропущено {s['missed']}, "
            f"SMS отвечено {s['sms_answered']}, без ответа {s['sms_missed']}, "
            f"встреч назначено {s['appointments']}"
        )
    if not stats:
        lines.append("Сегодня данных пока нет.")

    send_telegram(OWNER_CHAT_ID, "\n".join(lines))
    return jsonify({"status": "ok", "stats": stats}), 200


@app.route("/", methods=["GET"])
def health():
    return jsonify({"status": "агент-помощник работает"}), 200


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
