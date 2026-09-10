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
from zoneinfo import ZoneInfo
from flask import Flask, request, jsonify
import requests
import gspread
from google.oauth2.service_account import Credentials

TORONTO_TZ = ZoneInfo("America/Toronto")


def now_local():
    """Текущее время в часовом поясе Торонто (наивное, без tzinfo,
    чтобы не смешивать offset-aware и offset-naive даты при сравнении)."""
    return datetime.datetime.now(TORONTO_TZ).replace(tzinfo=None)

app = Flask(__name__)


def safe_get_json(request_obj):
    """Надёжно разбирает тело запроса. CallRail присылает данные в формате
    form-data (например: answered=false&callercity=...), а не JSON — поэтому
    сначала проверяем form-data, и только если её нет, пробуем JSON.
    Если это тестовый пинг ({"foo":"bar"} или пусто) — просто вернёт то,
    что есть, без падения с ошибкой."""
    try:
        if request_obj.form:
            return request_obj.form.to_dict()
        raw = request_obj.get_data(as_text=True) or ""
        if not raw.strip():
            return {}
        import json as _json
        return _json.loads(raw)
    except Exception:
        return {}

# ---------- НАСТРОЙКИ (берутся из переменных окружения — см. .env.example) ----------
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
OWNER_CHAT_ID = os.environ.get("OWNER_CHAT_ID")  # ваш личный chat_id, куда шлются алерты владельцу
CALLRAIL_WEBHOOK_SECRET = os.environ.get("CALLRAIL_WEBHOOK_SECRET")  # из настроек CallRail
GOOGLE_SHEET_ID = os.environ.get("GOOGLE_SHEET_ID")
GOOGLE_CREDENTIALS_FILE = os.environ.get("GOOGLE_CREDENTIALS_FILE", "google_credentials.json")
HOUSECALLPRO_WEBHOOK_SECRET = os.environ.get("HOUSECALLPRO_WEBHOOK_SECRET")  # если HCP поддерживает подпись, см. README

MISSED_CALL_ALERT_MINUTES = int(os.environ.get("MISSED_CALL_ALERT_MINUTES", "15"))
SMS_NO_RESPONSE_ALERT_MINUTES = int(os.environ.get("SMS_NO_RESPONSE_ALERT_MINUTES", "20"))

# Соответствие email диспетчера в CallRail -> человеческое имя (запасной вариант,
# используется только если не удалось определить по номеру телефона ниже)
DISPATCHER_NAMES = {
    "dispatcher1@example.com": "Диспетчер 1",
    "dispatcher2@example.com": "Диспетчер 2",
}

# Основной способ определения диспетчера — по его личному добавочному номеру.
# Каждый диспетчер отвечает на своей отдельной линии, поэтому номер, на который
# пришёл звонок (destinationnum), точно указывает, кто должен был ответить —
# это работает даже для ПРОПУЩЕННЫХ звонков, в отличие от email.
DISPATCHER_PHONES = {
    "4374196514": "Nancy",
    "6472373651": "Daniela",
    "6476007710": "Ubaldo",
    "3435710971": "Natasha",
    "6452315632": "Ana Luisa",
    "2368011270": "Sarah",
}

# Набор имён настоящих диспетчеров — используется, чтобы отфильтровать техников
# и любые посторонние имена из отчётов (Housecall Pro присылает и тех, и других
# через одно поле dispatched_employees)
KNOWN_DISPATCHERS = set(DISPATCHER_PHONES.values())


def _normalize_phone(raw):
    """Приводит номер к 10 цифрам без кода страны и разделителей, чтобы сравнивать
    номера в разных форматах (+1 437-419-6514, 14374196514, (437) 419-6514 и т.д.)."""
    if not raw:
        return ""
    digits = "".join(ch for ch in str(raw) if ch.isdigit())
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    return digits


def _resolve_dispatcher_by_phone(data):
    """Ищет диспетчера по номеру среди нескольких возможных полей CallRail.
    Для входящих звонков используется destinationnum (номер, на который позвонил клиент).
    Для исходящих (перезвон) — trackingnum или callernum, в зависимости от направления."""
    for field in ("destinationnum", "trackingnum", "callernum"):
        phone = _normalize_phone(data.get(field, ""))
        if phone in DISPATCHER_PHONES:
            return DISPATCHER_PHONES[phone]
    return None


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
        ws = sh.add_worksheet(title="Calls", rows=2000, cols=9)
        ws.append_row([
            "Timestamp", "Dispatcher", "Customer Name", "Phone", "Direction",
            "Status", "Duration", "Follow-up Needed", "Last Alert Time"
        ])
    if "Messages" not in existing:
        ws = sh.add_worksheet(title="Messages", rows=2000, cols=9)
        ws.append_row([
            "message_id", "timestamp", "direction", "from", "to",
            "agent_email", "responded", "resolved", "last_alert_time"
        ])
    if "Appointments" not in existing:
        ws = sh.add_worksheet(title="Appointments", rows=2000, cols=9)
        ws.append_row([
            "job_id", "timestamp_created", "scheduled_start", "customer_name", "customer_phone",
            "dispatcher", "technician", "event_type", "job_status"
        ])
    if "Summary" not in existing:
        ws = sh.add_worksheet(title="Summary", rows=200, cols=9)
        ws.append_row([
            "Dispatcher", "Total Calls", "Answered", "Missed/Voicemail",
            "Answer Rate", "Called Back Successfully", "Still Not Reached",
            "Appointments Booked", "Appointments Requested (pending)"
        ])
    if "Daily History" not in existing:
        ws = sh.add_worksheet(title="Daily History", rows=2000, cols=7)
        ws.append_row([
            "Date", "Dispatcher", "Calls Answered", "Calls Missed",
            "SMS Answered", "Appointments Booked", "Appointments Requested (pending)"
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
def _extract_call_fields(data):
    """CallRail присылает данные в формате form-data. Реальные названия полей
    (проверено по логам): answered, callername, callernum, customer_phone_number,
    destinationnum, duration, datetime, employee_email / agent_email — могут
    отличаться в зависимости от настроек аккаунта, поэтому пробуем несколько
    вариантов названий."""
    answered_raw = str(data.get("answered", "false")).strip().lower()
    answered = answered_raw in ("true", "1", "yes")

    customer_name = data.get("callername") or data.get("customer_name", "")
    customer_phone = (
        data.get("customer_phone_number")
        or data.get("callernum")
        or data.get("caller_id", "")
    )
    agent_email = (
        data.get("agent_email")
        or data.get("employee_email")
        or data.get("answered_by_email", "")
    )
    duration = data.get("duration", 0) or 0
    voicemail = str(data.get("voicemail", "false")).lower() in ("true", "1", "yes")
    call_id = data.get("id") or data.get("call_id", "")

    # Определяем диспетчера по его личному номеру — это надёжнее email,
    # так как работает даже для пропущенных звонков (см. DISPATCHER_PHONES выше)
    dispatcher_by_phone = _resolve_dispatcher_by_phone(data)

    # CallRail присылает поле "datetime" в UTC независимо от настроек аккаунта.
    # Переводим его в местное время (Торонто), чтобы всё в таблице было в одном поясе.
    raw_start_time = data.get("datetime") or data.get("start_time")
    if raw_start_time:
        try:
            utc_dt = datetime.datetime.fromisoformat(str(raw_start_time).replace("Z", "")).replace(tzinfo=ZoneInfo("UTC"))
            start_time = utc_dt.astimezone(TORONTO_TZ).replace(tzinfo=None).isoformat(sep=" ")
        except (ValueError, TypeError):
            start_time = raw_start_time
    else:
        start_time = now_local().isoformat()

    return {
        "call_id": call_id, "answered": answered, "customer_name": customer_name,
        "customer_phone": customer_phone, "agent_email": agent_email,
        "dispatcher_by_phone": dispatcher_by_phone,
        "duration": duration, "voicemail": voicemail, "start_time": start_time,
    }


def _format_duration(seconds):
    """Переводит секунды в формат минута:секунда, например 225 -> '3:45'."""
    try:
        total = int(seconds)
    except (ValueError, TypeError):
        return "0:00"
    minutes, secs = divmod(total, 60)
    return f"{minutes}:{secs:02d}"


def resolve_dispatcher_name(f):
    """Единая функция определения читаемого имени диспетчера:
    сначала пробуем по номеру телефона (надёжно, работает даже для пропущенных),
    и только если не нашли — пробуем по email (может быть общим на всех)."""
    if f.get("dispatcher_by_phone"):
        return f["dispatcher_by_phone"]
    return DISPATCHER_NAMES.get(f["agent_email"], f["agent_email"] or "unassigned")


# ---------- Роут 1а: вебхук ВХОДЯЩИХ звонков от CallRail (Post-Call) ----------
@app.route("/webhook/callrail/call", methods=["POST"])
def callrail_call_webhook():
    if not verify_callrail_signature(request):
        return jsonify({"error": "неверная подпись"}), 403

    data = safe_get_json(request)
    f = _extract_call_fields(data)
    if not f["call_id"] and not f["customer_phone"]:
        return jsonify({"status": "ok", "note": "тестовый запрос принят"}), 200

    sh = get_sheet()
    ensure_worksheets(sh)
    ws = sh.worksheet("Calls")

    # Считаем звонок "требующим перезвона" если либо не ответили вообще,
    # либо ответил автоответчик/голосовая почта (клиент не поговорил с живым человеком)
    needs_followup = not f["answered"] or f["voicemail"]

    if needs_followup:
        dispatcher_name = resolve_dispatcher_name(f)
        if f["voicemail"]:
            text = (
                f"📩 Customer left a voicemail\n"
                f"From: {f['customer_name'] or f['customer_phone']}\n"
                f"Phone: {f['customer_phone']}\n"
                f"Time: {f['start_time']}\n"
                f"The dispatcher needs to listen and call back. "
                f"If not reached, I'll remind again in {MISSED_CALL_ALERT_MINUTES} min."
            )
        else:
            text = (
                f"⚠️ Missed call\n"
                f"From: {f['customer_name'] or f['customer_phone']}\n"
                f"Phone: {f['customer_phone']}\n"
                f"Time: {f['start_time']}\n"
                f"The dispatcher needs to call back and reach the customer. "
                f"If not reached, I'll remind again in {MISSED_CALL_ALERT_MINUTES} min."
            )
        send_telegram(OWNER_CHAT_ID, text)
        status = "Voicemail" if f["voicemail"] else "Missed"
        ws.append_row([
            f["start_time"], dispatcher_name, f["customer_name"], f["customer_phone"],
            "Inbound", status, _format_duration(f["duration"]),
            "Yes", now_local().isoformat()
        ])
    else:
        ws.append_row([
            f["start_time"], resolve_dispatcher_name(f), f["customer_name"], f["customer_phone"],
            "Inbound", "Answered", _format_duration(f["duration"]), "No", ""
        ])

    # CallRail иногда ставит тег про запись на встречу уже в этом, первом вебхуке
    # (AI успевает проанализировать разговор быстро) — не ждём отдельного Call Modified
    schedule_event_type = _detect_schedule_event_type(data)
    if schedule_event_type:
        dispatcher_name = resolve_dispatcher_name(f)
        apt_ws = sh.worksheet("Appointments")
        apt_ws.append_row([
            f["call_id"], now_local().isoformat(), "",
            f["customer_name"], f["customer_phone"], dispatcher_name, "",
            schedule_event_type, ""
        ])

    return jsonify({"status": "ok"}), 200


# ---------- Роут 1б: вебхук ИСХОДЯЩИХ звонков от CallRail (Outbound Post-Call) ----------
# Отдельный адрес — чтобы точно знать, что это перезвон, а не новый звонок клиента
@app.route("/webhook/callrail/call/outbound", methods=["POST"])
def callrail_outbound_call_webhook():
    if not verify_callrail_signature(request):
        return jsonify({"error": "неверная подпись"}), 403

    data = safe_get_json(request)
    f = _extract_call_fields(data)
    if not f["call_id"] and not f["customer_phone"]:
        return jsonify({"status": "ok", "note": "тестовый запрос принят"}), 200

    sh = get_sheet()
    ensure_worksheets(sh)
    ws = sh.worksheet("Calls")

    # Если дозвонились клиенту, у которого был пропущенный звонок — закрываем его
    # и присылаем подтверждение, кто именно дозвонился
    if f["answered"]:
        original = _resolve_missed_call(ws, f["customer_phone"], f["start_time"])
        if original:
            dispatcher_name = resolve_dispatcher_name(f)
            text = (
                f"✅ Customer reached\n"
                f"Customer: {f['customer_name'] or f['customer_phone']}\n"
                f"Phone: {f['customer_phone']}\n"
                f"Missed call was at: {original.get('Timestamp', '')}\n"
                f"Called back by: {dispatcher_name}\n"
                f"Call back time: {f['start_time']}\n"
                f"Duration: {f['duration']} sec"
            )
            send_telegram(OWNER_CHAT_ID, text)

    ws.append_row([
        f["start_time"], resolve_dispatcher_name(f), f["customer_name"], f["customer_phone"],
        "Outbound", ("Answered" if f["answered"] else "Missed"), _format_duration(f["duration"]),
        "No", ""
    ])

    schedule_event_type = _detect_schedule_event_type(data)
    if schedule_event_type:
        dispatcher_name = resolve_dispatcher_name(f)
        apt_ws = sh.worksheet("Appointments")
        apt_ws.append_row([
            f["call_id"], now_local().isoformat(), "",
            f["customer_name"], f["customer_phone"], dispatcher_name, "",
            schedule_event_type, ""
        ])

    return jsonify({"status": "ok"}), 200


def _resolve_missed_call(calls_ws, customer_phone, new_call_timestamp):
    """Находит более ранний нерешённый пропущенный звонок этого же клиента,
    помечает его решённым (дозвонились) и возвращает данные исходного звонка,
    чтобы можно было прислать подтверждение с деталями."""
    if not customer_phone:
        return None
    records = calls_ws.get_all_records()
    for idx, row in enumerate(records, start=2):
        # Ищем звонок со статусом Missed или Voicemail — в обоих случаях
        # клиент ещё не поговорил с живым человеком
        was_unresolved_contact = row["Status"] in ("Missed", "Voicemail")
        if (
            row["Phone"] == customer_phone
            and was_unresolved_contact
            and row["Follow-up Needed"] == "Yes"
        ):
            calls_ws.update_cell(idx, 8, "No")  # колонка Follow-up Needed
            calls_ws.update_cell(idx, 9, "")    # очищаем Last Alert Time
            return row
    return None


# ---------- Роут: CallRail Call Modified (реагируем на теги записи на встречу) ----------
# Самый надёжный способ узнать, кто из диспетчеров назначил встречу — CallRail
# сам ставит тег на звонок по итогам AI-анализа разговора, а мы уже знаем, кто на
# него отвечал (по номеру телефона, см. DISPATCHER_PHONES).
# ВАЖНО: "Schedule booked" (точно подтверждена дата/время) и "Schedule requested"
# (клиент попросил записать, но точное время не согласовано в этом звонке) —
# разные по надёжности сигналы, поэтому считаем и храним их отдельно, не смешивая.
@app.route("/webhook/callrail/call/modified", methods=["POST"])
def callrail_call_modified_webhook():
    if not verify_callrail_signature(request):
        return jsonify({"error": "неверная подпись"}), 403

    data = safe_get_json(request)
    f = _extract_call_fields(data)
    if not f["call_id"] and not f["customer_phone"]:
        return jsonify({"status": "ok", "note": "тестовый запрос принят"}), 200

    event_type = _detect_schedule_event_type(data)
    if event_type is None:
        return jsonify({"status": "ok", "note": "нет тега про запись на встречу — пропущено"}), 200

    dispatcher_name = resolve_dispatcher_name(f)

    sh = get_sheet()
    ensure_worksheets(sh)
    ws = sh.worksheet("Appointments")
    ws.append_row([
        f["call_id"], now_local().isoformat(), "",
        f["customer_name"], f["customer_phone"], dispatcher_name, "",
        event_type, ""
    ])

    return jsonify({"status": "ok", "dispatcher": dispatcher_name, "event_type": event_type}), 200


@app.route("/webhook/callrail/sms/received", methods=["POST"])
def callrail_sms_received_webhook():
    if not verify_callrail_signature(request):
        return jsonify({"error": "неверная подпись"}), 403

    data = safe_get_json(request)
    message_id = data.get("id") or data.get("message_id", "")
    from_number = data.get("customer_phone_number") or data.get("callernum", "")
    to_number = data.get("tracking_phone_number") or data.get("destinationnum", "")
    # Определяем диспетчера по номеру линии, на которую пришло SMS — так же, как для звонков
    dispatcher_name = DISPATCHER_PHONES.get(_normalize_phone(to_number), "")
    timestamp = data.get("created_at") or data.get("datetime") or now_local().isoformat()

    if not from_number:
        return jsonify({"status": "ok", "note": "тестовый запрос принят"}), 200

    sh = get_sheet()
    ensure_worksheets(sh)
    ws = sh.worksheet("Messages")
    ws.append_row([
        message_id, timestamp, "inbound", from_number, to_number,
        dispatcher_name, "False", "False", now_local().isoformat()
    ])
    return jsonify({"status": "ok"}), 200


# ---------- Роут 2б: SMS ОТПРАВЛЕНО диспетчером (Text Message Sent) ----------
@app.route("/webhook/callrail/sms/sent", methods=["POST"])
def callrail_sms_sent_webhook():
    if not verify_callrail_signature(request):
        return jsonify({"error": "неверная подпись"}), 403

    data = safe_get_json(request)
    message_id = data.get("id") or data.get("message_id", "")
    from_number = data.get("tracking_phone_number") or data.get("destinationnum", "")
    to_number = data.get("customer_phone_number") or data.get("callernum", "")
    # Определяем диспетчера по номеру линии, с которой отправлено SMS
    dispatcher_name = DISPATCHER_PHONES.get(_normalize_phone(from_number), "")
    timestamp = data.get("created_at") or data.get("datetime") or now_local().isoformat()

    if not to_number:
        return jsonify({"status": "ok", "note": "тестовый запрос принят"}), 200

    sh = get_sheet()
    ensure_worksheets(sh)
    ws = sh.worksheet("Messages")
    # Это ответ диспетчера клиенту — закрываем висящее входящее SMS от этого номера
    _resolve_pending_sms(ws, to_number)
    ws.append_row([
        message_id, timestamp, "outbound", from_number, to_number,
        dispatcher_name, "True", "True", ""
    ])
    return jsonify({"status": "ok"}), 200


def _resolve_pending_sms(messages_ws, customer_phone):
    """Находит более раннее необработанное входящее SMS этого клиента и
    помечает его отвеченным — значит, диспетчер написал ответ."""
    if not customer_phone:
        return
    records = messages_ws.get_all_records()
    for idx, row in enumerate(records, start=2):
        if (
            row["from"] == customer_phone
            and row["direction"] == "inbound"
            and str(row["resolved"]) == "False"
        ):
            messages_ws.update_cell(idx, 7, "True")   # responded
            messages_ws.update_cell(idx, 8, "True")   # resolved


# ---------- Роут: вебхук Housecall Pro (назначенные встречи) ----------
# Подключается в Housecall Pro: My Apps -> App Store -> Webhooks (доступно на тарифе MAX).
# Подписаться нужно на события: job.scheduled, job.appointment.scheduled, job.appointment.rescheduled
@app.route("/webhook/housecallpro/appointment", methods=["POST"])
def housecallpro_appointment_webhook():
    data = safe_get_json(request)
    event_type = data.get("event", "")
    job = data.get("job")

    if not job or job.get("id") is None:
        # Событие "job.appointment.scheduled/rescheduled" не содержит телефон клиента,
        # а значит мы не можем определить диспетчера по нему — бесполезно для отчётов.
        # Полезные данные (с customer.mobile_number) приходят через "job.scheduled".
        return jsonify({"status": "ok", "note": "событие без данных клиента — пропущено"}), 200

    job_id = job.get("id", "")
    scheduled_start = job.get("schedule", {}).get("scheduled_start", "") if isinstance(job.get("schedule"), dict) else ""
    job_status = job.get("work_status", "")
    job_created_at = job.get("created_at", "")

    customer = job.get("customer", {})
    customer_name = ""
    customer_phone = ""
    if isinstance(customer, dict):
        customer_name = (customer.get("first_name", "") + " " + customer.get("last_name", "")).strip()
        customer_phone = customer.get("mobile_number") or customer.get("home_number") or customer.get("work_number") or ""

    # Технику назначена работа — это НЕ диспетчер, а исполнитель (для справки)
    technician_name = ""
    assigned = job.get("assigned_employees", [])
    if isinstance(assigned, list) and assigned:
        first = assigned[0]
        if isinstance(first, dict):
            technician_name = (first.get("first_name", "") + " " + first.get("last_name", "")).strip()

    sh = get_sheet()
    ensure_worksheets(sh)

    # Определяем диспетчера по недавнему звонку или SMS с этим же номером телефона —
    # логика: если клиент недавно общался с конкретным диспетчером (звонок или переписка),
    # скорее всего именно он и назначил эту встречу
    dispatcher_name = _resolve_dispatcher_by_recent_call(
        sh.worksheet("Calls"), sh.worksheet("Messages"), customer_phone, job_created_at
    ) or "Unknown"

    ws = sh.worksheet("Appointments")
    ws.append_row([
        job_id, now_local().isoformat(), scheduled_start,
        customer_name, customer_phone, dispatcher_name, technician_name, event_type, job_status
    ])

    return jsonify({"status": "ok"}), 200


def _detect_schedule_event_type(data):
    """Определяет, есть ли в данных звонка тег про запись на встречу (CallRail присылает
    теги списком в одних вебхуках и строкой в других — обрабатываем оба варианта).
    Возвращает 'callrail.schedule_booked', 'callrail.schedule_requested' или None."""
    tags_list = data.get("tags") or []
    if isinstance(tags_list, str):
        tags_list = [tags_list]
    single_tag = data.get("tag", "")
    if single_tag and single_tag not in tags_list:
        tags_list = list(tags_list) + [single_tag]
    tags_text = ", ".join(str(t) for t in tags_list).lower()

    if "schedule booked" in tags_text:
        return "callrail.schedule_booked"
    if "schedule requested" in tags_text:
        return "callrail.schedule_requested"
    return None


def _resolve_dispatcher_by_recent_call(calls_ws, messages_ws, customer_phone, before_time_str, window_hours=720):
    """Ищет диспетчера по звонку ИЛИ SMS с тем же номером телефона клиента за последние
    30 дней (окно намеренно широкое — клиент мог обратиться за несколько дней до самой
    записи), незадолго до создания этой работы — вероятно, именно он назначил встречу.
    Проверяем оба канала (звонки и переписку) и берём САМОЕ ПОЗДНЕЕ подходящее
    взаимодействие в пределах окна (ближайшее по времени к созданию job)."""
    target_phone = _normalize_phone(customer_phone)
    if not target_phone:
        return None
    try:
        before_time = datetime.datetime.fromisoformat(str(before_time_str).replace("Z", ""))
        if before_time.tzinfo is not None:
            before_time = before_time.astimezone(TORONTO_TZ).replace(tzinfo=None)
    except (ValueError, TypeError):
        before_time = now_local()
    window_start = before_time - datetime.timedelta(hours=window_hours)

    best_dispatcher = None
    best_time = None

    for row in calls_ws.get_all_records():
        if _normalize_phone(row.get("Phone", "")) != target_phone:
            continue
        try:
            call_time = datetime.datetime.fromisoformat(str(row["Timestamp"]).replace("Z", ""))
        except (ValueError, TypeError):
            continue
        if window_start <= call_time <= before_time:
            if best_time is None or call_time > best_time:
                best_time = call_time
                best_dispatcher = row.get("Dispatcher")

    for row in messages_ws.get_all_records():
        # У SMS клиент может быть либо в "from" (входящее), либо в "to" (исходящее)
        if target_phone not in (_normalize_phone(row.get("from", "")), _normalize_phone(row.get("to", ""))):
            continue
        dispatcher = row.get("agent_email", "")
        if not dispatcher:
            continue  # не смогли определить, чья это линия — пропускаем
        try:
            msg_time = datetime.datetime.fromisoformat(str(row["timestamp"]).replace("Z", ""))
        except (ValueError, TypeError):
            continue
        if window_start <= msg_time <= before_time:
            if best_time is None or msg_time > best_time:
                best_time = msg_time
                best_dispatcher = dispatcher

    return best_dispatcher


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
    now = now_local()

    if period == "week":
        cutoff = now - datetime.timedelta(days=7)
        label = "for the last 7 days"
    else:
        cutoff = now.replace(hour=0, minute=0, second=0, microsecond=0)
        label = "for today"

    def in_period(ts_str):
        try:
            ts = datetime.datetime.fromisoformat(str(ts_str).replace("Z", ""))
        except (ValueError, TypeError):
            return False
        if ts.tzinfo is not None:
            ts = ts.replace(tzinfo=None)
        return ts >= cutoff

    calls_raw = sh.worksheet("Calls").get_all_records()
    calls = [c for c in calls_raw if in_period(c["Timestamp"])]
    messages = [m for m in sh.worksheet("Messages").get_all_records() if in_period(m["timestamp"])]
    appointments = [a for a in sh.worksheet("Appointments").get_all_records() if in_period(a["timestamp_created"])]

    # ВРЕМЕННАЯ ДИАГНОСТИКА: почему отчёт может быть пустым
    debug_info = {
        "now_local": now.isoformat(),
        "cutoff": cutoff.isoformat(),
        "total_call_rows_in_sheet": len(calls_raw),
        "calls_matching_period": len(calls),
        "sample_raw_timestamp": calls_raw[-1]["Timestamp"] if calls_raw else None,
        "sample_dispatcher": calls_raw[-1]["Dispatcher"] if calls_raw else None,
    }

    agents = {}

    def get_agent(name):
        agents.setdefault(name, {
            "calls_total": 0, "calls_answered": 0,
            "sms_total": 0, "sms_answered": 0,
            "appointments": 0, "requested": 0, "last_activity": None, "callback_success": 0,
        })
        return agents[name]

    for c in calls:
        name = c["Dispatcher"]
        if name not in KNOWN_DISPATCHERS:
            # Номер не совпал ни с одним известным диспетчером — пропускаем,
            # чтобы в отчёте не было "мусорных" строк с email или "unassigned"
            continue
        a = get_agent(name)
        a["calls_total"] += 1
        if c["Status"] == "Answered":
            a["calls_answered"] += 1
        _update_last_activity(a, c["Timestamp"])
        # Успешный дозвон = исходящий звонок, на который клиент ответил
        if c["Direction"] == "Outbound" and c["Status"] == "Answered":
            a["callback_success"] = a.get("callback_success", 0) + 1

    for m in messages:
        # считаем только исходящие (ответы диспетчера) как "активность"
        name = DISPATCHER_NAMES.get(m["agent_email"], m["agent_email"] or "unassigned")
        a = get_agent(name)
        if m["direction"] == "inbound":
            a["sms_total"] += 1
            if str(m["responded"]) == "True":
                a["sms_answered"] += 1
        _update_last_activity(a, m["timestamp"])

    for ap in appointments:
        name = ap["dispatcher"]
        if name not in KNOWN_DISPATCHERS:
            # Это техник (или кто-то ещё не из списка диспетчеров) — пропускаем,
            # чтобы не засорять отчёт данными не про диспетчеров
            continue
        if ap["event_type"] == "callrail.schedule_requested":
            # Клиент только попросил записать — не путаем с точно назначенной встречей
            a = get_agent(name)
            a["requested"] = a.get("requested", 0) + 1
            continue
        if ap["event_type"] not in ("job.scheduled", "job.appointment.scheduled", "job.appointment.rescheduled", "callrail.schedule_booked"):
            # job.created — просто создание карточки, ещё не назначение времени;
            # пропускаем, чтобы не считать одну встречу дважды (created + scheduled)
            continue
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
        status = "no data"
        if a["last_activity"]:
            idle_hours = round((now - a["last_activity"]).total_seconds() / 3600, 1)
            if is_workday_now and idle_hours >= NO_ACTIVITY_ALERT_HOURS:
                status = f"⚠️ no activity for {idle_hours} h."
            else:
                status = "active"
        rows.append({
            "name": name, "answer_rate": answer_rate, "sms_rate": sms_rate,
            "calls_total": a["calls_total"], "sms_total": a["sms_total"],
            "appointments": a["appointments"], "requested": a.get("requested", 0),
            "idle_hours": idle_hours, "status": status,
        })

    # Sort by answer rate — worst to best, so problem dispatchers show up first
    rows.sort(key=lambda r: (r["answer_rate"] is None, r["answer_rate"]))

    lines = [f"📈 Dispatcher comparison {label}\n"]
    for r in rows:
        ar = f"{r['answer_rate']}%" if r["answer_rate"] is not None else "—"
        sr = f"{r['sms_rate']}%" if r["sms_rate"] is not None else "—"
        lines.append(
            f"— {r['name']}: calls {ar} ({r['calls_total']}), "
            f"SMS {sr} ({r['sms_total']}), appointments booked {r['appointments']}, "
            f"requested {r['requested']} — {r['status']}"
        )
    if not rows:
        lines.append("No data yet.")

    # Считаем, сколько случаев (пропущенные звонки + голосовые сообщения) до сих пор
    # не решены — то есть клиенту так и не перезвонили/не дозвонились
    still_not_reached = sum(
        1 for c in calls
        if c["Status"] in ("Missed", "Voicemail") and c["Follow-up Needed"] == "Yes"
    )

    # Записываем понятную сводку на отдельный лист Summary — без True/False,
    # чтобы Катрин могла открыть таблицу и сразу всё понять
    summary_ws = sh.worksheet("Summary")
    summary_ws.clear()
    summary_ws.append_row([
        "Dispatcher", "Total Calls", "Answered", "Missed/Voicemail",
        "Answer Rate", "Called Back Successfully", "Still Not Reached",
        "Appointments Booked", "Appointments Requested (pending)"
    ])
    for r in rows:
        answered = round(r["calls_total"] * (r["answer_rate"] or 0) / 100) if r["calls_total"] else 0
        missed = r["calls_total"] - answered
        ar = f"{r['answer_rate']}%" if r["answer_rate"] is not None else "—"
        agent_data = agents[r["name"]]
        summary_ws.append_row([
            r["name"], r["calls_total"], answered, missed, ar,
            agent_data.get("callback_success", 0), "", r["appointments"], r["requested"]
        ])
    summary_ws.append_row([
        "TOTAL — still not reached (missed/voicemail, no callback yet)", "", "", "",
        "", "", still_not_reached, "", ""
    ])

    send_telegram(OWNER_CHAT_ID, "\n".join(lines))
    return jsonify({"status": "ok", "report": rows, "debug": debug_info}), 200


def _update_last_activity(agent_dict, ts_str):
    try:
        ts = datetime.datetime.fromisoformat(str(ts_str).replace("Z", ""))
    except (ValueError, TypeError):
        return
    if ts.tzinfo is not None:
        ts = ts.replace(tzinfo=None)
    if agent_dict["last_activity"] is None or ts > agent_dict["last_activity"]:
        agent_dict["last_activity"] = ts


# ---------- Роут 3: проверка "зависших" звонков/сообщений без ответа ----------
# Вызывается внешним планировщиком (cron-job.org) каждые 15 минут
@app.route("/check-pending", methods=["POST", "GET"])
def check_pending():
    sh = get_sheet()
    ensure_worksheets(sh)
    now = now_local()
    alerts_sent = 0

    def minutes_since(ts_str):
        try:
            ts = datetime.datetime.fromisoformat(str(ts_str).replace("Z", ""))
        except (ValueError, TypeError):
            return None
        if ts.tzinfo is not None:
            ts = ts.replace(tzinfo=None)
        return (now - ts).total_seconds() / 60

    # Не проверяем случаи старше 24 часов — если за сутки не перезвонили,
    # дальнейшие напоминания каждые 5 минут уже не имеют смысла, а только
    # замедляют работу (таблица растёт, и полный обход всех строк — долгий)
    cutoff_recent = now - datetime.timedelta(hours=24)

    # --- Пропущенные звонки / голосовые сообщения, на которые ещё не перезвонили ---
    calls_ws = sh.worksheet("Calls")
    calls_updates = []  # накапливаем обновления, чтобы отправить одним пакетным запросом
    for idx, row in enumerate(calls_ws.get_all_records(), start=2):
        if row["Status"] not in ("Missed", "Voicemail") or row["Follow-up Needed"] != "Yes":
            continue
        try:
            call_time = datetime.datetime.fromisoformat(str(row["Timestamp"]).replace("Z", ""))
        except (ValueError, TypeError):
            continue
        if call_time < cutoff_recent:
            continue  # слишком старый случай, пропускаем ради скорости
        since_alert = minutes_since(row["Last Alert Time"])
        if since_alert is not None and since_alert >= MISSED_CALL_ALERT_MINUTES:
            text = (
                f"🔁 Customer still not reached\n"
                f"Phone: {row['Phone']}\n"
                f"Missed call was at: {row['Timestamp']}\n"
                f"Time without resolution: {int(since_alert)} min."
            )
            send_telegram(OWNER_CHAT_ID, text)
            calls_updates.append({"range": f"I{idx}", "values": [[now.isoformat()]]})  # Last Alert Time
            alerts_sent += 1
    if calls_updates:
        calls_ws.batch_update(calls_updates)

    # --- SMS клиентов без ответа (тоже ограничиваем последними 24 часами) ---
    messages_ws = sh.worksheet("Messages")
    messages_updates = []
    for idx, row in enumerate(messages_ws.get_all_records(), start=2):
        if row["direction"] != "inbound" or str(row["resolved"]) != "False":
            continue
        try:
            msg_time = datetime.datetime.fromisoformat(str(row["timestamp"]).replace("Z", ""))
        except (ValueError, TypeError):
            continue
        if msg_time < cutoff_recent:
            continue
        since_alert = minutes_since(row["last_alert_time"])
        if since_alert is not None and since_alert >= SMS_NO_RESPONSE_ALERT_MINUTES:
            text = (
                f"🔁 Customer SMS still unanswered\n"
                f"From: {row['from']}\n"
                f"Message received: {row['timestamp']}\n"
                f"Time without reply: {int(since_alert)} min."
            )
            send_telegram(OWNER_CHAT_ID, text)
            messages_updates.append({"range": f"I{idx}", "values": [[now.isoformat()]]})  # last_alert_time
            alerts_sent += 1
    if messages_updates:
        messages_ws.batch_update(messages_updates)

    return jsonify({"status": "ok", "alerts_sent": alerts_sent}), 200


# ---------- Роут 4: ежедневная сводка ----------
# Вызывается внешним планировщиком раз в день (например, в 20:00)
@app.route("/daily-summary", methods=["POST", "GET"])
def daily_summary():
    sh = get_sheet()
    ensure_worksheets(sh)
    today = now_local().date().isoformat()

    calls = sh.worksheet("Calls").get_all_records()
    messages = sh.worksheet("Messages").get_all_records()
    appointments = sh.worksheet("Appointments").get_all_records()

    today_calls = [c for c in calls if str(c["Timestamp"]).startswith(today)]
    today_messages = [m for m in messages if str(m["timestamp"]).startswith(today)]
    today_appointments = [a for a in appointments if str(a["timestamp_created"]).startswith(today)]

    stats = {}
    for c in today_calls:
        agent = c["Dispatcher"]
        if agent not in KNOWN_DISPATCHERS:
            continue
        stats.setdefault(agent, {"answered": 0, "missed": 0, "sms_answered": 0, "sms_missed": 0, "appointments": 0, "requested": 0})
        if c["Status"] == "Answered":
            stats[agent]["answered"] += 1
        else:
            stats[agent]["missed"] += 1

    for m in today_messages:
        agent = DISPATCHER_NAMES.get(m["agent_email"], m["agent_email"] or "unassigned")
        if agent not in KNOWN_DISPATCHERS:
            continue
        stats.setdefault(agent, {"answered": 0, "missed": 0, "sms_answered": 0, "sms_missed": 0, "appointments": 0, "requested": 0})
        if str(m["responded"]) == "True":
            stats[agent]["sms_answered"] += 1
        else:
            stats[agent]["sms_missed"] += 1

    for a in today_appointments:
        agent = a["dispatcher"]
        if agent not in KNOWN_DISPATCHERS:
            continue
        stats.setdefault(agent, {"answered": 0, "missed": 0, "sms_answered": 0, "sms_missed": 0, "appointments": 0, "requested": 0})
        if a["event_type"] == "callrail.schedule_requested":
            stats[agent]["requested"] += 1
            continue
        if a["event_type"] not in ("job.scheduled", "job.appointment.scheduled", "job.appointment.rescheduled", "callrail.schedule_booked"):
            continue
        stats[agent]["appointments"] += 1

    # Включаем всех известных диспетчеров даже с нулевой активностью —
    # так сразу видно, кто вообще не работал в этот день
    for name in KNOWN_DISPATCHERS:
        stats.setdefault(name, {"answered": 0, "missed": 0, "sms_answered": 0, "sms_missed": 0, "appointments": 0, "requested": 0})

    lines = [f"📊 Summary for {today}\n"]
    for agent, s in stats.items():
        lines.append(
            f"— {agent}: calls answered {s['answered']}, missed {s['missed']}, "
            f"SMS answered {s['sms_answered']}, unanswered {s['sms_missed']}, "
            f"appointments booked {s['appointments']}, requested {s.get('requested', 0)}"
        )
    if not stats:
        lines.append("No data yet today.")

    # Записываем историю по дням — накапливается со временем, можно смотреть
    # любой прошедший день, прокручивая вкладку Daily History вниз
    history_ws = sh.worksheet("Daily History")
    for agent, s in stats.items():
        history_ws.append_row([
            today, agent, s["answered"], s["missed"], s["sms_answered"],
            s["appointments"], s.get("requested", 0)
        ])

    send_telegram(OWNER_CHAT_ID, "\n".join(lines))
    return jsonify({"status": "ok", "stats": stats}), 200



# ---------- Роут: подробная статистика одного диспетчера за всё время ----------
# Пример: /dispatcher-report?name=Sarah — создаёт/обновляет отдельную вкладку
# с полной картиной по этому человеку с самого начала работы системы.
@app.route("/dispatcher-report", methods=["GET"])
def dispatcher_report():
    name = request.args.get("name", "Sarah")
    if name not in KNOWN_DISPATCHERS:
        return jsonify({
            "status": "error",
            "note": f"'{name}' не входит в список известных диспетчеров: {sorted(KNOWN_DISPATCHERS)}"
        }), 400

    sh = get_sheet()
    ensure_worksheets(sh)

    calls = sh.worksheet("Calls").get_all_records()
    appointments = sh.worksheet("Appointments").get_all_records()

    her_calls = [c for c in calls if c["Dispatcher"] == name]
    total_calls = len(her_calls)
    answered = sum(1 for c in her_calls if c["Status"] == "Answered")
    missed = sum(1 for c in her_calls if c["Status"] in ("Missed", "Voicemail"))
    inbound = sum(1 for c in her_calls if c["Direction"] == "Inbound")
    outbound = sum(1 for c in her_calls if c["Direction"] == "Outbound")
    successful_callbacks = sum(
        1 for c in her_calls if c["Direction"] == "Outbound" and c["Status"] == "Answered"
    )
    answer_rate = round(answered / total_calls * 100, 1) if total_calls else 0
    missed_rate = round(missed / total_calls * 100, 1) if total_calls else 0

    # Средняя длительность отвеченных разговоров (Duration хранится как "мин:сек" текстом)
    def _duration_to_seconds(text):
        try:
            m, s = str(text).split(":")
            return int(m) * 60 + int(s)
        except (ValueError, AttributeError):
            return 0

    answered_durations = [
        _duration_to_seconds(c["Duration"]) for c in her_calls if c["Status"] == "Answered"
    ]
    avg_duration_sec = round(sum(answered_durations) / len(answered_durations)) if answered_durations else 0

    her_appointments = [
        a for a in appointments
        if a["dispatcher"] == name
        and a["event_type"] in ("job.scheduled", "job.appointment.scheduled", "job.appointment.rescheduled", "callrail.schedule_booked")
    ]
    appointments_booked = len(her_appointments)
    her_requested = [a for a in appointments if a["dispatcher"] == name and a["event_type"] == "callrail.schedule_requested"]
    appointments_requested = len(her_requested)
    # "Закрываемость" — какая доля отвеченных звонков в итоге привела к назначенной встрече
    conversion_rate = round(appointments_booked / answered * 100, 1) if answered else 0

    # Точный период, за который считается статистика — чтобы не было путаницы
    all_timestamps = [c["Timestamp"] for c in her_calls if c["Timestamp"]]
    period_start = min(all_timestamps) if all_timestamps else "no data"
    period_end = max(all_timestamps) if all_timestamps else "no data"

    rows = [
        ["Metric", "Value"],
        ["Report generated (Toronto time)", now_local().isoformat(sep=" ", timespec="seconds")],
        ["Period covered", f"{period_start}  →  {period_end}"],
        ["Note", "Stats reset each time the Calls sheet is cleared — not lifetime totals"],
        ["", ""],
        ["Total Calls (all-time)", total_calls],
        ["  — Inbound", inbound],
        ["  — Outbound (callbacks made)", outbound],
        ["Calls Answered", answered],
        ["Calls Missed / Voicemail", missed],
        ["Answer Rate", f"{answer_rate}%"],
        ["Missed Rate", f"{missed_rate}%"],
        ["Average Call Duration (answered calls)", _format_duration(avg_duration_sec)],
        ["Successful Callbacks (reached customer)", successful_callbacks],
        ["", ""],
        ["Appointments Booked (confirmed, all-time)", appointments_booked],
        ["Appointments Requested (pending, not yet confirmed)", appointments_requested],
        ["Conversion Rate (booked ÷ answered calls)", f"{conversion_rate}%"],
    ]

    existing = [ws.title for ws in sh.worksheets()]
    if name not in existing:
        ws = sh.add_worksheet(title=name, rows=50, cols=2)
    else:
        ws = sh.worksheet(name)
        ws.clear()
    ws.update(values=rows, range_name="A1")

    return jsonify({
        "status": "ok", "dispatcher": name,
        "stats": {
            "total_calls": total_calls, "inbound": inbound, "outbound": outbound,
            "answered": answered, "missed": missed, "answer_rate": answer_rate,
            "successful_callbacks": successful_callbacks,
            "appointments_booked": appointments_booked, "conversion_rate": conversion_rate,
        }
    }), 200



# ---------- Роут: общая сводка по всем диспетчерам вместе ----------
@app.route("/company-overview", methods=["GET"])
def company_overview():
    sh = get_sheet()
    ensure_worksheets(sh)

    calls = sh.worksheet("Calls").get_all_records()
    appointments = sh.worksheet("Appointments").get_all_records()

    def _duration_to_seconds(text):
        try:
            m, s = str(text).split(":")
            return int(m) * 60 + int(s)
        except (ValueError, AttributeError):
            return 0

    known_calls = [c for c in calls if c["Dispatcher"] in KNOWN_DISPATCHERS]
    total_calls = len(known_calls)
    total_answered = sum(1 for c in known_calls if c["Status"] == "Answered")
    total_missed = sum(1 for c in known_calls if c["Status"] in ("Missed", "Voicemail"))
    company_answer_rate = round(total_answered / total_calls * 100, 1) if total_calls else 0
    answered_durations = [_duration_to_seconds(c["Duration"]) for c in known_calls if c["Status"] == "Answered"]
    company_avg_duration = round(sum(answered_durations) / len(answered_durations)) if answered_durations else 0

    known_appointments = [
        a for a in appointments
        if a["dispatcher"] in KNOWN_DISPATCHERS
        and a["event_type"] in ("job.scheduled", "job.appointment.scheduled", "job.appointment.rescheduled", "callrail.schedule_booked")
    ]
    total_appointments = len(known_appointments)
    known_requested = [
        a for a in appointments
        if a["dispatcher"] in KNOWN_DISPATCHERS and a["event_type"] == "callrail.schedule_requested"
    ]
    total_requested = len(known_requested)
    company_conversion = round(total_appointments / total_answered * 100, 1) if total_answered else 0

    all_timestamps = [c["Timestamp"] for c in known_calls if c["Timestamp"]]
    period_start = min(all_timestamps) if all_timestamps else "no data"
    period_end = max(all_timestamps) if all_timestamps else "no data"

    rows = [
        ["COMPANY TOTALS (all dispatchers combined)", ""],
        ["Report generated (Toronto time)", now_local().isoformat(sep=" ", timespec="seconds")],
        ["Period covered", f"{period_start}  →  {period_end}"],
        ["", ""],
        ["Total Calls", total_calls],
        ["Calls Answered", total_answered],
        ["Calls Missed / Voicemail", total_missed],
        ["Company Answer Rate", f"{company_answer_rate}%"],
        ["Average Call Duration", _format_duration(company_avg_duration)],
        ["", ""],
        ["Appointments Booked (confirmed)", total_appointments],
        ["Appointments Requested (pending, not yet confirmed)", total_requested],
        ["Company Conversion Rate (booked ÷ answered calls)", f"{company_conversion}%"],
        ["", ""],
        ["PER-DISPATCHER BREAKDOWN", ""],
        ["Dispatcher", "Calls", "Answered", "Missed", "Answer Rate", "Booked", "Requested", "Conversion Rate"],
    ]

    per_dispatcher = []
    for name in sorted(KNOWN_DISPATCHERS):
        her_calls = [c for c in known_calls if c["Dispatcher"] == name]
        t = len(her_calls)
        a_ = sum(1 for c in her_calls if c["Status"] == "Answered")
        m_ = sum(1 for c in her_calls if c["Status"] in ("Missed", "Voicemail"))
        ar = round(a_ / t * 100, 1) if t else 0
        ap_count = sum(1 for a in known_appointments if a["dispatcher"] == name)
        req_count = sum(1 for a in known_requested if a["dispatcher"] == name)
        cr = round(ap_count / a_ * 100, 1) if a_ else 0
        per_dispatcher.append((name, t, a_, m_, ar, ap_count, req_count, cr))

    # Сортируем от худшего к лучшему по % ответов — проблемные сразу видны сверху
    per_dispatcher.sort(key=lambda r: r[4])
    for name, t, a_, m_, ar, ap_count, req_count, cr in per_dispatcher:
        rows.append([name, t, a_, m_, f"{ar}%", ap_count, req_count, f"{cr}%"])

    existing = [ws.title for ws in sh.worksheets()]
    if "Company Overview" not in existing:
        ws = sh.add_worksheet(title="Company Overview", rows=50, cols=8)
    else:
        ws = sh.worksheet("Company Overview")
        ws.clear()
    ws.update(values=rows, range_name="A1")

    return jsonify({
        "status": "ok",
        "company_totals": {
            "total_calls": total_calls, "answered": total_answered, "missed": total_missed,
            "answer_rate": company_answer_rate, "appointments_booked": total_appointments,
            "appointments_requested": total_requested, "conversion_rate": company_conversion,
        },
        "per_dispatcher": [
            {"name": n, "calls": t, "answered": a_, "missed": m_, "answer_rate": ar,
             "appointments_booked": ap_count, "appointments_requested": req_count, "conversion_rate": cr}
            for n, t, a_, m_, ar, ap_count, req_count, cr in per_dispatcher
        ],
    }), 200


@app.route("/", methods=["GET"])
def health():
    return jsonify({"status": "агент-помощник работает"}), 200


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
