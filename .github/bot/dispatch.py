"""Диспетчер Telegram для пайплайна «Записки диванного архівіста».

Запускається з GitHub Actions (див. .github/workflows/dispatch.yml) раз на 20 хвилин,
після змін у data/ і вручну. Рішень не приймає — веде діалог з користувачем кнопками
й переносить його вибір у data/, а хмарні задачі запускає через API.

Потоки: 🧵 серйозний тред, 😏 жартівливий, 🌶 провокація, 📰 Telegram-допис.
Кожна ідея належить одному потоку.

1. Ідеї (09:00): «Готовий до ідей?» → користувач обирає потік → запит у
   data/requests.json → задача «Ідеї» шукає 10 ідей цього потоку → ✅/❌.
2. Тексти (12:00): погоджені теми без треду, згруповані за потоками → користувач
   обирає тему → задача «Тексти» пише тред → «✅ Готово» або правки відповіддю.
3. Публікація: готові треди за потоками → фінальна перевірка (тема, фото) → час:
   найближчий рекомендований слот або «зараз». Заплановане публікується саме
   (воркер будить диспетчер у потрібний момент). Якщо на рекомендований час нічого
   не заплановано — нагадування за годину.
4. Аналітика: статистика всіх постів акаунта в data/analytics.json — раз на 3 години
   під час інших запусків, без окремих пробуджень (тижневий звіт більше не надсилається).

5. Дашборд: закріплене rich-повідомлення (конвеєр, розклад на 7 днів, стан системи);
   редагується наприкінці запуску, лише якщо зміст змінився (або раз на 3 години).

Команди: /ideas, /proposed, /texts, /publish, /queue, /tgqueue, /published, /dashboard.
Режим `--fire` (окремий крок workflow після пушу) запускає хмарні задачі, щоб вони
бачили вже збережені в репозиторії запити.
"""

import hashlib
import json
import os
import random
import re
import sys
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[2]
IDEAS_FILE = ROOT / "data" / "ideas.json"
REQUESTS_FILE = ROOT / "data" / "requests.json"
STRESS_FILE = ROOT / "data" / "stress_requests.json"
ANALYTICS_FILE = ROOT / "data" / "analytics.json"
RECS_FILE = ROOT / "data" / "recommendations.json"
RUBRICS_FILE = ROOT / "data" / "rubrics.json"
STATE_FILE = Path(__file__).resolve().parent / "state.json"
FIRE_FILE = Path(os.environ.get("RUNNER_TEMP", "/tmp")) / "genshorts_fire.json"
ACK_FILE = Path(os.environ.get("RUNNER_TEMP", "/tmp")) / "genshorts_ack.json"
# Черга оновлень у Cloudflare Worker (вебхук). Без неї — старий режим getUpdates.
QUEUE_URL = os.environ.get("QUEUE_URL", "").strip().rstrip("/")
QUEUE_SECRET = os.environ.get("QUEUE_SECRET", "").strip()
EVENT_UPDATE = os.environ.get("EVENT_UPDATE", "").strip()

TOKEN = os.environ["TELEGRAM_BOT_TOKEN"].strip()
CHAT_ID = int(os.environ["TELEGRAM_CHAT_ID"].strip())
ROUTINES = {
    "ideas": (os.environ.get("ROUTINE_IDEAS_URL", "").strip(), os.environ.get("ROUTINE_IDEAS_TOKEN", "").strip()),
    "texts": (os.environ.get("ROUTINE_TEXTS_URL", "").strip(), os.environ.get("ROUTINE_TEXTS_TOKEN", "").strip()),
    "analytics": (os.environ.get("ROUTINE_ANALYTICS_URL", "").strip(),
                  os.environ.get("ROUTINE_ANALYTICS_TOKEN", "").strip()),
}

THREADS_TOKEN = os.environ.get("THREADS_ACCESS_TOKEN", "").strip()
THREADS_USER_ID = os.environ.get("THREADS_USER_ID", "").strip()
THREADS_TOKEN_UPDATED = os.environ.get("THREADS_TOKEN_UPDATED", "").strip()  # YYYY-MM-DD
THREADS_API = "https://graph.threads.net/v1.0"
THREADS_POST_LIMIT = 500

KYIV = ZoneInfo("Europe/Kyiv")
DEFAULT_TAG = "Генеалогія"  # стандартна тема поста в Threads
STALE_AFTER = timedelta(minutes=30)  # незавершена робота без запуску задачі довше — перезапуск
ANALYTICS_EVERY = timedelta(hours=3)  # як часто оновлювати статистику Threads
# Окремо заради статистики диспетчер не прокидається: збирає її під час інших запусків
# (щонайменше запобіжні о 08:00 і 20:00 за Києвом).
# Хвилини GitHub Actions округлюються вгору до цілої хвилини, тож після роботи диспетчер
# ще чекає нові натискання до кінця вже оплаченої хвилини (крім часу на коміт і пуш).
JOB_STARTED_AT = float(os.environ.get("JOB_STARTED_AT") or time.time())
LINGER_MARGIN = 20  # секунд лишаємо на збереження стану й запуск задач після скрипта
LINGER_POLL = 2.5  # як часто перевіряти чергу натискань, секунд
MENU_VERSION = 4
MENU_BUTTONS = {"💡 Ідеї": "/ideas", "✍️ Тексти": "/texts", "📣 Публікація": "/publish", "📋 Черга": "/queue",
                "📰 TG-черга": "/tgqueue", "📤 Опубліковані": "/published", "📊 Дашборд": "/dashboard"}
MAIN_KEYBOARD = {"keyboard": [[{"text": "💡 Ідеї"}, {"text": "✍️ Тексти"}], [{"text": "📣 Публікація"}, {"text": "📋 Черга"}],
                              [{"text": "📰 TG-черга"}, {"text": "📤 Опубліковані"}], [{"text": "📊 Дашборд"}]],
                 "is_persistent": True, "resize_keyboard": True}
HELP = ("Кнопки внизу: 💡 Ідеї — нові ідеї, ✍️ Тексти — написати тред, 📣 Публікація — опублікувати, "
        "📋 Черга — треди Threads, 📰 TG-черга — Telegram-дописи, 📤 Опубліковані — що вже вийшло, "
        "📊 Дашборд — закріплене зведення (конвеєр, розклад, стан системи), що оновлюється само.\n"
        "Своя тема — просто напишіть її звичайним повідомленням.")
# Дашборд — закріплене rich-повідомлення (Bot API 10.1+, sendRichMessage). Диспетчер редагує
# його наприкінці кожного запуску, якщо дані змінилися, або щонайменше раз на DASH_REFRESH.
DASH_DAYS = 7  # скільки днів розкладу показувати
DASH_REFRESH = timedelta(hours=3)
ASK_AT = {"ideas": 9, "texts": 12}  # година за Києвом
SLOT_GAP = timedelta(hours=4)  # мінімальний проміжок між публікаціями бота
DAILY_LIMIT = 1  # тредів бота на день (автор: одна публікація щодня)
ALTERNATION_HORIZON = timedelta(days=3)  # скільки шукати слот із чергуванням, перш ніж поступитися
REMIND_BEFORE = timedelta(hours=1)  # нагадування, якщо на рекомендований час нічого не заплановано
WEEKDAYS = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Нд"]
METRICS = "views,likes,replies,reposts,quotes,shares"
TG_LIMIT = 3900
STREAMS = {
    "big": ("🧵", "Серйозний"),  # колишні «малі серйозні» (small) злиті сюди
    "fun": ("😏", "Жартівливий"),
    "provoke": ("🌶", "Провокація"),
    "tg": ("📰", "Telegram-допис"),
}
# Telegram-дописи: окрема черга. Щовівторка о 16:00 — у тестовий канал (TG_CHANNEL_ID),
# потім той самий допис — в основний у випадковий час 17:00–19:00. Опубліковано = вийшов в основному.
TG_CHANNEL_ID = os.environ.get("TG_CHANNEL_ID", "").strip()
TG_MAIN_CHANNEL_ID = os.environ.get("TG_MAIN_CHANNEL_ID", "").strip() or "@archivist_notes"
# Окремий бот автора, який уже є адміністратором обох каналів; без нього — бот пайплайна.
TG_POSTER_TOKEN = os.environ.get("TG_POSTER_TOKEN", "").strip()
TG_SLOT = (1, 16, 0)  # вівторок, 16:00 за Києвом — тестовий канал
TG_MAIN_WINDOW = ((17, 0), (19, 0))  # основний канал — випадковий час у цьому вікні
TG_POST_LIMIT = 4096
# Рубрика вівторкових дописів: перший рядок кожного допису (клікабельний хештег у каналі).
TG_RUBRIC = "#РодовіднаПросвіта"


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def load_json(path: Path, default):
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return default


def save_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


def tg(method: str, **params):
    request = urllib.request.Request(
        f"https://api.telegram.org/bot{TOKEN}/{method}",
        data=json.dumps(params).encode("utf-8"),
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.load(response).get("result")
    except urllib.error.HTTPError as exc:
        description = json.load(exc).get("description", "")
        # Старі callback-запити Telegram відхиляє — це не помилка пайплайна.
        if method in ("answerCallbackQuery", "editMessageReplyMarkup", "sendPhoto", "setMyCommands"):
            print(f"[tg] {method}: {description}")
            return None
        raise SystemExit(f"[tg] {method} failed: {description}")


def tg_try(method: str, **params):
    """Як tg(), але не зупиняє диспетчер: (результат, None) або (None, опис помилки)."""
    request = urllib.request.Request(
        f"https://api.telegram.org/bot{TOKEN}/{method}",
        data=json.dumps(params).encode("utf-8"),
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.load(response).get("result"), None
    except urllib.error.HTTPError as exc:
        try:
            return None, json.load(exc).get("description", f"HTTP {exc.code}")
        except ValueError:
            return None, f"HTTP {exc.code}"
    except (urllib.error.URLError, OSError) as exc:
        return None, str(exc)


def esc(text) -> str:
    """Екранування для rich HTML (підтримуються лише базові іменовані сутності)."""
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")


def short(text: str, limit: int = 48) -> str:
    text = " ".join(str(text).split())
    return text if len(text) <= limit else text[:limit - 1].rstrip() + "…"


def tg_time(moment: datetime, fmt: str, fallback: str) -> str:
    """Дата-час, який Telegram показує в часовому поясі й мові читача (r — «5 хв тому»)."""
    return f'<tg-time unix="{int(moment.timestamp())}" format="{fmt}">{esc(fallback)}</tg-time>'


def fmt_slot(moment: datetime) -> str:
    local = moment.astimezone(KYIV)
    return f"{WEEKDAYS[local.weekday()]} {local:%d.%m %H:%M}"


def parse_time(value: str) -> datetime:
    """ISO-час з репозиторію чи Threads (формат «+0000» теж)."""
    return datetime.fromisoformat(re.sub(r"([+-]\d\d)(\d\d)$", r"\1:\2", value))


def kb(*rows):
    return {"inline_keyboard": [[{"text": t, "callback_data": d} for t, d in row] for row in rows if row]}


def chunks(items, size):
    return [items[i:i + size] for i in range(0, len(items), size)]


def stream_label(stream: str | None) -> str:
    icon, name = STREAMS.get(stream or "", ("•", "Без потоку"))
    return f"{icon} {name}"


def history(idea: dict, event: str) -> None:
    idea.setdefault("history", []).append({"at": now(), "event": event})


def split_for_telegram(text: str, limit: int = TG_LIMIT) -> list[str]:
    """Ділить довгий текст на частини до limit, не розриваючи пости треду."""
    parts, current = [], ""
    for block in text.split("\n\n"):
        candidate = f"{current}\n\n{block}" if current else block
        if len(candidate) <= limit:
            current = candidate
            continue
        if current:
            parts.append(current)
        while len(block) > limit:
            parts.append(block[:limit])
            block = block[limit:]
        current = block
    if current:
        parts.append(current)
    return parts


def queue_call(method: str, path: str, body=None):
    request = urllib.request.Request(
        f"{QUEUE_URL}{path}", method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Authorization": f"Bearer {QUEUE_SECRET}", "Content-Type": "application/json",
                 "User-Agent": "genshorts-dispatch"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def fire_routine(name: str, note: str) -> str | None:
    """Повертає текст помилки або None, якщо задачу запущено."""
    url, token = ROUTINES[name]
    if not (url and token):
        return f"задача «{name}» не підключена (немає секретів ROUTINE_{name.upper()}_URL/TOKEN)"
    request = urllib.request.Request(
        url,
        data=json.dumps({"text": note}).encode(),
        headers={
            "Authorization": f"Bearer {token}",
            "anthropic-beta": "experimental-cc-routine-2026-04-01",
            "anthropic-version": "2023-06-01",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=30):
            print(f"[routine] {name} fired")
            return None
    except urllib.error.HTTPError as exc:
        return f"не вдалося запустити задачу «{name}»: HTTP {exc.code}"


class ThreadsError(Exception):
    def __init__(self, message: str, status: int = 0, subcode: int = 0):
        super().__init__(message)
        self.status = status
        self.subcode = subcode


def threads_call(method: str, path: str, **params):
    params["access_token"] = THREADS_TOKEN
    query = urllib.parse.urlencode(params)
    url = f"{THREADS_API}/{path}"
    request = (urllib.request.Request(f"{url}?{query}") if method == "GET"
               else urllib.request.Request(url, data=query.encode(), method="POST"))
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.load(response)
    except urllib.error.HTTPError as exc:
        try:
            error = json.load(exc).get("error", {})
        except ValueError:
            error = {}
        details = " ".join(f"{k}={error[k]}" for k in ("message", "type", "code", "error_subcode", "fbtrace_id") if error.get(k))
        print(f"[threads] {method} {path.split('/')[-1]} -> HTTP {exc.code} {details}")
        raise ThreadsError(f"HTTP {exc.code} {error.get('message', '')}".strip(), exc.code,
                           int(error.get("error_subcode") or 0)) from None


def threads_retry(method: str, path: str, **params):
    """Повторює запит при збоях 5xx на боці Threads."""
    for attempt in range(3):
        try:
            return threads_call(method, path, **params)
        except ThreadsError as exc:
            if exc.status < 500 or attempt == 2:
                raise
            time.sleep(10 * (attempt + 1))


def threads_length(text: str) -> int:
    """Довжина за правилами Threads: емодзі рахуються за кількістю байтів UTF-8."""
    return sum(len(ch.encode("utf-8")) if ord(ch) > 0xFFFF or unicodedata.category(ch) == "So" else 1
               for ch in text)


def unverified(text: str) -> int:
    """Кількість позначок [ПЕРЕВІРИТИ] — з ними текст не можна погоджувати й публікувати."""
    return len(re.findall(r"\[\s*перевірити\s*\]", text, flags=re.IGNORECASE))


UNVERIFIED_HINT = ("Поки вони є, тред не можна погодити чи опублікувати. Дайте правку відповіддю на тред: "
                   "підтвердіть факт (наприклад, «факт про … правильний, прибери позначку») або попросіть його прибрати.")


def thread_posts(text: str) -> list[str]:
    """Пости треду без службового блоку «Джерела»."""
    blocks = [b.strip() for b in re.split(r"\n\s*---\s*\n", text) if b.strip()]
    return [b for b in blocks if not b.lower().startswith("джерела")]


def edit_hint(plain_reply: str) -> str:
    """Підказка про правки під текстом; plain_reply — що робить звичайна відповідь без цитати й «→»."""
    return ("✏️ Як правити — відповіддю на повідомлення з текстом:\n"
            "• одне місце: виділіть фрагмент у тексті → «Відповісти» → напишіть, на що замінити "
            "(«-» — видалити фрагмент);\n"
            "• кілька місць: кожна заміна окремим рядком «старе → нове»;\n"
            f"• {plain_reply}\n"
            "Точкова правка змінює лише вказаний фрагмент; виправлений текст прийде на повторне погодження.")


EDIT_ARROW = re.compile(r"\s*(?:→|=>|->)\s*")
EDIT_DELETE = {"-", "—", "видалити", "видали", "прибрати", "прибери"}


def unquote(text: str) -> str:
    text = text.strip()
    for left, right in (("«", "»"), ('"', '"'), ("“", "”"), ("'", "'")):
        if len(text) >= 2 and text.startswith(left) and text.endswith(right):
            return text[1:-1]
    return text


def parse_edits(text: str, quote: str = "") -> list[tuple[str, str]] | None:
    """Точкові правки з відповіді автора або None, якщо це звичайні побажання для переписування.
    Цитата (Telegram «Відповісти» з виділеним фрагментом) + текст — фрагмент замінюється текстом;
    рядки «старе → нове» — кожен рядок окрема заміна. Порожнє «нове» або «-» — видалити фрагмент."""
    if quote.strip():
        new = "" if text.strip().lower() in EDIT_DELETE else unquote(text)
        return [(quote.strip(), new)]
    lines = [line for line in text.splitlines() if line.strip()]
    if not lines or not all(EDIT_ARROW.search(line) for line in lines):
        return None
    edits = []
    for line in lines:
        old, new = EDIT_ARROW.split(line, maxsplit=1)
        new = "" if new.strip().lower() in EDIT_DELETE else unquote(new)
        if not unquote(old):
            return None
        edits.append((unquote(old), new))
    return edits


def fragment_pattern(fragment: str) -> re.Pattern:
    """Шукає фрагмент, не зважаючи на різницю в пробілах/переносах і на вид апострофа."""
    words = [re.sub(r"['’ʼ`]", "['’ʼ`]", re.escape(w).replace("\\'", "'")) for w in fragment.split()]
    return re.compile(r"\s+".join(words))


def apply_edits(body: str, edits: list[tuple[str, str]]) -> tuple[str, list[str]]:
    """Застосовує всі правки або жодної: (новий текст, список проблем)."""
    problems = []
    for old, new in edits:
        matches = list(fragment_pattern(old).finditer(body))
        short = old if len(old) <= 60 else old[:57] + "…"
        if not matches:
            problems.append(f"не знайшов «{short}»")
        elif len(matches) > 1:
            problems.append(f"«{short}» трапляється {len(matches)} рази — дайте довший фрагмент")
        else:
            left, right = body[:matches[0].start()], body[matches[0].end():]
            if not new and left.endswith(" ") and (right[:1] in ("", " ", "\n") or right[:1] in ",.;:!?…)»"):
                left = left[:-1]  # після видалення не лишати зайвого пробілу
            body = left + new + right
    return body, problems


def tg_permalink(channel: str, message_id) -> str:
    return (f"https://t.me/{channel[1:]}/{message_id}" if channel.startswith("@")
            else f"https://t.me/c/{channel[4:]}/{message_id}" if channel.startswith("-100") else "")


def tg_main_slot(after: datetime) -> datetime:
    """Випадковий момент (з точністю до хвилини) у вікні основного каналу, не раніше ніж за 5 хв."""
    earliest = after.astimezone(KYIV) + timedelta(minutes=5)
    day = earliest.date()
    while True:
        (h1, m1), (h2, m2) = TG_MAIN_WINDOW
        start = max(datetime(day.year, day.month, day.day, h1, m1, tzinfo=KYIV), earliest.replace(second=0, microsecond=0))
        end = datetime(day.year, day.month, day.day, h2, m2, tzinfo=KYIV)
        if start < end:
            return start + timedelta(minutes=random.randrange(int((end - start).total_seconds() // 60)))
        day += timedelta(days=1)


def sources_block(text: str) -> str:
    """Службовий блок «Джерела» з thread.md (порожній рядок, якщо його немає)."""
    blocks = [b.strip() for b in re.split(r"\n\s*---\s*\n", text) if b.strip()]
    return "\n\n".join(b for b in blocks if b.lower().startswith("джерела"))


def tg_post_text(text: str) -> str:
    """Текст Telegram-допису для каналу: завжди починається з рубрики TG_RUBRIC."""
    body = "\n\n".join(thread_posts(text))
    if not body or body.lower().startswith(TG_RUBRIC.lower()):
        return body
    return f"{TG_RUBRIC}\n{body}"


class Bot:
    def __init__(self):
        self.ideas_doc = load_json(IDEAS_FILE, {"ideas": []})
        self.ideas = {i["id"]: i for i in self.ideas_doc["ideas"]}
        self.requests = load_json(REQUESTS_FILE, {"ideas": []})
        self.stress = load_json(STRESS_FILE, {"requests": []})
        self.state = load_json(STATE_FILE, {})
        for key, default in (("offset", 0), ("sent", {}), ("messages", {}), ("approved_threads", {}), ("asked", {})):
            self.state.setdefault(key, default)
        self.fire = {}  # routine -> set(ids)
        self.acked = set()  # оновлення з черги воркера, які можна підтвердити після пушу
        self.announced = 0  # до якого моменту диспетчер пообіцяв воркеру опитувати чергу (/live)

    # ------------------------------------------------------------ відправка

    def send(self, text: str, markup=None, record: dict | None = None, reply_to: int | None = None,
             limit: int = TG_LIMIT) -> int:
        """Надсилає текст (довгий — кількома повідомленнями); кнопки й запис — на останньому."""
        pieces = split_for_telegram(text, limit)
        message_id = None
        for n, piece in enumerate(pieces):
            last = n == len(pieces) - 1
            params = {"chat_id": CHAT_ID, "text": piece, "link_preview_options": {"is_disabled": True}}
            if last and markup:
                params["reply_markup"] = markup
            if reply_to and n == 0:
                params["reply_to_message_id"] = reply_to
            message_id = tg("sendMessage", **params)["message_id"]
            if record is not None:
                self.state["messages"][str(message_id)] = record
        return message_id

    def close_buttons(self, message_id: int, label: str) -> None:
        tg("editMessageReplyMarkup", chat_id=CHAT_ID, message_id=message_id, reply_markup=kb([(label, "noop")]))

    def mark_publishing(self, item: dict, message_id: int, label: str = "⏳ Публікується…") -> None:
        """Замість кнопок — статус; після виходу publish_done замінить його на посилання."""
        self.close_buttons(message_id, label)
        item["status_mid"] = message_id

    def publish_done(self, item: dict, permalink: str) -> None:
        message_id = item.pop("status_mid", None)
        if message_id:
            markup = ({"inline_keyboard": [[{"text": "✅ Опубліковано — відкрити", "url": permalink}]]} if permalink
                      else kb([("✅ Опубліковано", "noop")]))
            tg("editMessageReplyMarkup", chat_id=CHAT_ID, message_id=message_id, reply_markup=markup)

    def thread_text(self, idea: dict) -> str | None:
        path = idea.get("thread", {}).get("file")
        if path and (ROOT / path).is_file():
            return (ROOT / path).read_text(encoding="utf-8").strip()
        return None

    # ------------------------------------------------------------ списки тем

    def grouped(self, predicate):
        groups = {}
        for idea in self.ideas.values():
            if idea.get("test") or not predicate(idea):
                continue
            groups.setdefault(idea.get("stream"), []).append(idea)
        order = list(STREAMS) + [None]
        return [(s, groups[s]) for s in order if s in groups]

    def topic_menu(self, header: str, groups, action: str, empty: str, quiet: bool) -> None:
        if not groups:
            if not quiet:
                self.send(empty)
            return
        lines, buttons, n = [header], [], 0
        for stream, ideas in groups:
            lines.append(f"\n{stream_label(stream)}")
            for idea in ideas:
                n += 1
                lines.append(f"{n}. {idea['title']}")
                buttons.append((str(n), f"{action}:{idea['id']}"))
        self.send("\n".join(lines), kb(*chunks(buttons, 5)), {"kind": "menu"})

    # ------------------------------------------------------------ меню

    def ask_ideas(self):
        rows = [[(stream_label(s), f"ideas:{s}")] for s in STREAMS] + [[("Не сьогодні", "ideas:skip")]]
        pending = len(self.proposed())
        if pending:
            rows.insert(0, [(f"💡 Нерозглянуті ідеї ({pending})", "ideas:review")])
        self.send("☀️ Готовий до нових ідей? Оберіть потік:", kb(*rows), {"kind": "ask"})

    # ------------------------------------------------------------ нерозглянуті ідеї

    def proposed(self, stream: str | None = None) -> list[dict]:
        """Ідеї, які автор ще не погодив і не відхилив (найстаріші спершу)."""
        return sorted((i for i in self.ideas.values() if i.get("status") == "proposed" and not i.get("test")
                       and (stream is None or (i.get("stream") or "") == stream)),
                      key=lambda i: (i.get("proposed_at", ""), i["id"]))

    def show_proposed(self, stream: str | None = None) -> None:
        """Без потоку — зведення за потоками; з потоком — список тем із кнопками перегляду."""
        if stream is None:
            groups = self.grouped(lambda i: i.get("status") == "proposed")
            if not groups:
                self.send("💡 Нерозглянутих ідей немає. Нові: /ideas")
                return
            total = sum(len(ideas) for _, ideas in groups)
            rows = [[(f"{stream_label(s)} ({len(ideas)})", f"prop:{s or '-'}")] for s, ideas in groups]
            self.send(f"💡 Нерозглянуті ідеї: {total}. Оберіть потік, щоб переглянути, погодити чи відхилити:",
                      kb(*rows), {"kind": "menu"})
            return
        ideas = self.proposed("" if stream == "-" else stream)
        if not ideas:
            self.send(f"💡 У потоці «{stream_label(stream)}» нерозглянутих ідей немає.")
            return
        lines = [f"💡 Нерозглянуті · {stream_label(stream)}: {len(ideas)}\nНатисніть номер — покажу ідею з кнопками ✅/❌."]
        buttons = []
        for n, idea in enumerate(ideas, 1):
            lines.append(f"{n}. {idea['title']} · {idea.get('proposed_at', '')}")
            buttons.append((str(n), f"propshow:{idea['id']}"))
        rows = chunks(buttons, 5) + [[(f"❌ Відхилити всі {len(ideas)}", f"proprej:{stream}")]]
        self.send("\n".join(lines), kb(*rows), {"kind": "menu"})

    def idea_card(self, idea: dict) -> str:
        sources = "\n".join(f"• {s}" for s in idea.get("sources", [])[:4])
        rubrics = {r.get("id"): r for r in load_json(RUBRICS_FILE, {"rubrics": []}).get("rubrics", [])}
        rubric = rubrics.get(idea.get("rubric"))
        used_before = any(i.get("rubric") == idea.get("rubric") and i.get("proposed_at", "") < idea.get("proposed_at", "")
                          for i in self.ideas.values())
        rubric_line = (f"\n🏷 {rubric.get('name', idea['rubric'])}"
                       + (" — нова рубрика: " + rubric.get("about", "") if rubric.get("added_by") == "routine"
                          and not used_before else "")) if rubric else ""
        text = f"{stream_label(idea.get('stream'))}{rubric_line}\n💡 {idea['title']}\n\n{idea.get('summary', '')}"
        if idea.get("why_now"):
            text += f"\n\nЧому зараз: {idea['why_now']}"
        if sources:
            text += f"\n\nДжерела:\n{sources}"
        return text

    def send_idea(self, idea: dict) -> int:
        return self.send(self.idea_card(idea),
                         kb([("✅ Беру", f"idea:{idea['id']}:yes"), ("❌ Ні", f"idea:{idea['id']}:no")]),
                         {"kind": "idea", "idea": idea["id"]})

    def menu_texts(self, quiet=False):
        self.topic_menu(
            "✍️ Пишемо тред? Оберіть тему:",
            self.grouped(lambda i: i.get("status") == "approved" and i.get("thread", {}).get("status", "none") == "none"),
            "write", "✍️ Немає погоджених тем без треду. Нові ідеї: /ideas", quiet)

    def menu_publish(self, quiet=False):
        self.topic_menu(
            "📣 Публікуємо? Оберіть готовий тред:",
            self.grouped(lambda i: i.get("thread", {}).get("status") == "approved"),
            "pub", "📣 Немає готових тредів. Написати: /texts", quiet)

    def show_queue(self, channel: str = "threads"):
        """Зведення тем за стадіями: окремо треди Threads і Telegram-дописи."""
        stream_icon = {s: icon for s, (icon, _) in STREAMS.items()}
        items = self.state["approved_threads"]
        stages = [
            ("💡 Погоджені ідеї, тексту ще немає", "🗑", "drop", []),
            ("✍️ Тред пишеться", "🗑", "drop", []),
            ("👀 Чекає вашого погодження", "🗑", "drop", []),
            ("✅ Готові, без часу публікації", "🕒", "pub", []),
            ("🕒 Заплановано", "↩️", "unq", []),
        ]
        for idea in self.ideas.values():
            thread = idea.get("thread", {}).get("status", "none")
            if idea.get("test") or idea.get("status") != "approved" or thread == "posted":
                continue
            if (self.category(idea) == "T") != (channel == "tg"):
                continue
            item = items.get(idea["id"], {})
            if thread == "none":
                stages[0][3].append((idea, ""))
            elif thread in ("requested", "revise"):
                stages[1][3].append((idea, ""))
            elif thread == "ready":
                stages[2][3].append((idea, ""))
            elif thread == "approved" or (thread == "queued" and item.get("publish_at") == "pending"):
                stages[3][3].append((idea, ""))
            elif thread == "scheduled" and item.get("test_id") and item.get("main_at"):
                stages[4][3].append((idea, f"🧪→ основний {fmt_slot(parse_time(item['main_at']))}"))
            elif thread == "scheduled" and item.get("publish_at") not in (None, "pending"):
                stages[4][3].append((idea, fmt_slot(parse_time(item["publish_at"]))))
        due = {i: m for m, i, _ in self.scheduled()}
        stages[4][3].sort(key=lambda x: due.get(x[0]["id"]) or parse_time(items[x[0]["id"]]["publish_at"]))
        lines, buttons, n = ["📰 Черга Telegram-дописів" if channel == "tg" else "📋 Черга тредів Threads"], [], 0
        for title, icon, action, rows in stages:
            if not rows:
                continue
            lines.append(f"\n{title} ({len(rows)})")
            for idea, extra in rows:
                n += 1
                label = f"{extra} — " if extra and action == "unq" else ""
                suffix = f" — {extra}" if extra and action != "unq" else ""
                lines.append(f"{stream_icon.get(idea.get('stream'), '•')} {n}. {label}{idea['title']}{suffix}")
                buttons.append((str(n), f"item:{idea['id']}"))
        if n == 0:
            self.send("📰 Погоджених тем для Telegram-дописів немає. Нові ідеї — 💡 Ідеї → 📰." if channel == "tg"
                      else "📋 Погоджених тем для Threads немає. Нові ідеї — 💡 Ідеї.")
            return
        lines.append("\nНатисніть номер теми, щоб побачити, що з нею можна зробити.")
        self.send("\n".join(lines), kb(*chunks(buttons, 5)), {"kind": "menu"})

    def show_published(self) -> None:
        analytics = load_json(ANALYTICS_FILE, {"posts": {}}).get("posts", {})
        posted = [(parse_time(item["posted_at"]), idea_id, item) for idea_id, item in self.state["approved_threads"].items()
                  if item.get("posted") and item.get("posted_at")]
        if not posted:
            self.send("📤 Через бота ще нічого не опубліковано.")
            return
        stream_icon = {s: icon for s, (icon, _) in STREAMS.items()}
        lines, buttons = ["📤 Опубліковані треди (нові вгорі):"], []
        for n, (moment, idea_id, item) in enumerate(sorted(posted, reverse=True)[:20], 1):
            idea = self.ideas.get(idea_id, {"title": idea_id})
            latest = analytics.get((item.get("posted_ids") or [""])[0], {}).get("latest") or {}
            views = f" — 👁 {latest['views']}" if latest.get("views") is not None else ""
            lines.append(f"{stream_icon.get(idea.get('stream'), '•')} {n}. {fmt_slot(moment)} · {idea['title']}{views}")
            buttons.append((str(n), f"item:{idea_id}"))
        lines.append("\nНатисніть номер, щоб побачити статистику й посилання.")
        self.send("\n".join(lines), kb(*chunks(buttons, 5)), {"kind": "menu"})

    def show_item(self, idea: dict) -> None:
        """Картка теми з діями, доречними на її стадії."""
        thread = idea.get("thread", {}).get("status", "none")
        item = self.state["approved_threads"].get(idea["id"], {})
        iid = idea["id"]
        lines = [f"{stream_label(idea.get('stream'))}\n«{idea['title']}»"]
        rows = []
        if idea.get("status") == "cancelled" or thread == "cancelled":
            lines.append("🗑 Тему скасовано.")
        elif thread == "none":
            lines.append("💡 Ідея погоджена, треду ще немає.")
            rows = [[("✍️ Написати тред", f"write:{iid}")]]
        elif thread in ("requested", "revise"):
            lines.append("✍️ Тред пишеться — прийде за кілька хвилин.")
        elif thread == "ready":
            lines.append("👀 Тред написано, чекає вашого погодження.")
            rows = [[("👀 Показати тред", f"show:{iid}")]]
        elif (thread == "approved" or (thread == "queued" and item.get("publish_at") == "pending")) \
                and self.category(idea) == "P":
            lines.append("✅ Провокація готова, час публікації не обрано. Виходить без теми й без фото.")
            rows = [[("🕒 Запланувати чи опублікувати", f"pub:{iid}")]]
        elif thread == "approved" or (thread == "queued" and item.get("publish_at") == "pending"):
            lines.append("✅ Тред готовий, час публікації не обрано.")
            rows = [[("🕒 Запланувати", f"pub:{iid}")]]
        elif thread == "scheduled" and item.get("test_id") and item.get("main_at"):
            lines.append(f"🧪 Вийшов у тестовому каналі{(': ' + item['test_permalink']) if item.get('test_permalink') else '.'}"
                         f"\n🕒 В основний канал — {fmt_slot(parse_time(item['main_at']))}.")
            rows = [[("⚡ В основний зараз", f"when:{iid}:now")]]
        elif thread == "scheduled" and item.get("publish_at") not in (None, "pending"):
            lines.append(f"🕒 Заплановано на {fmt_slot(parse_time(item['publish_at']))}.")
            rows = [[("⬆️ Раніше", f"move:{iid}:up"), ("⬇️ Пізніше", f"move:{iid}:down")],
                    [("🖼 Фото й тема", f"pub:{iid}")] if self.category(idea) != "P" else [],
                    [("⚡ Опублікувати зараз", f"when:{iid}:now")],
                    [("🕒 Інший час", f"askwhen:{iid}"), ("↩️ Зняти з розкладу", f"unq:{iid}")]]
        elif thread == "posted":
            analytics = load_json(ANALYTICS_FILE, {"posts": {}}).get("posts", {})
            latest = (analytics.get((item.get("posted_ids") or [""])[0], {}).get("latest") or {})
            names = {"views": "👁 перегляди", "likes": "❤️ лайки", "replies": "💬 відповіді", "reposts": "🔁 репости"}
            stats = " · ".join(f"{names[k]} {latest[k]}" for k in names if latest.get(k) is not None)
            lines.append(f"📤 Опубліковано{(' ' + fmt_slot(parse_time(item['posted_at']))) if item.get('posted_at') else ''}."
                         + (f"\n{stats}" if stats else "")
                         + (f"\n{idea['thread'].get('permalink')}" if idea["thread"].get("permalink") else ""))
        if thread in ("ready", "approved", "queued", "scheduled") and not item.get("test_id") \
                and self.category(idea) != "P":
            count = len(idea.get("images", []))
            rows.append([(f"🔄 Інші фото ({count})", f"photos:{iid}:more") if count
                         else ("🖼 Знайти фото", f"photos:{iid}"),
                         ("🔎 Фото за словами", f"photokw:{iid}")])
        if self.format_changeable(idea):
            rows.append([("🔀 Змінити формат", f"fmt:{iid}")])
        if thread not in ("posted", "cancelled") and idea.get("status") != "cancelled":
            rows.append([("🗑 Скасувати тему", f"drop:{iid}")])
        self.send("\n".join(lines), kb(*rows) if rows else None, {"kind": "item", "idea": iid})

    def format_changeable(self, idea: dict) -> bool:
        """Формат (потік) можна змінити, поки тред не пишеться, не в розкладі й не опублікований."""
        thread = idea.get("thread", {}).get("status", "none")
        item = self.state["approved_threads"].get(idea["id"], {})
        if idea.get("status") == "cancelled" or item.get("posted_ids") or item.get("test_id"):
            return False
        return thread in ("none", "ready", "approved") or (thread == "queued" and item.get("publish_at") == "pending")

    def ask_format(self, idea: dict) -> None:
        thread = idea.get("thread", {}).get("status", "none")
        if thread in ("requested", "revise"):
            self.send(f"✍️ «{idea['title']}» зараз пишеться. Змінити формат можна, щойно текст прийде.")
            return
        if thread == "scheduled":
            self.send(f"🕒 «{idea['title']}» уже в розкладі. Щоб змінити формат, спершу зніміть тред з розкладу "
                      "(↩️ у картці теми).")
            return
        if not self.format_changeable(idea):
            self.send(f"Формат «{idea['title']}» вже не змінити: тред опубліковано або тему скасовано.")
            return
        rows = [[(stream_label(s), f"fmtset:{idea['id']}:{s}")] for s in STREAMS if s != idea.get("stream")]
        rewrite = "" if thread == "none" else "\nТред буде переписано під новий формат, і його треба буде погодити ще раз."
        self.send(f"🔀 Зараз: {stream_label(idea.get('stream'))}. На який формат змінити «{idea['title']}»?{rewrite}",
                  kb(*rows, [("❌ Скасувати", "cancel")]), {"kind": "format", "idea": idea["id"]})

    def change_format(self, idea: dict, stream: str) -> str:
        """Міняє потік ідеї; написаний тред іде на переписування під новий формат. Повертає підпис кнопки."""
        old = idea.get("stream")
        if stream not in STREAMS or stream == old:
            return "Формат не змінено"
        if not self.format_changeable(idea):
            return "Формат уже не змінити"
        thread = idea.setdefault("thread", {"status": "none"})
        idea["stream"] = stream
        history(idea, f"stream changed in telegram: {old} → {stream}")
        if thread.get("status", "none") == "none":
            self.send(f"🔀 «{idea['title']}»: тепер {stream_label(stream)}.",
                      kb([("✍️ Написати тред", f"write:{idea['id']}")]), {"kind": "item", "idea": idea["id"]})
            return f"✅ {stream_label(stream)}"
        # Попередній текст більше не актуальний: знімаємо його кнопки й чергу без часу.
        for key, message_id in list(self.state["sent"].items()):
            if key.startswith(f"thread:{idea['id']}:") and message_id:
                try:
                    self.close_buttons(message_id, "🔀 Формат змінено")
                except SystemExit:
                    pass
        self.state["approved_threads"].pop(idea["id"], None)
        thread.pop("approved_sha", None)
        thread["status"] = "revise"
        idea.setdefault("revisions", []).append({
            "kind": "format", "at": now(),
            "text": f"Автор змінив формат з «{stream_label(old)}» на «{stream_label(stream)}». "
                    f"Перепиши тред повністю з нуля за правилами потоку `{stream}`."})
        self.fire.setdefault("texts", set()).add(idea["id"])
        self.send(f"🔀 «{idea['title']}»: тепер {stream_label(stream)}. Переписую тред — нова версія прийде сюди.")
        return f"✅ {stream_label(stream)}"

    def request_photos(self, idea: dict, more: bool = False, keywords: str = "") -> None:
        """Запит на пошук фото; «інші» й пошук за словами виключають уже показані фото."""
        requested = idea.get("photo_requested_at")
        if idea.get("photo_request") and requested and \
                datetime.now(timezone.utc) - parse_time(requested) < STALE_AFTER:
            self.send(f"🔎 Фото для «{idea['title']}» вже шукаються — надішлю, щойно знайду.")
            return
        if more or keywords:
            excluded = idea.setdefault("images_excluded", [])
            excluded += [i.get("url") for i in idea.get("images", []) if i.get("url") and i.get("url") not in excluded]
        idea["photo_request"] = True
        idea["photo_error"] = ""
        idea["photo_keywords"] = keywords
        idea["photo_requested_at"] = now()
        history(idea, f"photo search requested in telegram{': ' + keywords if keywords else ' (more)' if more else ''}")
        self.fire.setdefault("texts", set()).add(idea["id"])
        what = f" за словами «{keywords}»" if keywords else (" (інші)" if more else "")
        self.send(f"🔎 Шукаю фото{what} для «{idea['title']}» — надішлю, щойно знайду.")

    def rule_warning(self, idea_id: str, moment: datetime) -> str:
        """Порожній рядок, якщо слот відповідає правилам розкладу, інакше — попередження."""
        pubs = self.bot_publications(exclude=idea_id)
        cat = self.category(self.ideas.get(idea_id))
        if not self.slot_allowed(moment.astimezone(KYIV), cat, pubs, strict=False):
            return "⚠️ Порушує правила розкладу (ліміт на день, 1 серйозний на день або проміжок 4 год)."
        if not self.slot_allowed(moment.astimezone(KYIV), cat, pubs, strict=True):
            return "ℹ️ Порушено чергування серйозних і жартівливих."
        return ""

    def move_item(self, idea: dict, direction: str) -> None:
        """Переносить заплановану тему на сусідній рекомендований слот; зайнятий — обмін."""
        item = self.state["approved_threads"].get(idea["id"], {})
        if item.get("posted") or item.get("posted_ids") or item.get("test_id") or item.get("publish_at") in (None, "pending"):
            return
        current = parse_time(item["publish_at"])
        earliest = datetime.now(KYIV) + timedelta(minutes=15)
        source = self.tg_slots() if self.category(idea) == "T" else self.recommended_slots(days=21)
        slots = [s for s in source if s >= earliest]
        if direction == "down":
            target = next((s for s in slots if s > current + timedelta(minutes=1)), None)
        else:
            earlier = [s for s in slots if s < current - timedelta(minutes=1)]
            target = earlier[-1] if earlier else None
        if target is None:
            self.send("Раніших рекомендованих слотів немає." if direction == "up" else "Пізніших слотів немає.")
            return
        other = next((i for m, i, _ in self.scheduled() if i != idea["id"] and abs(m - target) < timedelta(minutes=1)), None)
        item["publish_at"] = target.isoformat()
        history(idea, f"thread moved to {item['publish_at']}")
        notes = [f"🕒 «{idea['title']}» → {fmt_slot(target)}."]
        if other:
            other_item = self.state["approved_threads"][other]
            other_item["publish_at"] = current.isoformat()
            if other in self.ideas:
                history(self.ideas[other], f"thread swapped to {other_item['publish_at']}")
            notes.append(f"🔁 Обмін: «{self.ideas.get(other, {}).get('title', other)}» → {fmt_slot(current)}.")
        for idea_id, moment in [(idea["id"], target)] + ([(other, current)] if other else []):
            if self.category(self.ideas.get(idea_id)) == "T":
                continue
            warning = self.rule_warning(idea_id, moment)
            if warning:
                notes.append(f"{warning} («{self.ideas.get(idea_id, {}).get('title', idea_id)}»)")
        self.send("\n".join(notes))
        self.show_item(idea)

    def request_ideas(self, stream: str):
        pending = [r for r in self.requests["ideas"] if r.get("status") == "pending"]
        if pending:
            self.send(f"🔎 Уже шукаю ідеї ({stream_label(pending[-1]['stream'])}) — надішлю, щойно будуть готові.")
            return
        self.requests["ideas"].append({"stream": stream, "requested_at": now(), "status": "pending"})
        self.fire.setdefault("ideas", set()).add(stream)
        self.send(f"🔎 Шукаю 10 ідей: {stream_label(stream)}. Це займе близько 10 хвилин.")

    def show_photos(self, idea: dict) -> list[int]:
        """Показує кандидатів на фото один раз; повертає номери тих, що Telegram зміг відкрити."""
        shown = self.state.setdefault("photos_shown", {})
        key = f"{idea['id']}:{sha(json.dumps([i.get('url') for i in idea.get('images', [])[:3]]))}"
        if key not in shown:
            ok = []
            for n, image in enumerate(idea.get("images", [])[:3], 1):
                caption = f"🖼 {n} · {image.get('credit', '')}"[:1000]
                if tg("sendPhoto", chat_id=CHAT_ID, photo=image.get("url", ""), caption=caption):
                    ok.append(n)
            shown[key] = ok
        return shown[key]

    def send_text_parts(self, idea: dict, body: str, info: str, markup, record: dict) -> int:
        """Текст, джерела й пояснення — окремими повідомленнями, щоб було видно, чи текст влазить
        в одне повідомлення Telegram. Запис (для правок відповіддю) — на всіх, кнопки — на поясненні."""
        if self.category(idea) == "T":
            text = tg_post_text(body)
        else:
            text = "\n---\n".join(thread_posts(body))
        # Текст — до повної межі повідомлення Telegram (4096), щоб його поділ означав «не влазить».
        self.send(text or body, None, record, limit=TG_POST_LIMIT)
        sources = sources_block(body)
        if sources:
            self.send(sources, None, record)
        return self.send(info, markup, record)

    def send_preview(self, idea: dict, body: str, topic: str) -> None:
        digest = sha(body)
        photos = self.show_photos(idea) if self.category(idea) != "P" else []
        if self.category(idea) == "T":
            rows = [[(f"🕒 Запланувати з фото {n}", f"confirm:{idea['id']}:{digest}:{n}")] for n in photos]
            rows.append([("🕒 Запланувати без фото", f"confirm:{idea['id']}:{digest}:0")])
            rows.append([("🔄 Інші фото", f"photos:{idea['id']}:more") if photos else ("🖼 Знайти фото", f"photos:{idea['id']}"),
                         ("🔎 Фото за словами", f"photokw:{idea['id']}")])
            rows.append([("❌ Скасувати", "cancel")])
            size = len(tg_post_text(body))
            self.send_text_parts(idea, body,
                      f"{stream_label(idea.get('stream'))} · фінальна перевірка — «{idea['title']}»"
                      f"\n\nℹ️ Текст допису — у повідомленні вище{' — і він не вліз в одне повідомлення Telegram!' if size > TG_POST_LIMIT else ''}; "
                      f"блок «Джерела» в канал не публікується. Розмір допису: {size} з {TG_POST_LIMIT} символів."
                      + (f"\n📷 Фото над текстом: варіанти {', '.join(map(str, photos))} вище." if photos else "")
                      + "\n🕒 Після вибору фото допис стане на найближчий вільний вівторок: о 16:00 — тестовий канал, "
                        "потім у випадковий час 17:00–19:00 — основний."
                      "\n\n" + edit_hint("ширші зміни — у повідомленні з текстом і кнопкою «✅ Готово»."),
                      kb(*rows), {"kind": "preview", "idea": idea["id"], "sha": digest, "text": body, "topic": ""})
            return
        if self.category(idea) == "P":
            self.send_text_parts(idea, body,
                      f"{stream_label(idea.get('stream'))} · фінальна перевірка — «{idea['title']}»"
                      "\n\nℹ️ Текст — у повідомленні вище; блок «Джерела» в Threads не публікується. Провокація виходить без теми й без фото."
                      "\n🕒 «Запланувати» — на найближчу вільну рекомендовану годину, за тими самими правилами, що й треди "
                      "(одна публікація на день)."
                      "\n\n" + edit_hint("ширші зміни — у повідомленні з текстом і кнопкою «✅ Готово»."),
                      kb([("🕒 Запланувати", f"confirm:{idea['id']}:{digest}:0")],
                         [("⚡ Опублікувати зараз", f"confirm:{idea['id']}:{digest}:now")], [("❌ Не зараз", "cancel")]),
                      {"kind": "preview", "idea": idea["id"], "sha": digest, "text": body, "topic": ""})
            return
        topic_line = (f"🏷 Тема поста: {topic}" if topic else "🏷 Тема поста не обрана — оберіть кнопкою нижче.")
        photo_line = (f"\n📷 Фото до першого поста: варіанти {', '.join(map(str, photos))} вище." if photos else "")
        rows = []
        if topic:
            rows = [[(f"🕒 Запланувати з фото {n}", f"confirm:{idea['id']}:{digest}:{n}")] for n in photos]
            rows.append([("🕒 Запланувати без фото", f"confirm:{idea['id']}:{digest}:0")])
            rows.append([("🏷 Змінити тему", f"tagask:{idea['id']}")])
        else:
            rows.append([(f"🏷 {DEFAULT_TAG}", f"tag:{idea['id']}:std"), ("✏️ Своя тема", f"tag:{idea['id']}:own")])
        rows.append([("🔄 Інші фото", f"photos:{idea['id']}:more") if photos else ("🖼 Знайти фото", f"photos:{idea['id']}"),
                     ("🔎 Фото за словами", f"photokw:{idea['id']}")])
        rows.append([("❌ Скасувати", "cancel")])
        self.send_text_parts(idea, body,
                  f"{stream_label(idea.get('stream'))} · фінальна перевірка — «{idea['title']}»"
                  f"\n\nℹ️ Тред — у повідомленні вище; блок «Джерела» в Threads не публікується.{photo_line}\n{topic_line}\n"
                  "🕒 Після вибору фото тред автоматично стане на найближчу вільну рекомендовану годину."
                  "\n\n" + edit_hint("звичайна відповідь без цитати й «→» — нова тема поста."),
                  kb(*rows),
                  {"kind": "preview", "idea": idea["id"], "sha": digest, "text": body, "topic": topic})

    def apply_topic(self, idea: dict, topic: str) -> None:
        topic = topic.lstrip("#").strip()
        if not (1 <= len(topic) <= 50) or "." in topic or "&" in topic:
            self.state["awaiting"] = {"kind": "tag", "idea": idea["id"], "at": now()}
            self.send("⚠️ Тема має бути до 50 символів, без крапок і «&». Напишіть іншу.")
            return
        self.state["last_topic"] = topic
        body = self.thread_text(idea)
        if body and idea.get("thread", {}).get("status") in ("approved", "queued", "scheduled"):
            self.send_preview(idea, body, topic)

    def propose_topic(self, text: str) -> None:
        pid = sha(text + now())[:8]
        self.state.setdefault("proposals", {})[pid] = {"text": text, "at": now()}
        rows = [[(stream_label(s), f"own:{pid}:{s}")] for s in STREAMS] + [[("❌ Скасувати", "cancel")]]
        self.send(f"💡 Ваша тема: «{text}»\nОберіть потік — і я одразу почну писати тред:", kb(*rows), {"kind": "own"})

    def create_own_idea(self, pid: str, stream: str) -> None:
        proposal = self.state.get("proposals", {}).pop(pid, None)
        if proposal is None or stream not in STREAMS:
            return
        today = datetime.now(KYIV).date().isoformat()
        n = 1 + sum(1 for i in self.ideas if i.startswith(f"{today}-a"))
        text = proposal["text"]
        idea = {
            "id": f"{today}-a{n:02d}", "proposed_at": today, "stream": stream, "origin": "author",
            "title": text if len(text) <= 80 else text[:79] + "…", "summary": text, "why_now": "",
            "sources": [], "images": [], "status": "approved",
            "thread": {"status": "requested", "file": None}, "script": {"status": "none", "file": None},
            "video": {"status": "none", "telegram_message_id": None}, "revisions": [],
            "history": [{"at": now(), "event": "author topic from telegram; thread requested"}],
        }
        self.ideas_doc["ideas"].append(idea)
        self.ideas[idea["id"]] = idea
        self.fire.setdefault("texts", set()).add(idea["id"])
        self.send(f"✍️ Пишу тред ({stream_label(stream)}): «{idea['title']}». Надішлю, щойно буде готовий.")

    @staticmethod
    def day_slot(day, prev: datetime | None, window: dict, min_shift: int) -> datetime:
        """Стабільний для дня час у вікні, що відрізняється від попереднього дня ≥ min_shift хв."""
        start_h, start_m = map(int, window["start"].split(":"))
        end_h, end_m = map(int, window["end"].split(":"))
        first, last = start_h * 60 + start_m, end_h * 60 + end_m
        minutes = list(range(first, last + 1))
        if prev is not None:
            prev_local = prev.astimezone(KYIV)
            prev_minute = prev_local.hour * 60 + prev_local.minute
            minutes = [m for m in minutes if abs(m - prev_minute) >= min_shift] or minutes
        pick = minutes[int(hashlib.sha256(day.isoformat().encode()).hexdigest(), 16) % len(minutes)]
        return datetime(day.year, day.month, day.day, pick // 60, pick % 60, tzinfo=KYIV)

    def recommended_slots(self, days: int = 7) -> list[datetime]:
        recs = load_json(RECS_FILE, {"slots": []})
        start = datetime.now(KYIV)
        window = recs.get("window")
        if window:
            # Публікації бота, що вже стоять чи вийшли, задають «учорашній» час для наступного дня.
            actual = {}
            for moment, _ in self.bot_publications():
                actual.setdefault(moment.astimezone(KYIV).date(), moment.astimezone(KYIV))
            result, prev = [], None
            for offset in range(-1, days + 1):
                day = (start + timedelta(days=offset)).date()
                chosen = actual.get(day) or self.day_slot(day, prev, window, recs.get("min_shift_minutes", 45))
                if offset >= 0 and chosen > start + timedelta(minutes=15):
                    result.append(chosen)
                prev = chosen
            return result
        result = []
        for offset in range(days + 1):
            day = (start + timedelta(days=offset)).date()
            for slot in recs.get("slots", []):
                if slot.get("weekday") == day.weekday():
                    moment = datetime(day.year, day.month, day.day, slot.get("hour", 9), slot.get("minute", 0), tzinfo=KYIV)
                    if moment > start + timedelta(minutes=15):
                        result.append(moment)
        return sorted(result)

    def scheduled(self) -> list[tuple[datetime, str, dict]]:
        items = []
        for idea_id, item in self.state["approved_threads"].items():
            at = item.get("main_at") if item.get("test_id") else item.get("publish_at")
            if not item.get("posted") and not item.get("failed") and at and at != "pending":
                items.append((parse_time(at), idea_id, item))
        return sorted(items, key=lambda x: x[0])

    @staticmethod
    def category(idea: dict | None) -> str:
        """«S» — серйозний тред, «F» — жартівливий, «P» — провокація, «T» — Telegram-допис."""
        stream = (idea or {}).get("stream")
        return {"tg": "T", "fun": "F", "provoke": "P"}.get(stream, "S")

    def bot_publications(self, exclude: str | None = None, channel: str = "threads") -> list[tuple[datetime, str]]:
        """Заплановані й уже опубліковані публікації бота: (момент, категорія).
        channel="threads" — треди (S/F/P), channel="tg" — Telegram-дописи (T)."""
        result = []
        for idea_id, item in self.state["approved_threads"].items():
            if idea_id == exclude or item.get("failed"):
                continue
            cat = self.category(self.ideas.get(idea_id))
            if (cat == "T") != (channel == "tg"):
                continue
            at = item.get("posted_at") if item.get("posted") else item.get("publish_at")
            if at and at != "pending":
                result.append((parse_time(at), cat))
        return sorted(result)

    def tg_slots(self, days: int = 120) -> list[datetime]:
        start = datetime.now(KYIV)
        result = []
        for offset in range(days + 1):
            day = (start + timedelta(days=offset)).date()
            if day.weekday() == TG_SLOT[0]:
                moment = datetime(day.year, day.month, day.day, TG_SLOT[1], TG_SLOT[2], tzinfo=KYIV)
                if moment > start + timedelta(minutes=15):
                    result.append(moment)
        return result

    def slot_allowed(self, slot: datetime, cat: str, pubs, strict: bool) -> bool:
        if any(abs(slot - moment) < SLOT_GAP for moment, _ in pubs):
            return False
        same_day = [c for moment, c in pubs if moment.astimezone(KYIV).date() == slot.date()]
        if len(same_day) >= DAILY_LIMIT or (cat == "S" and "S" in same_day):
            return False
        if strict:  # серйозні й жартівливі — по черзі
            before = [c for moment, c in pubs if moment < slot]
            after = [c for moment, c in pubs if moment > slot]
            if (before and before[-1] == cat) or (after and after[0] == cat):
                return False
        return True

    def retime_scheduled(self) -> list[tuple[str, datetime]]:
        """Ставить заплановані (ще не розпочаті) треди на час вікна в межах їхніх днів."""
        moved = []
        future = sorted((m, i) for m, i, item in self.scheduled()
                        if m > datetime.now(KYIV) + timedelta(minutes=15) and not item.get("posted_ids")
                        and self.category(self.ideas.get(i)) != "T")
        for moment, idea_id in future:
            item = self.state["approved_threads"][idea_id]
            item["publish_at"] = "pending"
            day = moment.astimezone(KYIV).date()
            slot = next((s for s in self.recommended_slots(days=40) if s.date() == day), None)
            item["publish_at"] = (slot or moment).isoformat()
            if slot and slot != moment and idea_id in self.ideas:
                history(self.ideas[idea_id], f"retimed to window: {item['publish_at']}")
                moved.append((idea_id, slot))
        return moved

    def free_slots(self, count: int = 3, idea: dict | None = None) -> list[datetime]:
        """Рекомендовані слоти за правилами: ліміт на день, проміжок, чергування категорій.
        Чергування поступається лише тоді, коли в найближчі дні підходящого слоту немає."""
        cat = self.category(idea)
        if cat == "T":
            taken = {m.astimezone(KYIV).date() for m, _ in self.bot_publications((idea or {}).get("id"), "tg")}
            return [s for s in self.tg_slots() if s.date() not in taken][:count]
        pubs = self.bot_publications(exclude=(idea or {}).get("id"))
        slots = self.recommended_slots(days=60)
        horizon = datetime.now(KYIV) + ALTERNATION_HORIZON
        strict = [s for s in slots if s <= horizon and self.slot_allowed(s, cat, pubs, strict=True)]
        loose = [s for s in slots if s not in strict and self.slot_allowed(s, cat, pubs, strict=False)]
        return sorted(strict)[:count] or sorted(loose)[:count]

    def auto_schedule(self, idea: dict) -> None:
        """Ставить тред на найближчу вільну рекомендовану годину; змінити можна кнопками."""
        item = self.state["approved_threads"].get(idea["id"])
        slots = self.free_slots(1, idea)
        if not item or not slots:
            self.ask_when(idea)
            return
        item["publish_at"] = slots[0].isoformat()
        idea["thread"]["status"] = "scheduled"
        history(idea, f"thread auto-scheduled: {item['publish_at']}")
        self.send(f"🕒 «{idea['title']}» заплановано на {fmt_slot(slots[0])} — найближчу вільну рекомендовану годину.",
                  kb([("⚡ Опублікувати зараз", f"when:{idea['id']}:now")],
                     [("🕒 Інший час", f"askwhen:{idea['id']}"), ("↩️ Зняти з розкладу", f"unq:{idea['id']}")]),
                  {"kind": "when"})

    def ask_when(self, idea: dict) -> None:
        rows = [[(f"🕒 {fmt_slot(s)}", f"when:{idea['id']}:{int(s.timestamp())}")] for s in self.free_slots(3, idea)]
        rows += [[("⚡ Опублікувати зараз", f"when:{idea['id']}:now")], [("↩️ Не публікувати", f"unq:{idea['id']}")]]
        self.send(f"🕒 Коли публікуємо «{idea['title']}»?\nЗапропоновано найближчі рекомендовані години за правилами розкладу "
                  "(одна публікація на день, серйозні й жартівливі по черзі).",
                  kb(*rows), {"kind": "when"})

    # ------------------------------------------------------------ вхідні

    def on_callback(self, cq: dict) -> None:
        message = cq.get("message") or {}
        mid = message.get("message_id")
        if message.get("chat", {}).get("id") != CHAT_ID or cq.get("from", {}).get("id") != CHAT_ID:
            return
        data = cq.get("data", "")
        tg("answerCallbackQuery", callback_query_id=cq["id"])
        record = self.state["messages"].get(str(mid))
        if data == "noop" or record is None:
            return
        parts = data.split(":")
        action = parts[0]

        if action == "ideas" and parts[1] == "review":
            self.close_buttons(mid, "💡 Нерозглянуті ідеї")
            self.show_proposed()
        elif action == "prop":
            self.show_proposed(parts[1])
        elif action == "propshow":
            idea = self.ideas.get(parts[1])
            if idea and idea.get("status") == "proposed":
                self.send_idea(idea)
            elif idea:
                self.send(f"«{idea['title']}» вже розглянуто.")
        elif action == "proprej":
            count = len(self.proposed("" if parts[1] == "-" else parts[1]))
            if count:
                self.send(f"❌ Відхилити всі нерозглянуті ідеї потоку «{stream_label(parts[1])}» ({count})?\n"
                          "Вони лишаться в історії й не пропонуватимуться знову.",
                          kb([("Так, відхилити", f"proprejyes:{parts[1]}"), ("Ні", "cancel")]), {"kind": "confirm"})
        elif action == "proprejyes":
            ideas = self.proposed("" if parts[1] == "-" else parts[1])
            for idea in ideas:
                idea["status"] = "rejected"
                history(idea, "idea rejected in telegram (bulk)")
            self.close_buttons(mid, f"❌ Відхилено: {len(ideas)}")
        elif action == "ideas":
            if parts[1] in STREAMS:
                self.close_buttons(mid, f"✅ {stream_label(parts[1])}")
                self.request_ideas(parts[1])
            else:
                self.close_buttons(mid, "Добре, іншим разом")
        elif action == "go":
            self.close_buttons(mid, "➡️")
            self.menu_texts() if parts[1] == "texts" else self.menu_publish()
        elif action == "dash":  # кнопки закріпленого дашборда — самі кнопки лишаються
            if parts[1] == "refresh":
                self.refresh_dashboard(force=True)
            elif parts[1] == "publish":
                self.menu_publish()
            elif parts[1] == "proposed":
                self.show_proposed()
            elif parts[1] in ("queue", "tgqueue"):
                self.show_queue("tg" if parts[1] == "tgqueue" else "threads")
        elif action == "idea":
            idea = self.ideas.get(parts[1])
            if idea and idea.get("status") == "proposed":
                idea["status"] = "approved" if parts[2] == "yes" else "rejected"
                history(idea, f"idea {idea['status']} in telegram")
            status = (idea or {}).get("status")
            self.close_buttons(mid, {"approved": "✅ Погоджено", "rejected": "❌ Відхилено"}.get(status, "Тему вже розглянуто"))
        elif action == "write":
            idea = self.ideas.get(parts[1])
            if idea and idea.get("thread", {}).get("status", "none") == "none":
                idea.setdefault("thread", {})["status"] = "requested"
                history(idea, "thread requested in telegram")
                self.fire.setdefault("texts", set()).add(idea["id"])
                self.send(f"✍️ Пишу тред ({stream_label(idea.get('stream'))}): «{idea['title']}». Надішлю, щойно буде готовий.")
        elif action in ("done", "thread"):
            # «✅ Готово» під тредом (і кнопка старого формату «thread:<id>:yes»).
            idea = self.ideas.get(parts[1])
            marks = unverified(self.thread_text(idea) or "") if idea else 0
            if marks:
                self.send(f"⚠️ У треді є позначки [ПЕРЕВІРИТИ] ({marks}). {UNVERIFIED_HINT}", reply_to=mid)
                return
            if idea and idea.get("thread", {}).get("status") in ("ready", "revise"):
                idea["thread"]["status"] = "approved"
                idea["thread"]["approved_sha"] = record.get("sha")
                history(idea, "thread text approved in telegram")
            self.close_buttons(mid, "✅ Тред готовий")
            body = self.thread_text(idea) if idea else None
            if body and idea.get("thread", {}).get("status") == "approved":
                self.send_preview(idea, body, self.state.get("last_topic", ""))
        elif action == "pub":
            idea = self.ideas.get(parts[1])
            body = self.thread_text(idea) if idea else None
            if body is None or idea.get("thread", {}).get("status") not in ("approved", "queued", "scheduled"):
                return
            if unverified(body):
                idea["thread"]["status"] = "ready"
                self.send(f"⚠️ «{idea['title']}»: у треді є [ПЕРЕВІРИТИ], не публікую. {UNVERIFIED_HINT}")
                return
            self.send_preview(idea, body, self.state.get("last_topic", ""))
        elif action == "confirm":
            idea, digest = self.ideas.get(parts[1]), parts[2]
            if not idea or record.get("sha") != digest or \
                    idea.get("thread", {}).get("status") not in ("approved", "queued", "scheduled"):
                return
            previous = self.state["approved_threads"].get(idea["id"], {})
            if previous.get("posted") or previous.get("posted_ids") or previous.get("test_id"):
                return
            keep_time = previous.get("publish_at") if previous.get("publish_at") not in (None, "pending") else None
            # У чергу йде саме той текст, який користувач щойно бачив.
            photo = int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else 0
            images = idea.get("images", [])
            image = images[photo - 1] if 0 < photo <= len(images) else None
            self.state["approved_threads"][idea["id"]] = {"sha": digest, "text": record["text"],
                                                          "topic": record.get("topic", ""),
                                                          "image": ({"url": image.get("url"), "credit": image.get("credit", "")}
                                                                    if image else None),
                                                          "approved_at": now(), "posted": False,
                                                          "publish_at": "pending"}
            if self.category(idea) == "P":  # провокація — завжди без теми й фото
                self.state["approved_threads"][idea["id"]].update(topic="", image=None)
                if len(parts) > 3 and parts[3] == "now":
                    self.state["approved_threads"][idea["id"]]["publish_at"] = now()
                    idea["thread"]["status"] = "scheduled"
                    history(idea, "provocation published manually")
                    self.mark_publishing(self.state["approved_threads"][idea["id"]], mid)
                    return
            idea["thread"]["status"] = "queued"
            history(idea, "thread approved for Threads, awaiting time")
            self.close_buttons(mid, "✅ Текст обрано" if self.category(idea) == "P" else "✅ Текст і фото обрано")
            if keep_time:
                self.state["approved_threads"][idea["id"]]["publish_at"] = keep_time
                idea["thread"]["status"] = "scheduled"
                self.send(f"🖼 Фото й тему оновлено, час без змін: {fmt_slot(parse_time(keep_time))}.")
            else:
                self.auto_schedule(idea)
        elif action == "askwhen":
            idea = self.ideas.get(parts[1])
            if idea and idea["id"] in self.state["approved_threads"]:
                self.ask_when(idea)
        elif action == "when":
            idea, item = self.ideas.get(parts[1]), self.state["approved_threads"].get(parts[1])
            if not idea or not item or item.get("posted") or item.get("posted_ids"):
                return
            if item.get("test_id"):  # уже в тестовому каналі: можна лише прискорити основний
                if parts[2] == "now":
                    item["main_at"] = now()
                    history(idea, "telegram main channel: publish now")
                    self.mark_publishing(item, mid, "⏳ Публікується в основний канал…")
                return
            if parts[2] == "now":
                item["publish_at"] = now()
                label = "⏳ Публікується…"
            else:
                item["publish_at"] = datetime.fromtimestamp(int(parts[2]), KYIV).isoformat()
                label = f"🕒 {fmt_slot(parse_time(item['publish_at']))}"
            idea["thread"]["status"] = "scheduled"
            history(idea, f"thread scheduled: {item['publish_at']}")
            if parts[2] == "now":
                self.mark_publishing(item, mid, "⏳ Публікується в тестовий канал…" if self.category(idea) == "T" else label)
            else:
                self.close_buttons(mid, label)
            if parts[2] != "now":
                self.send(f"🕒 «{idea['title']}» заплановано на {label[2:]}. Змінити чи зняти — у 📋 Черга.")
        elif action == "unq":
            idea, item = self.ideas.get(parts[1]), self.state["approved_threads"].get(parts[1])
            if not idea or not item or item.get("posted") or item.get("posted_ids") or item.get("test_id"):
                return
            self.state["approved_threads"].pop(idea["id"])
            idea["thread"]["status"] = "approved"
            history(idea, "thread unscheduled")
            self.close_buttons(mid, "↩️ Знято з черги")
            self.send(f"↩️ «{idea['title']}» знято з розкладу — тред серед готових без часу (📣 Публікація).")
        elif action == "item":
            idea = self.ideas.get(parts[1])
            if idea:
                self.show_item(idea)
        elif action == "photos":
            idea = self.ideas.get(parts[1])
            if idea:
                self.request_photos(idea, more=len(parts) > 2 and parts[2] == "more")
        elif action == "tagask":
            idea = self.ideas.get(parts[1])
            if idea:
                self.send(f"🏷 Тема поста для «{idea['title']}»:",
                          kb([(f"🏷 {DEFAULT_TAG}", f"tag:{idea['id']}:std"), ("✏️ Своя тема", f"tag:{idea['id']}:own")]),
                          {"kind": "tagask", "idea": idea["id"]})
        elif action == "tag":
            idea = self.ideas.get(parts[1])
            if not idea:
                return
            if parts[2] == "std":
                self.close_buttons(mid, f"🏷 {DEFAULT_TAG}")
                self.apply_topic(idea, DEFAULT_TAG)
            else:
                self.close_buttons(mid, "✏️ Своя тема")
                self.state["awaiting"] = {"kind": "tag", "idea": idea["id"], "at": now()}
                self.send("✏️ Напишіть тему поста наступним повідомленням (до 50 символів, без # і крапок).")
        elif action == "photokw":
            idea = self.ideas.get(parts[1])
            if idea:
                self.state["awaiting"] = {"kind": "photokw", "idea": idea["id"], "at": now()}
                self.send(f"🔎 Напишіть наступним повідомленням ключові слова для пошуку фото до «{idea['title']}» "
                          "(напр. «метрична книга 1850 Київська губернія»).",
                          None, {"kind": "photokw", "idea": idea["id"]})
        elif action == "recs":
            recs = load_json(RECS_FILE, {})
            if recs.get("proposal_id") != parts[2] or not recs.get("proposed_window"):
                self.close_buttons(mid, "Пропозиція вже неактуальна")
                return
            approved = parts[1] == "yes"
            if approved:
                recs["window"] = recs["proposed_window"]
                recs["basis"] = "account (погоджено автором)"
                recs["updated_at"] = datetime.now(KYIV).date().isoformat()
                self.close_buttons(mid, "✅ Новий діапазон погоджено")
            else:
                recs.setdefault("rejected_proposals", []).append(
                    {"id": parts[2], "window": recs["proposed_window"], "at": now()})
                self.close_buttons(mid, "❌ Лишаємо як є")
            for key in ("proposed_window", "proposal_reason", "proposal_id"):
                recs.pop(key, None)
            save_json(RECS_FILE, recs)
            if approved:
                moved = self.retime_scheduled()
                lines = [f"• {fmt_slot(m)} — {self.ideas[i]['title']}" for i, m in moved]
                self.send("✅ Новий діапазон діє." + ("\nЧас запланованих тредів перераховано:\n" + "\n".join(lines)
                                                    if lines else ""))
        elif action == "move":
            idea = self.ideas.get(parts[1])
            if idea:
                self.move_item(idea, parts[2])
        elif action == "show":
            idea = self.ideas.get(parts[1])
            body = self.thread_text(idea) if idea else None
            if body and idea.get("thread", {}).get("status") in ("ready", "revise"):
                self.send_thread(idea, body)
        elif action == "own":
            self.close_buttons(mid, f"✅ {stream_label(parts[2])}")
            self.create_own_idea(parts[1], parts[2])
        elif action == "drop":
            idea = self.ideas.get(parts[1])
            if idea:
                self.send(f"🗑 Скасувати тему «{idea['title']}»?\nТред не буде опубліковано, "
                          "тема зникне з меню й більше не пропонуватиметься.",
                          kb([("Так, скасувати", f"dropyes:{idea['id']}"), ("Ні", "cancel")]), {"kind": "confirm"})
        elif action == "dropyes":
            idea = self.ideas.get(parts[1])
            if not idea:
                return
            if idea.get("thread", {}).get("status") == "posted" or \
                    self.state["approved_threads"].get(idea["id"], {}).get("posted_ids"):
                self.close_buttons(mid, "Вже опубліковано")
                return
            idea["status"] = "cancelled"
            idea.setdefault("thread", {})["status"] = "cancelled"
            self.state["approved_threads"].pop(idea["id"], None)
            history(idea, "cancelled by author in telegram")
            self.close_buttons(mid, "🗑 Тему скасовано")
        elif action == "fmt":
            idea = self.ideas.get(parts[1])
            if idea:
                self.ask_format(idea)
        elif action == "fmtset":
            idea = self.ideas.get(parts[1])
            if idea:
                self.close_buttons(mid, self.change_format(idea, parts[2]))
        elif action == "cancel":
            self.close_buttons(mid, "Скасовано")

    def on_message(self, msg: dict) -> None:
        if msg.get("chat", {}).get("id") != CHAT_ID:
            return
        text = (msg.get("text") or "").strip()
        text = MENU_BUTTONS.get(text, text)
        command = text.split()[0].split("@")[0] if text.startswith("/") else ""
        if command:
            self.state.pop("awaiting", None)
        if command == "/ideas":
            return self.ask_ideas()
        if command in ("/texts", "/text"):
            return self.menu_texts()
        if command == "/publish":
            return self.menu_publish()
        if command == "/queue":
            return self.show_queue()
        if command == "/tgqueue":
            return self.show_queue("tg")
        if command == "/published":
            return self.show_published()
        if command == "/proposed":
            return self.show_proposed()
        if command == "/dashboard":
            return self.refresh_dashboard(repost=True)
        if command in ("/start", "/help"):
            return self.send(HELP, MAIN_KEYBOARD)
        if command:
            return

        reply_id = (msg.get("reply_to_message") or {}).get("message_id")
        record = self.state["messages"].get(str(reply_id)) if reply_id else None
        if text and record and record.get("kind") in ("thread", "preview"):
            edits = parse_edits(text, (msg.get("quote") or {}).get("text", ""))
            idea = self.ideas.get(record.get("idea"))
            if edits is not None and idea:
                return self.edit_thread(idea, edits, msg["message_id"])
        if text and record and record.get("kind") in ("photos", "photokw"):
            idea = self.ideas.get(record["idea"])
            if idea:
                self.request_photos(idea, keywords=text[:200])
            return
        if text and record and record.get("kind") == "preview":
            idea = self.ideas.get(record["idea"])
            if idea and self.category(idea) in ("T", "P"):
                self.send("✏️ Тут працюють лише точкові правки: виділіть фрагмент → «Відповісти» → заміна, "
                          "або рядок «старе → нове».", reply_to=msg["message_id"])
            elif idea:
                self.close_buttons(reply_id, "🏷 Тему змінено")
                self.apply_topic(idea, text)
            return
        awaiting = self.state.pop("awaiting", None)
        if text and not reply_id and awaiting and \
                datetime.now(timezone.utc) - parse_time(awaiting["at"]) < timedelta(minutes=30):
            idea = self.ideas.get(awaiting.get("idea"))
            if awaiting.get("kind") == "photokw" and idea:
                return self.request_photos(idea, keywords=text[:200])
            if awaiting.get("kind") == "tag" and idea:
                return self.apply_topic(idea, text)
        if text and not reply_id:
            return self.propose_topic(text)
        if not text or not record or record.get("kind") != "thread":
            return
        idea = self.ideas.get(record["idea"])
        if idea and idea.get("thread", {}).get("status") in ("ready", "revise"):
            idea.setdefault("revisions", []).append({"kind": "thread", "text": text, "at": now()})
            idea["thread"]["status"] = "revise"
            history(idea, "thread revision requested")
            self.fire.setdefault("texts", set()).add(idea["id"])
            self.send("✏️ Правки прийнято, нова версія прийде сюди.", reply_to=msg["message_id"])

    def edit_thread(self, idea: dict, edits: list[tuple[str, str]], reply_to: int) -> None:
        """Точкова правка тексту без переписування: замінює лише вказані фрагменти у файлі."""
        status = idea.get("thread", {}).get("status")
        if status in ("queued", "scheduled"):
            self.send("🕒 Текст уже в розкладі. Щоб виправити, зніміть його з розкладу (↩️ у картці теми), "
                      "потім надішліть правку ще раз.", reply_to=reply_to)
            return
        if status not in ("ready", "approved"):
            self.send("✍️ Зараз цей текст переписується або вже опублікований — правку не застосовано.", reply_to=reply_to)
            return
        path = ROOT / idea["thread"]["file"]
        body = path.read_text(encoding="utf-8")
        new_body, problems = apply_edits(body, edits)
        if problems:
            self.send("✂️ Нічого не змінив: " + "; ".join(problems) + ".\n\nВиділіть фрагмент у тексті й натисніть "
                      "«Відповісти» (цитата), або напишіть «старе → нове».", reply_to=reply_to)
            return
        if new_body == body:
            self.send("✂️ Текст і так такий — нічого не змінив.", reply_to=reply_to)
            return
        path.write_text(new_body, encoding="utf-8")
        idea["thread"]["status"] = "ready"
        idea["thread"].pop("approved_sha", None)
        history(idea, f"thread edited in telegram ({len(edits)} fragment(s))")
        done = "\n".join(f"• «{old}» → «{new}»" if new else f"• «{old}» — видалено" for old, new in edits)
        self.send(f"✂️ Виправлено:\n{done}\n\nОновлений текст — нижче; погодьте його ще раз.", reply_to=reply_to)

    # ------------------------------------------------------------ вихідні

    def send_new_ideas(self):
        fresh = [i for i in self.ideas.values() if i.get("status") == "proposed" and f"idea:{i['id']}" not in self.state["sent"]]
        if not fresh:
            return
        self.send(f"💡 Нові ідеї: {len(fresh)}. Позначте ✅ ті, що подобаються.")
        for idea in fresh:
            self.state["sent"][f"idea:{idea['id']}"] = self.send_idea(idea)
        self.send("Коли оберете — переходьте до текстів.", kb([("➡️ До текстів", "go:texts")]), {"kind": "nav"})

    def send_ready_threads(self):
        for idea in self.ideas.values():
            if idea.get("thread", {}).get("status") != "ready":
                continue
            body = self.thread_text(idea)
            if body is None:
                continue
            digest = sha(body)
            key = f"thread:{idea['id']}:{digest}"
            if key in self.state["sent"]:
                continue
            self.state["sent"][key] = self.send_thread(idea, body)

    def send_thread(self, idea: dict, body: str) -> int:
        marks = unverified(body)
        warning = f"\n\n⚠️ Є позначки [ПЕРЕВІРИТИ] ({marks}). {UNVERIFIED_HINT}" if marks else ""
        size = f" Розмір допису: {len(tg_post_text(body))} з {TG_POST_LIMIT} символів." if self.category(idea) == "T" else ""
        return self.send_text_parts(
            idea, body,
            f"{stream_label(idea.get('stream'))} · тред — «{idea['title']}»\n\n"
            f"ℹ️ Текст — у повідомленні вище, джерела — окремо.{size}{warning}\n\n"
            + edit_hint("побажання своїми словами (напр. «скороти вступ») — текст доопрацюють, "
                        "змінивши лише потрібне місце."),
            kb([("✅ Готово", f"done:{idea['id']}"), ("🔀 Змінити формат", f"fmt:{idea['id']}")],
               [("🗑 Скасувати тему", f"drop:{idea['id']}")]),
            {"kind": "thread", "idea": idea["id"], "sha": sha(body)})

    def publish_tg(self, idea_id: str, item: dict) -> None:
        """Telegram-допис: спершу тестовий канал, потім той самий текст в основний у випадковий час
        вікна TG_MAIN_WINDOW. Одне повідомлення, фото — великим прев'ю над текстом."""
        idea = self.ideas.get(idea_id, {"title": idea_id})
        if unverified(item["text"]):
            item["failed"] = "у тексті є [ПЕРЕВІРИТИ]"
            self.send(f"⚠️ Не публікую «{idea['title']}»: у тексті є [ПЕРЕВІРИТИ].")
            return
        text = tg_post_text(item["text"])
        if not text or len(text) > TG_POST_LIMIT:
            item["failed"] = f"допис довший за {TG_POST_LIMIT} символів ({len(text)})" if text else "порожній допис"
            self.send(f"⚠️ Не публікую «{idea['title']}»: {item['failed']}.")
            return
        test_stage = not item.get("test_id")
        if test_stage and not TG_CHANNEL_ID:  # тестового каналу немає — одразу в основний за розкладом
            item.update({"test_id": "skipped", "main_at": tg_main_slot(datetime.now(KYIV)).isoformat()})
            return
        channel = TG_CHANNEL_ID if test_stage else TG_MAIN_CHANNEL_ID
        where = "тестовому" if test_stage else "основному"
        image = item.get("image")
        preview = ({"url": image["url"], "prefer_large_media": True, "show_above_text": True}
                   if image and image.get("url") else {"is_disabled": True})
        request = urllib.request.Request(
            f"https://api.telegram.org/bot{TG_POSTER_TOKEN or TOKEN}/sendMessage",
            data=json.dumps({"chat_id": channel, "text": text, "link_preview_options": preview}).encode("utf-8"),
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                message_id = json.load(response)["result"]["message_id"]
        except (urllib.error.URLError, OSError, ValueError, KeyError) as exc:
            try:
                reason = json.load(exc).get("description", "") if isinstance(exc, urllib.error.HTTPError) else str(exc)
            except ValueError:
                reason = str(exc)
            item["attempts"] = item.get("attempts", 0) + 1
            if item["attempts"] >= 3:
                item["failed"] = reason
            self.send(f"⚠️ Telegram-допис «{idea['title']}» не опубліковано в {where} каналі: {reason}."
                      + (" Більше не пробую — напишіть мені." if item.get("failed") else " Повторю протягом години."))
            return
        permalink = tg_permalink(channel, message_id)
        item["attempts"] = 0
        if test_stage:
            item.update({"test_id": str(message_id), "test_permalink": permalink, "test_posted_at": now(),
                         "main_at": tg_main_slot(datetime.now(KYIV)).isoformat()})
            if idea_id in self.ideas:
                history(self.ideas[idea_id], f"telegram post published to test channel, main at {item['main_at']}")
            if item.get("status_mid"):
                self.close_buttons(item["status_mid"], f"🧪 У тестовому · основний {fmt_slot(parse_time(item['main_at']))}")
            self.send(f"🧪 Допис у тестовому каналі: «{idea['title']}»\n{permalink}\n\n"
                      f"🕒 В основний канал — {fmt_slot(parse_time(item['main_at']))}.",
                      kb([("⚡ В основний зараз", f"when:{idea_id}:now")]), {"kind": "when"})
            return
        item.update({"posted": True, "posted_at": now(), "permalink": permalink, "posted_ids": [str(message_id)]})
        if idea_id in self.ideas:
            self.ideas[idea_id]["thread"]["status"] = "posted"
            self.ideas[idea_id]["thread"]["permalink"] = permalink
            history(self.ideas[idea_id], "telegram post published to main channel")
        self.publish_done(item, permalink)
        self.send(f"✅ Telegram-допис опубліковано в основному каналі: «{idea['title']}»\n{permalink}")

    def publish_queued(self):
        """Публікує одну публікацію за запуск; прогрес тредів зберігає після кожного поста."""
        current = datetime.now(timezone.utc)
        for idea_id, item in self.state["approved_threads"].items():
            if item.get("posted") or item.get("failed"):
                continue
            at = item.get("main_at") if item.get("test_id") else item.get("publish_at")
            if at == "pending" or (at and parse_time(at) > current):
                continue
            if self.category(self.ideas.get(idea_id)) == "T":
                self.publish_tg(idea_id, item)
                return
            if not (THREADS_TOKEN and THREADS_USER_ID):
                continue
            idea = self.ideas.get(idea_id, {"title": idea_id})
            if unverified(item["text"]):
                item["failed"] = "у тексті є [ПЕРЕВІРИТИ]"
                self.send(f"⚠️ Не публікую «{idea['title']}»: у тексті є [ПЕРЕВІРИТИ].")
                return
            posts = thread_posts(item["text"])
            image = item.get("image")
            too_long = [n for n, p in enumerate(posts, 1) if threads_length(p) > THREADS_POST_LIMIT]
            if not posts or too_long:
                item["failed"] = f"пости {too_long} довші за {THREADS_POST_LIMIT} символів" if too_long else "порожній тред"
                self.send(f"⚠️ Не публікую «{idea['title']}»: {item['failed']}.")
                return
            posted = item.setdefault("posted_ids", [])
            try:
                for text in posts[len(posted):]:
                    params = {"media_type": "TEXT", "text": text}
                    if not posted and image and image.get("url"):
                        params.update(media_type="IMAGE", image_url=image["url"])
                    if not posted and item.get("topic"):
                        params["topic_tag"] = item["topic"]
                    if posted:
                        params["reply_to_id"] = posted[-1]
                        time.sleep(10)  # дати Threads «доварити» попередній пост
                    container = threads_retry("POST", f"{THREADS_USER_ID}/threads", **params)["id"]
                    for _ in range(40):  # Threads радить дочекатися обробки контейнера (до 2 хв)
                        status = threads_retry("GET", container, fields="status,error_message")
                        if status.get("status") in ("FINISHED", "PUBLISHED"):
                            break
                        if status.get("status") in ("ERROR", "EXPIRED"):
                            raise ThreadsError(status.get("error_message") or status["status"])
                        time.sleep(3)
                    else:
                        raise ThreadsError("Threads не встиг обробити пост за 2 хвилини")
                    for attempt in range(4):
                        try:
                            posted.append(threads_retry("POST", f"{THREADS_USER_ID}/threads_publish",
                                                        creation_id=container)["id"])
                            break
                        except ThreadsError as exc:
                            # 4279009 — «ресурс не існує»: пост ще не доступний для публікації.
                            if exc.subcode != 4279009 or attempt == 3:
                                raise
                            time.sleep(15 * (attempt + 1))
                    save_json(STATE_FILE, self.state)
            except ThreadsError as exc:
                item["attempts"] = item.get("attempts", 0) + 1
                if item["attempts"] >= 3:
                    item["failed"] = str(exc)
                done = (f"Опубліковано {len(posted)} з {len(posts)} постів. " if posted else "Нічого не опубліковано. ")
                follow = (" Більше не пробую — напишіть мені, розберемося." if item.get("failed") else
                          f" Повторю автоматично протягом години й продовжу з поста {len(posted) + 1} — "
                          "уже опубліковані пости не дублюються.")
                self.send(f"⚠️ «{idea['title']}»: пост {len(posted) + 1} не вийшов через збій Threads ({exc}). "
                          f"{done}{follow}")
                return
            permalink = ""
            try:
                permalink = threads_call("GET", posted[0], fields="permalink").get("permalink", "")
            except ThreadsError:
                pass
            item.update({"posted": True, "posted_at": now(), "permalink": permalink})
            if idea_id in self.ideas:
                self.ideas[idea_id]["thread"]["status"] = "posted"
                self.ideas[idea_id]["thread"]["permalink"] = permalink
                history(self.ideas[idea_id], f"thread posted to Threads ({len(posted)} posts)")
            self.publish_done(item, permalink)
            self.send(f"✅ Опубліковано в Threads: «{idea['title']}» ({len(posted)} постів)\n{permalink}",
                      kb([("📣 Ще публікація", "go:publish")]), {"kind": "nav"})
            return

    def token_reminder(self):
        if not THREADS_TOKEN_UPDATED:
            return
        age = (datetime.now(KYIV).date() - datetime.fromisoformat(THREADS_TOKEN_UPDATED).date()).days
        week = datetime.now(KYIV).strftime("%G-%V")
        if age >= 50 and self.state.get("token_reminded") != week:
            self.state["token_reminded"] = week
            self.send(f"🔑 Токену Threads {age} днів (діє 60). Оновіть на Маці:\n"
                      "cd ~/GenShorts && MoneyPrinterTurbo/.venv/bin/python tools/threads_token.py refresh")

    def fetch_insights(self, media_id: str) -> dict | None:
        for metrics in (METRICS, "views,likes,replies,reposts,quotes"):
            try:
                response = threads_call("GET", f"{media_id}/insights", metric=metrics)
            except ThreadsError:
                continue
            values = {}
            for item in response.get("data", []):
                value = (item.get("total_value") or {}).get("value")
                if value is None and item.get("values"):
                    value = item["values"][0].get("value")
                values[item.get("name")] = value
            return values
        return None

    def collect_analytics(self) -> None:
        """Раз на ANALYTICS_EVERY: список постів акаунта й статистика; зрізи через 24 год, 3 і 7 днів."""
        if not (THREADS_TOKEN and THREADS_USER_ID):
            return
        last = self.state.get("analytics_at")
        if last and datetime.now(timezone.utc) - parse_time(last) < ANALYTICS_EVERY - timedelta(minutes=10):
            return
        self.state["analytics_at"] = now()
        data = load_json(ANALYTICS_FILE, {"posts": {}})
        posts = data.setdefault("posts", {})
        bot_roots = {item["posted_ids"][0]: (idea_id, item)
                     for idea_id, item in self.state["approved_threads"].items() if item.get("posted_ids")}
        after, pages = None, (1 if posts else 6)
        try:
            for _ in range(pages):
                params = {"fields": "id,timestamp,media_type,permalink,text", "limit": 50}
                if after:
                    params["after"] = after
                page = threads_call("GET", f"{THREADS_USER_ID}/threads", **params)
                for post in page.get("data", []):
                    record = posts.setdefault(post["id"], {})
                    record.update({"timestamp": post.get("timestamp"), "permalink": post.get("permalink"),
                                   "media_type": post.get("media_type"), "text_len": len(post.get("text") or "")})
                    if post["id"] in bot_roots:
                        idea_id, item = bot_roots[post["id"]]
                        idea = self.ideas.get(idea_id, {})
                        record.update({"origin": "bot", "idea": idea_id, "title": idea.get("title"),
                                       "stream": idea.get("stream"), "topic": item.get("topic"),
                                       "posts_count": len(item.get("posted_ids", [])), "has_photo": bool(item.get("image")),
                                       "scheduled": item.get("publish_at") not in (None, "pending")})
                    else:
                        record.setdefault("origin", "manual")
                        record.setdefault("has_photo", post.get("media_type") not in ("TEXT_POST", "TEXT"))
                paging = page.get("paging", {})
                after = paging.get("cursors", {}).get("after")
                if not paging.get("next"):
                    break
            budget, current = 40, datetime.now(timezone.utc)
            for media_id, record in sorted(posts.items(), key=lambda kv: kv[1].get("timestamp") or "", reverse=True):
                if budget <= 0 or not record.get("timestamp"):
                    continue
                age = current - parse_time(record["timestamp"])
                if age > timedelta(days=8) and record.get("final"):
                    continue
                metrics = self.fetch_insights(media_id)
                budget -= 1
                if metrics is None:
                    continue
                record["latest"] = {**metrics, "at": now()}
                for label, hours in (("h24", 24), ("h72", 72), ("h168", 168)):
                    if age >= timedelta(hours=hours) and label not in record:
                        record[label] = metrics
                if age > timedelta(days=8):
                    record["final"] = True
        except ThreadsError as exc:
            print(f"[analytics] {exc}")
        data["updated_at"] = now()
        save_json(ANALYTICS_FILE, data)

    def slot_reminders(self) -> None:
        current = datetime.now(KYIV)
        reminded = self.state.setdefault("reminded_slots", [])
        busy = [moment for moment, _ in self.bot_publications()]
        for slot in self.recommended_slots(days=1):
            if not (timedelta(0) < slot - current <= REMIND_BEFORE + timedelta(minutes=5)):
                continue
            key = slot.isoformat()
            if key in reminded or any(abs(slot - b) < SLOT_GAP / 2 for b in busy):
                continue
            reminded.append(key)
            day_cats = [c for moment, c in self.bot_publications() if moment.astimezone(KYIV).date() == slot.date()]
            serious = "" if "S" in day_cats else " Серйозного треду на сьогодні теж немає."
            if self.grouped(lambda i: i.get("thread", {}).get("status") == "approved"
                            and i.get("stream") != "tg"):
                self.send(f"⏰ {slot:%H:%M} — рекомендований час публікації, але нічого не заплановано.{serious}",
                          kb([("📣 Обрати тред", "go:publish")]), {"kind": "nav"})
            else:
                self.send(f"⏰ {slot:%H:%M} — рекомендований час публікації, але готових тредів немає.{serious}",
                          kb([("✍️ Написати тред", "go:texts")]), {"kind": "nav"})
        tg_taken = {m.astimezone(KYIV).date() for m, _ in self.bot_publications(channel="tg")}
        for slot in self.tg_slots(days=1):
            key = f"tg:{slot.isoformat()}"
            if not (timedelta(0) < slot - current <= REMIND_BEFORE + timedelta(minutes=5)) \
                    or key in reminded or slot.date() in tg_taken:
                continue
            reminded.append(key)
            ready = self.grouped(lambda i: i.get("stream") == "tg" and i.get("thread", {}).get("status") == "approved")
            self.send(f"⏰ {slot:%H:%M} — час Telegram-допису, але нічого не заплановано"
                      + ("." if ready else ", і готових дописів немає."),
                      kb([("📣 Обрати допис", "go:publish")] if ready else [("✍️ Написати допис", "go:texts")]),
                      {"kind": "nav"})
        self.state["reminded_slots"] = reminded[-50:]

    def next_due(self) -> int:
        """Найближчий момент, коли воркер має розбудити диспетчер: публікація, нагадування,
        ранкові питання, аналітика, тижневий звіт чи перезапуск задачі. Щогодинних запусків немає."""
        current = datetime.now(KYIV)
        moments = [m for m, _, _ in self.scheduled() if m > current]
        moments += self.planned_moments(current)
        busy = [m for m, _ in self.bot_publications()]
        reminded = set(self.state.get("reminded_slots", []))
        tg_taken = {m.astimezone(KYIV).date() for m, _ in self.bot_publications(channel="tg")}
        for slot in self.tg_slots(days=8):
            if slot.date() not in tg_taken and f"tg:{slot.isoformat()}" not in reminded and slot - REMIND_BEFORE > current:
                moments.append(slot - REMIND_BEFORE)
        for slot in self.recommended_slots(days=2):
            if any(abs(slot - b) < SLOT_GAP / 2 for b in busy):
                continue
            if slot.isoformat() not in reminded and slot - REMIND_BEFORE > current:
                moments.append(slot - REMIND_BEFORE)
        return int(min(moments).timestamp()) if moments else 0

    def planned_moments(self, current: datetime) -> list[datetime]:
        """Планові справи, які раніше чекали щогодинного запуску."""
        moments = []
        for name, hour in ASK_AT.items():  # питання про ідеї й тексти
            day = current.date() if self.state["asked"].get(name) != current.date().isoformat() else \
                current.date() + timedelta(days=1)
            moments.append(datetime(day.year, day.month, day.day, hour, 0, tzinfo=KYIV))
        fired = self.state.get("fired_at", {})  # незавершена робота задач — перевірити, чи не зависла
        busy = {"texts": any(i.get("thread", {}).get("status") in ("requested", "revise") or i.get("photo_request")
                             for i in self.ideas.values()),
                "ideas": any(r.get("status") == "pending" for r in self.requests.get("ideas", []))}
        for name, has_work in busy.items():
            if has_work and fired.get(name):
                moments.append(parse_time(fired[name]).astimezone(KYIV) + STALE_AFTER + timedelta(minutes=2))
        return [m for m in moments if m > current]

    def send_slot_proposal(self) -> None:
        """Пропозиція аналітики змінити години публікацій — лише з погодження автора."""
        recs = load_json(RECS_FILE, {})
        pid = recs.get("proposal_id")
        if not recs.get("proposed_window") or not pid or f"recs:{pid}" in self.state["sent"]:
            return
        def fmt(window):
            return f"щодня {window.get('start')}–{window.get('end')}" if window else "—"
        self.state["sent"][f"recs:{pid}"] = self.send(
            f"📊 Аналітика пропонує змінити діапазон публікацій.\nЗараз: {fmt(recs.get('window'))}\n"
            f"Пропонує: {fmt(recs['proposed_window'])}\n\n{recs.get('proposal_reason', '')}",
            kb([("✅ Погодити", f"recs:yes:{pid}"), ("❌ Лишити як є", f"recs:no:{pid}")]), {"kind": "recs"})

    def send_photo_results(self) -> None:
        """Повідомляє про завершений пошук фото, запитаний кнопкою."""
        for idea in self.ideas.values():
            requested = idea.get("photo_requested_at")
            if not requested or idea.get("photo_request"):
                continue
            key = f"photos:{idea['id']}:{requested}"
            if key in self.state["sent"]:
                continue
            if idea.get("photo_error"):
                self.state["sent"][key] = self.send(
                    f"⚠️ Пошук фото для «{idea['title']}» не вдався: {idea['photo_error']}",
                    kb([("🔄 Спробувати ще", f"photos:{idea['id']}"), ("🔎 Фото за словами", f"photokw:{idea['id']}")]),
                    {"kind": "photos", "idea": idea["id"]})
                continue
            if idea.get("images"):
                photos = self.show_photos(idea)
                text = (f"🖼 Для «{idea['title']}» знайдено фото: {', '.join(map(str, photos))} (вище). "
                        "Обрати можна на фінальній перевірці." if photos else
                        f"🖼 Для «{idea['title']}» знайшлися фото, але Telegram не зміг їх відкрити — Threads теж не зможе.")
            else:
                text = f"🖼 Для «{idea['title']}» доречних фото з відкритою ліцензією не знайшлося."
            status = idea.get("thread", {}).get("status")
            label = "🖼 Обрати фото" if status == "scheduled" else "🕒 Обрати фото й запланувати"
            rows = []
            if idea.get("images") and status in ("approved", "queued", "scheduled"):
                rows.append([(label, f"pub:{idea['id']}")])
            rows.append([("🔄 Інші фото", f"photos:{idea['id']}:more"), ("🔎 Фото за словами", f"photokw:{idea['id']}")])
            text += "\n🔎 «Фото за словами» — шукати за вашими ключовими словами."
            self.state["sent"][key] = self.send(text, kb(*rows), {"kind": "photos", "idea": idea["id"]})

    def revive_stale_work(self) -> None:
        """Якщо є незавершена робота, а задачу давно не запускали (обрив мережі тощо) — запуск."""
        fired = self.state.setdefault("fired_at", {})
        pending = {
            "texts": {i["id"] for i in self.ideas.values()
                      if i.get("thread", {}).get("status") in ("requested", "revise") or i.get("photo_request")},
            "ideas": {r["stream"] for r in self.requests.get("ideas", []) if r.get("status") == "pending"},
        }
        current = datetime.now(timezone.utc)
        revives = self.state.setdefault("revive_count", {})
        names = {"texts": "Тексти", "ideas": "Ідеї"}
        for name, ids in pending.items():
            if not ids:
                revives.pop(name, None)
                continue
            last = fired.get(name)
            if name in self.fire or (last and current - parse_time(last) <= STALE_AFTER):
                continue
            revives[name] = revives.get(name, 0) + 1
            what = ", ".join(f"«{self.ideas[i]['title']}»" for i in sorted(ids) if i in self.ideas) or "запит ідей"
            if revives[name] > 3:
                if revives[name] == 4:
                    self.send(f"⚠️ Задача «{names[name]}» тричі не завершила роботу ({what}). "
                              "Більше не перезапускаю автоматично — напишіть мені, розберемося.")
                continue
            self.fire.setdefault(name, set()).update(ids)
            print(f"[revive] {name}: {sorted(ids)}")
            self.send(f"⏳ Задача «{names[name]}» не завершила роботу ({what}) — перезапускаю "
                      f"(спроба {revives[name]} з 3).")
        for name in self.fire:
            fired[name] = now()

    def scheduled_prompts(self):
        local = datetime.now(KYIV)
        today = local.date().isoformat()
        for name, hour in ASK_AT.items():
            if local.hour < hour or self.state["asked"].get(name) == today:
                continue
            self.state["asked"][name] = today
            if name == "ideas":
                self.ask_ideas()
            elif name == "texts":
                self.menu_texts(quiet=True)

    # ------------------------------------------------------------ дашборд

    def dash_stage(self, idea: dict) -> str | None:
        """Етап конвеєра для дашборда (ті самі правила, що в show_queue)."""
        status = idea.get("status")
        thread = idea.get("thread", {}).get("status", "none")
        item = self.state["approved_threads"].get(idea["id"], {})
        if status == "proposed":
            return "proposed"
        if status != "approved" or thread == "cancelled":
            return None
        if thread == "none":
            return "none"
        if thread in ("requested", "revise"):
            return "writing"
        if thread == "ready":
            return "ready"
        if thread == "approved" or (thread == "queued" and item.get("publish_at") in (None, "pending")):
            return "approved"
        if thread in ("queued", "scheduled"):
            return "scheduled"
        if thread == "posted":
            return "posted"
        return None

    def dash_pipeline(self) -> list[str]:
        stages = [("proposed", "💡 Нові ідеї на розгляді"), ("none", "✅ Погоджені, тексту немає"),
                  ("writing", "✍️ Пишеться"), ("ready", "👀 Чекає погодження"),
                  ("approved", "📦 Готові, без часу"), ("scheduled", "🕒 Заплановано")]
        attention = {"proposed", "ready", "approved"}  # де чекають саме автора
        counts = {(key, ch): 0 for key, _ in stages + [("posted", "")] for ch in ("threads", "tg")}
        week_ago = datetime.now(timezone.utc) - timedelta(days=7)
        recent = {"threads": 0, "tg": 0}
        for idea in self.ideas.values():
            stage = self.dash_stage(idea) if not idea.get("test") else None
            if not stage:
                continue
            channel = "tg" if self.category(idea) == "T" else "threads"
            counts[(stage, channel)] += 1
            posted_at = self.state["approved_threads"].get(idea["id"], {}).get("posted_at")
            if stage == "posted" and posted_at and parse_time(posted_at) >= week_ago:
                recent[channel] += 1

        def cell(key: str, channel: str) -> str:
            value = counts[(key, channel)]
            if not value:
                return '<td align="center">·</td>'
            text = f"<b>{value}</b>" if key in attention else str(value)
            return f'<td align="center">{text}</td>'

        rows = ['<tr><th align="left">Етап</th><th align="center">🧵 Threads</th><th align="center">📰 TG</th></tr>']
        for key, title in stages:
            rows.append(f'<tr><td align="left">{title}</td>{cell(key, "threads")}{cell(key, "tg")}</tr>')
        rows.append('<tr><td align="left">📤 Опубліковано: 7 днів / усього</td>'
                    + "".join(f'<td align="center">{recent[ch]} / {counts[("posted", ch)]}</td>' for ch in ("threads", "tg"))
                    + "</tr>")
        return ["<h3>🏭 Конвеєр тем</h3>",
                "<table bordered striped compact><caption>Жирним — там, де чекають вашого рішення</caption>"
                f"{''.join(rows)}</table>"]

    def dash_schedule(self) -> list[str]:
        current = datetime.now(KYIV)
        icons = {s: icon for s, (icon, _) in STREAMS.items()}
        days = {}  # дата -> {"threads": [...], "tg": [...]}
        for idea_id, item in self.state["approved_threads"].items():
            idea = self.ideas.get(idea_id)
            if not idea or idea.get("test"):
                continue
            cat = self.category(idea)
            if item.get("posted") and item.get("posted_at"):
                moment, mark = parse_time(item["posted_at"]), "✅"
            elif item.get("failed"):
                at = item.get("main_at") if item.get("test_id") else item.get("publish_at")
                if not at or at == "pending":
                    continue
                moment, mark = parse_time(at), "❗"
            else:
                at = item.get("main_at") if item.get("test_id") else item.get("publish_at")
                if not at or at == "pending":
                    continue
                moment, mark = parse_time(at), "🧪" if item.get("test_id") else "🕒"
            local = moment.astimezone(KYIV)
            entry = f"{mark} {local:%H:%M} {icons.get(idea.get('stream'), '•')} {esc(short(idea['title'], 40))}"
            days.setdefault(local.date(), {"threads": [], "tg": []})["tg" if cat == "T" else "threads"].append((local, entry))
        threads_slots = {s.date() for s in self.recommended_slots(days=DASH_DAYS)}
        tg_slots = {s.date() for s in self.tg_slots(days=DASH_DAYS)}
        rows = ['<tr><th align="left">День</th><th align="left">🧵 Threads</th><th align="left">📰 TG</th></tr>']
        empty_days = 0
        for offset in range(DASH_DAYS):
            day = (current + timedelta(days=offset)).date()
            label = {0: "сьогодні", 1: "завтра"}.get(offset, f"{WEEKDAYS[day.weekday()]} {day:%d.%m}")
            found = days.get(day, {"threads": [], "tg": []})
            cells = []
            for channel, slots in (("threads", threads_slots), ("tg", tg_slots)):
                entries = [e for _, e in sorted(found[channel])]
                if entries:
                    cells.append(" · ".join(entries))
                elif day in slots:
                    cells.append("⚠️ <i>порожньо</i>")
                    empty_days += channel == "threads"
                else:
                    cells.append("·")
            rows.append(f'<tr><td align="left"><b>{label}</b></td><td align="left">{cells[0]}</td>'
                        f'<td align="left">{cells[1]}</td></tr>')
        parts = ["<h3>🗓 Розклад</h3>",
                 "<table bordered striped compact>"
                 "<caption>🕒 заплановано · 🧪 вийшло в тестовому, чекає основного · ✅ вийшло · ❗ помилка</caption>"
                 f"{''.join(rows)}</table>"]
        notes = []
        free = self.free_slots(1)
        if free:
            notes.append(f"Найближча вільна година для треду: <b>{fmt_slot(free[0])}</b>")
        ready = sum(1 for i in self.ideas.values() if not i.get("test") and self.dash_stage(i) == "approved"
                    and self.category(i) in ("S", "F"))
        if empty_days and ready:
            notes.append(f"Порожніх днів: {empty_days}, а готових тредів без часу: {ready} — можна запланувати.")
        elif empty_days:
            notes.append(f"Порожніх днів: {empty_days}, готових тредів немає — варто написати нові.")
        parts += [f"<p>{n}</p>" for n in notes]
        return parts

    def dash_system(self) -> list[str]:
        current = datetime.now(timezone.utc)
        fired = self.state.get("fired_at", {})
        items = []

        def ago(at: str | None) -> str:
            return tg_time(parse_time(at), "r", fmt_slot(parse_time(at))) if at else "ще ні разу"

        writing = [i for i in self.ideas.values() if i.get("thread", {}).get("status") in ("requested", "revise")]
        photos = [i for i in self.ideas.values() if i.get("photo_request")]
        if writing or photos:
            stale = fired.get("texts") and current - parse_time(fired["texts"]) > STALE_AFTER
            what = ", ".join(f"«{esc(short(i['title'], 32))}»" for i in writing[:3])
            extra = f" (+{len(writing) - 3})" if len(writing) > 3 else ""
            jobs = [f"текстів: {len(writing)}"] if writing else []
            jobs += [f"пошуків фото: {len(photos)}"] if photos else []
            items.append(f"{'⚠️' if stale else '⏳'} Задача «Тексти» працює ({', '.join(jobs)}) — запущена "
                         f"{ago(fired.get('texts'))}{'; довго немає результату' if stale else ''}"
                         + (f". {what}{extra}" if what else ""))
        else:
            items.append(f"✅ Задача «Тексти» вільна; востаннє запускалася {ago(fired.get('texts'))}")
        pending = [r for r in self.requests.get("ideas", []) if r.get("status") == "pending"]
        if pending:
            stale = fired.get("ideas") and current - parse_time(fired["ideas"]) > STALE_AFTER
            streams = ", ".join(stream_label(r.get("stream")) for r in pending)
            items.append(f"{'⚠️' if stale else '⏳'} Задача «Ідеї» шукає: {esc(streams)} — запит "
                         f"{ago(pending[0].get('requested_at'))}")
        else:
            items.append(f"✅ Задача «Ідеї» вільна; востаннє запускалася {ago(fired.get('ideas'))}")
        names = {"texts": "Тексти", "ideas": "Ідеї"}
        for name, count in self.state.get("revive_count", {}).items():
            if count > 3:
                items.append(f"🛑 Задача «{names.get(name, name)}» тричі не завершила роботу — автоматичні "
                             "перезапуски зупинено, потрібне втручання")
        failed = [(i, item) for i, item in self.state["approved_threads"].items()
                  if item.get("failed") and not item.get("posted") and i in self.ideas
                  and self.ideas[i].get("thread", {}).get("status") not in ("posted", "cancelled")]
        for idea_id, item in failed[:5]:
            items.append(f"❗ Не опубліковано «{esc(short(self.ideas[idea_id]['title'], 40))}»: "
                         f"{esc(short(item['failed'], 80))}")
        if not failed:
            items.append("✅ Помилок публікації немає")
        items.append(f"📈 Статистика Threads оновлена {ago(self.state.get('analytics_at'))}")
        if THREADS_TOKEN_UPDATED:
            age = (datetime.now(KYIV).date() - datetime.fromisoformat(THREADS_TOKEN_UPDATED).date()).days
            items.append(f"{'⚠️' if age >= 50 else '🔑'} Токену Threads {age} дн. із 60"
                         + (" — час оновити" if age >= 50 else ""))
        if not (THREADS_TOKEN and THREADS_USER_ID):
            items.append("⚠️ Threads не підключено (немає токена чи ID користувача)")
        return ["<h3>⚙️ Стан системи</h3>", "<ul>" + "".join(f"<li>{i}</li>" for i in items) + "</ul>"]

    def dashboard_html(self) -> tuple[str, str]:
        """HTML дашборда й відбиток його змісту (без позначки часу оновлення)."""
        body = ["<h2>📊 Записки диванного архівіста</h2>"]
        body += self.dash_pipeline() + ["<hr/>"] + self.dash_schedule() + ["<hr/>"] + self.dash_system()
        body.append('<tg-button-row align="center">'
                    '<tg-button type="callback_data" data="dash:refresh">🔄 Оновити</tg-button>'
                    '<tg-button type="callback_data" data="dash:publish">📣 Публікація</tg-button>'
                    '</tg-button-row>'
                    + (f'<tg-button-row align="center"><tg-button type="callback_data" data="dash:proposed">'
                       f'💡 Нерозглянуті ідеї ({len(self.proposed())})</tg-button></tg-button-row>'
                       if self.proposed() else '')
                    + '<tg-button-row align="center">'
                    '<tg-button type="callback_data" data="dash:queue">📋 Черга</tg-button>'
                    '<tg-button type="callback_data" data="dash:tgqueue">📰 TG-черга</tg-button>'
                    '</tg-button-row>')
        content = "".join(body)
        stamp = datetime.now(timezone.utc)
        footer = f"<footer>Оновлено {tg_time(stamp, 'r', fmt_slot(stamp))} · час розкладу — київський</footer>"
        return content + footer, sha(content)

    def refresh_dashboard(self, force: bool = False, repost: bool = False) -> None:
        """Редагує закріплений дашборд, якщо зміст змінився (або force); repost — надіслати
        заново внизу чату й закріпити, старий прибрати. Помилки дашборда не зупиняють диспетчер."""
        try:
            html, digest = self.dashboard_html()
        except Exception as exc:  # noqa: BLE001 — дашборд не має ламати основну роботу
            print(f"[dashboard] build failed: {exc!r}")
            return
        dash = self.state.setdefault("dashboard", {})
        fresh = dash.get("at") and datetime.now(timezone.utc) - parse_time(dash["at"]) < DASH_REFRESH
        if dash.get("mid") and not repost:
            if dash.get("sha") == digest and fresh and not force:
                return
            _, error = tg_try("editMessageText", chat_id=CHAT_ID, message_id=dash["mid"],
                              rich_message={"html": html})
            if error is None or "not modified" in error:
                dash.update({"sha": digest, "at": now()})
                dash.pop("error", None)
                return
            print(f"[dashboard] edit: {error}")
            if "not found" not in error and "can't be edited" not in error:
                return  # тимчасовий збій — спробуємо наступного запуску
        result, error = tg_try("sendRichMessage", chat_id=CHAT_ID, rich_message={"html": html},
                               disable_notification=True)
        if error:
            print(f"[dashboard] send: {error}")
            if dash.get("error") != error:  # повідомити автора лише раз про кожну нову помилку
                dash["error"] = error
                self.send(f"⚠️ Не вдалося оновити дашборд: {error}")
            return
        old = dash.get("mid")
        if old:
            tg_try("unpinChatMessage", chat_id=CHAT_ID, message_id=old)
            tg_try("deleteMessage", chat_id=CHAT_ID, message_id=old)
            self.state["messages"].pop(str(old), None)
        mid = result["message_id"]
        tg_try("pinChatMessage", chat_id=CHAT_ID, message_id=mid, disable_notification=True)
        self.state["messages"][str(mid)] = {"kind": "dashboard"}
        self.state["dashboard"] = {"mid": mid, "sha": digest, "at": now()}

    # ------------------------------------------------------------ запуск

    def ensure_menu(self) -> None:
        if self.state.get("menu_version") == MENU_VERSION:
            return
        tg("setMyCommands", commands=[
            {"command": "ideas", "description": "Нові ідеї"},
            {"command": "texts", "description": "Написати тред"},
            {"command": "publish", "description": "Публікація"},
            {"command": "queue", "description": "Черга тредів Threads"},
            {"command": "tgqueue", "description": "Черга Telegram-дописів"},
            {"command": "published", "description": "Опубліковані треди"},
            {"command": "dashboard", "description": "Дашборд: конвеєр, розклад, стан"},
            {"command": "help", "description": "Довідка"},
        ])
        self.send("Меню оновлено 👇\n" + HELP, MAIN_KEYBOARD)
        self.state["menu_version"] = MENU_VERSION

    def collect_updates(self) -> list[dict]:
        if not (QUEUE_URL and QUEUE_SECRET):
            updates = tg("getUpdates", offset=self.state["offset"], timeout=0,
                         allowed_updates=["message", "callback_query"]) or []
            if updates:
                self.state["offset"] = updates[-1]["update_id"] + 1
            return updates
        found = {}
        if EVENT_UPDATE:
            update = json.loads(EVENT_UPDATE)
            found[update["update_id"]] = update
        # Черга підхоплює й оновлення, чиї запуски GitHub скасував через паралельність.
        for update in queue_call("GET", "/pending"):
            found[update["update_id"]] = update
        done = set(self.state.setdefault("processed", []))
        self.acked.update(found)
        return [found[i] for i in sorted(found) if i not in done]

    def handle(self, updates: list[dict]) -> None:
        for update in updates:
            self.state.setdefault("processed", []).append(update["update_id"])
            self.state["processed"] = self.state["processed"][-500:]
            if "callback_query" in update:
                self.on_callback(update["callback_query"])
            elif "message" in update:
                self.on_message(update["message"])

    def take_pending(self) -> int:
        """Забирає з черги воркера ще не оброблені натискання й одразу на них відповідає."""
        done = set(self.state.get("processed", []))
        fresh = sorted((u for u in queue_call("GET", "/pending") if u["update_id"] not in done),
                       key=lambda u: u["update_id"])
        self.acked.update(u["update_id"] for u in fresh)
        if fresh:
            self.handle(fresh)
            self.publish_queued()  # «⚡ Опублікувати зараз»
            self.send_ready_threads()  # текст після точкової правки
            self.refresh_dashboard()  # редагує лише якщо зміст змінився
        return len(fresh)

    def announce_live(self) -> float:
        """Каже воркеру, до якого моменту диспетчер опитуватиме чергу (кінець уже оплаченої
        хвилини мінус час на коміт і пуш), щоб нові натискання не створювали запусків.
        0 — чекати вже ніколи."""
        elapsed = time.time() - JOB_STARTED_AT
        stop = JOB_STARTED_AT + max(1, -(-(elapsed + LINGER_MARGIN) // 60)) * 60 - LINGER_MARGIN
        if stop - time.time() < LINGER_POLL + 1:
            return 0
        if stop != self.announced:
            queue_call("POST", "/live", {"until": int(stop)})
            self.announced = stop
        return stop

    def linger(self) -> int:
        """До кінця вже оплаченої хвилини Actions забирає нові натискання з черги й одразу
        відповідає на них. Поки диспетчер чекає, воркер не створює нових запусків (/live).
        Зупиняється, щойно треба запустити хмарну задачу: вона клонує репозиторій і має
        побачити збережений стан."""
        if not (QUEUE_URL and QUEUE_SECRET):
            return 0
        handled = 0
        try:
            while not self.fire and self.announce_live():
                handled += self.take_pending()  # спершу — що прийшло, поки йшла основна робота
                if not self.fire:
                    time.sleep(LINGER_POLL)
        except (urllib.error.URLError, OSError, ValueError) as exc:
            print(f"[linger] {exc}")
        if self.announced:
            try:
                queue_call("POST", "/live", {"until": 0})  # нові натискання знову запускають диспетчер
                handled += self.take_pending()  # що прийшло, поки диспетчер завершував чекати
                # Воркер звірить: якщо пропущене натискання новіше за побачені — запустить диспетчер.
                queue_call("POST", "/live", {"until": 0, "seen": max(self.state.get("processed") or [0])})
            except (urllib.error.URLError, OSError, ValueError) as exc:
                print(f"[linger] {exc}")
        return handled

    def run(self) -> int:
        updates = self.collect_updates()
        if QUEUE_URL and QUEUE_SECRET:
            try:  # одразу: натискання під час основної роботи підхопить цей же запуск
                self.announce_live()
            except (urllib.error.URLError, OSError, ValueError) as exc:
                print(f"[linger] {exc}")
        self.handle(updates)

        self.ensure_menu()
        self.publish_queued()
        self.collect_analytics()
        self.send_new_ideas()
        self.send_ready_threads()
        self.send_photo_results()
        self.send_slot_proposal()
        self.token_reminder()
        self.scheduled_prompts()
        self.slot_reminders()

        self.revive_stale_work()
        self.refresh_dashboard()
        lingered = self.linger()
        if QUEUE_URL and QUEUE_SECRET:
            save_json(ACK_FILE, sorted(self.acked))
        save_json(IDEAS_FILE, self.ideas_doc)
        save_json(REQUESTS_FILE, self.requests)
        save_json(STRESS_FILE, self.stress)
        save_json(STATE_FILE, self.state)
        save_json(FIRE_FILE, {name: sorted(ids) for name, ids in self.fire.items()})
        if QUEUE_URL and QUEUE_SECRET:
            try:
                queue_call("POST", "/due", {"at": self.next_due()})
            except (urllib.error.URLError, ValueError) as exc:
                print(f"[due] {exc}")
        print(f"[dispatch] updates: {len(updates)} + {lingered} while waiting, to fire: {sorted(self.fire)}")
        return 0


def fire_pending() -> int:
    """Окремий крок після пушу: задачі клонують репозиторій уже з новими запитами,
    а черга вебхука очищується лише від оновлень, які вже збережено в репозиторії."""
    acked = load_json(ACK_FILE, [])
    if acked and QUEUE_URL and QUEUE_SECRET:
        try:
            queue_call("POST", "/ack", {"ids": acked})
        except (urllib.error.URLError, OSError, ValueError) as exc:
            print(f"[ack] {exc}")  # не критично: оброблені оновлення відсіються за списком processed
    pending = load_json(FIRE_FILE, {})
    for name, ids in pending.items():
        error = None
        for attempt in range(3):
            try:
                error = fire_routine(name, "Нові запити: " + ", ".join(ids))
                break
            except (urllib.error.URLError, OSError) as exc:
                error = f"не вдалося запустити задачу «{name}»: {exc}"
                time.sleep(5 * (attempt + 1))
        if error:
            print(f"[routine] {error}")
            try:
                tg("sendMessage", chat_id=CHAT_ID,
                   text=f"⚠️ {error[0].upper() + error[1:]}. Повторю автоматично приблизно за 30 хвилин.")
            except SystemExit:
                pass
    return 0


if __name__ == "__main__":
    sys.exit(fire_pending() if "--fire" in sys.argv else Bot().run())
