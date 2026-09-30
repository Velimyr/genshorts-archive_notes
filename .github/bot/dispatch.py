"""Диспетчер Telegram для пайплайна «Записки диванного архівіста».

Запускається з GitHub Actions (див. .github/workflows/dispatch.yml) раз на 20 хвилин,
після змін у data/ і вручну. Рішень не приймає — веде діалог з користувачем кнопками
й переносить його вибір у data/, а хмарні задачі запускає через API.

Три потоки тредів: 🧵 великий серйозний, 📌 малий серйозний, 😏 жартівливий.
Кожна ідея належить одному потоку.

1. Ідеї (09:00): «Готовий до ідей?» → користувач обирає потік → запит у
   data/requests.json → задача «Ідеї» шукає 10 ідей цього потоку → ✅/❌.
2. Тексти (12:00): погоджені теми без треду, згруповані за потоками → користувач
   обирає тему → задача «Тексти» пише тред → «✅ Готово» або правки відповіддю.
3. Публікація: готові треди за потоками → фінальна перевірка (тема, фото) → час:
   найближчий рекомендований слот або «зараз». Заплановане публікується саме
   (воркер будить диспетчер у потрібний момент). Якщо на рекомендований час нічого
   не заплановано — нагадування за годину.
4. Аналітика: щогодини статистика всіх постів акаунта в data/analytics.json;
   щонеділі о 18:00 задача «Аналітика» пише звіт і оновлює data/recommendations.json.

Команди: /ideas, /texts, /publish, /queue.
Режим `--fire` (окремий крок workflow після пушу) запускає хмарні задачі, щоб вони
бачили вже збережені в репозиторії запити.
"""

import hashlib
import json
import os
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
REPORTS_DIR = ROOT / "data" / "reports"
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
MENU_VERSION = 2
MENU_BUTTONS = {"💡 Ідеї": "/ideas", "✍️ Тексти": "/texts", "📣 Публікація": "/publish", "📋 Черга": "/queue",
                "📤 Опубліковані": "/published"}
MAIN_KEYBOARD = {"keyboard": [[{"text": "💡 Ідеї"}, {"text": "✍️ Тексти"}], [{"text": "📣 Публікація"}, {"text": "📋 Черга"}],
                              [{"text": "📤 Опубліковані"}]],
                 "is_persistent": True, "resize_keyboard": True}
HELP = ("Кнопки внизу: 💡 Ідеї — нові ідеї, ✍️ Тексти — написати тред, 📣 Публікація — опублікувати, "
        "📋 Черга — стан тем, 📤 Опубліковані — треди в Threads зі статистикою.\n"
        "Своя тема — просто напишіть її звичайним повідомленням.")
ASK_AT = {"ideas": 9, "texts": 12}  # година за Києвом
REPORT_AT = (6, 18)  # неділя, 18:00 за Києвом — тижневий звіт
SLOT_GAP = timedelta(hours=4)  # мінімальний проміжок між публікаціями бота
DAILY_LIMIT = 2  # тредів бота на день: 1 серйозний + 1 жартівливий, або 2 жартівливі
ALTERNATION_HORIZON = timedelta(days=3)  # скільки шукати слот із чергуванням, перш ніж поступитися
REMIND_BEFORE = timedelta(hours=1)  # нагадування, якщо на рекомендований час нічого не заплановано
WEEKDAYS = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Нд"]
METRICS = "views,likes,replies,reposts,quotes,shares"
TG_LIMIT = 3900
STREAMS = {
    "big": ("🧵", "Великий серйозний"),
    "small": ("📌", "Малий серйозний"),
    "fun": ("😏", "Жартівливий"),
}


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


def split_for_telegram(text: str) -> list[str]:
    """Ділить довгий текст на частини до TG_LIMIT, не розриваючи пости треду."""
    parts, current = [], ""
    for block in text.split("\n\n"):
        candidate = f"{current}\n\n{block}" if current else block
        if len(candidate) <= TG_LIMIT:
            current = candidate
            continue
        if current:
            parts.append(current)
        while len(block) > TG_LIMIT:
            parts.append(block[:TG_LIMIT])
            block = block[TG_LIMIT:]
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
    def __init__(self, message: str, status: int = 0):
        super().__init__(message)
        self.status = status


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
        raise ThreadsError(f"HTTP {exc.code} {error.get('message', '')}".strip(), exc.code) from None


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

    # ------------------------------------------------------------ відправка

    def send(self, text: str, markup=None, record: dict | None = None, reply_to: int | None = None) -> int:
        """Надсилає текст (довгий — кількома повідомленнями); кнопки й запис — на останньому."""
        pieces = split_for_telegram(text)
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
        self.send("☀️ Готовий до нових ідей? Оберіть потік:", kb(*rows), {"kind": "ask"})

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

    def show_queue(self):
        """Зведення тем за стадіями з кнопками дій."""
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
            item = items.get(idea["id"], {})
            if thread == "none":
                stages[0][3].append((idea, ""))
            elif thread in ("requested", "revise"):
                stages[1][3].append((idea, ""))
            elif thread == "ready":
                stages[2][3].append((idea, ""))
            elif thread == "approved" or (thread == "queued" and item.get("publish_at") == "pending"):
                stages[3][3].append((idea, ""))
            elif thread == "scheduled" and item.get("publish_at") not in (None, "pending"):
                stages[4][3].append((idea, fmt_slot(parse_time(item["publish_at"]))))
        stages[4][3].sort(key=lambda x: parse_time(items[x[0]["id"]]["publish_at"]))
        lines, buttons, n = ["📋 Стан тем"], [], 0
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
            self.send("📋 Погоджених тем немає. Нові ідеї — 💡 Ідеї.")
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
        elif thread == "approved" or (thread == "queued" and item.get("publish_at") == "pending"):
            lines.append("✅ Тред готовий, час публікації не обрано.")
            rows = [[("🕒 Запланувати", f"pub:{iid}")]]
        elif thread == "scheduled" and item.get("publish_at") not in (None, "pending"):
            lines.append(f"🕒 Заплановано на {fmt_slot(parse_time(item['publish_at']))}.")
            rows = [[("⬆️ Раніше", f"move:{iid}:up"), ("⬇️ Пізніше", f"move:{iid}:down")],
                    [("🖼 Фото й тема", f"pub:{iid}")],
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
        if thread in ("ready", "approved", "queued", "scheduled"):
            count = len(idea.get("images", []))
            rows.append([(f"🔄 Знайти інші фото (зараз {count})", f"photos:{iid}:more") if count
                         else ("🖼 Знайти фото", f"photos:{iid}"),
                         ("🔎 Фото за словами", f"photokw:{iid}")])
        if thread not in ("posted", "cancelled") and idea.get("status") != "cancelled":
            rows.append([("🗑 Скасувати тему", f"drop:{iid}")])
        self.send("\n".join(lines), kb(*rows) if rows else None, {"kind": "item", "idea": iid})

    def request_photos(self, idea: dict, more: bool = False, keywords: str = "") -> None:
        """Запит на пошук фото; «інші» й пошук за словами виключають уже показані фото."""
        if idea.get("photo_request"):
            self.send(f"🔎 Фото для «{idea['title']}» вже шукаються — надішлю, щойно знайду.")
            return
        if more or keywords:
            excluded = idea.setdefault("images_excluded", [])
            excluded += [i.get("url") for i in idea.get("images", []) if i.get("url") and i.get("url") not in excluded]
        idea["photo_request"] = True
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
        if item.get("posted") or item.get("posted_ids") or item.get("publish_at") in (None, "pending"):
            return
        current = parse_time(item["publish_at"])
        earliest = datetime.now(KYIV) + timedelta(minutes=15)
        slots = [s for s in self.recommended_slots(days=21) if s >= earliest]
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

    def send_preview(self, idea: dict, body: str, topic: str) -> None:
        digest = sha(body)
        photos = self.show_photos(idea)
        topic_line = (f"🏷 Тема: {topic}" if topic else "🏷 Тема не задана — без неї публікація недоступна.")
        photo_line = (f"\n📷 Фото до першого поста: варіанти {', '.join(map(str, photos))} вище." if photos else "")
        rows = []
        if topic:
            rows = [[(f"🕒 Запланувати з фото {n}", f"confirm:{idea['id']}:{digest}:{n}")] for n in photos]
            rows.append([("🕒 Запланувати без фото", f"confirm:{idea['id']}:{digest}:0")])
        rows.append([("🔄 Інші фото", f"photos:{idea['id']}:more") if photos else ("🖼 Знайти фото", f"photos:{idea['id']}"),
                     ("🔎 Фото за словами", f"photokw:{idea['id']}")])
        rows.append([("❌ Скасувати", "cancel")])
        self.send(f"{stream_label(idea.get('stream'))} · фінальна перевірка — «{idea['title']}»\n\n{body}"
                  f"\n\nℹ️ Блок «Джерела» в Threads не публікується.{photo_line}\n{topic_line}\n"
                  "🕒 Після вибору фото тред автоматично стане на найближчу вільну рекомендовану годину.\n"
                  "↩️ Щоб задати чи змінити тему — відповідайте на це повідомлення її назвою (без #).",
                  kb(*rows),
                  {"kind": "preview", "idea": idea["id"], "sha": digest, "text": body, "topic": topic})

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

    def recommended_slots(self, days: int = 7) -> list[datetime]:
        recs = load_json(RECS_FILE, {"slots": []})
        start = datetime.now(KYIV)
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
            at = item.get("publish_at")
            if not item.get("posted") and not item.get("failed") and at and at != "pending":
                items.append((parse_time(at), idea_id, item))
        return sorted(items, key=lambda x: x[0])

    @staticmethod
    def category(idea: dict | None) -> str:
        """«S» — серйозний (великий чи малий), «F» — жартівливий."""
        return "F" if (idea or {}).get("stream") == "fun" else "S"

    def bot_publications(self, exclude: str | None = None) -> list[tuple[datetime, str]]:
        """Заплановані й уже опубліковані треди бота: (момент, категорія)."""
        result = []
        for idea_id, item in self.state["approved_threads"].items():
            if idea_id == exclude or item.get("failed"):
                continue
            at = item.get("posted_at") if item.get("posted") else item.get("publish_at")
            if at and at != "pending":
                result.append((parse_time(at), self.category(self.ideas.get(idea_id))))
        return sorted(result)

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

    def free_slots(self, count: int = 3, idea: dict | None = None) -> list[datetime]:
        """Рекомендовані слоти за правилами: ліміт на день, проміжок, чергування категорій.
        Чергування поступається лише тоді, коли в найближчі дні підходящого слоту немає."""
        cat = self.category(idea)
        pubs = self.bot_publications(exclude=(idea or {}).get("id"))
        slots = self.recommended_slots()
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
                  "(до 2 тредів на день, 1 серйозний, серйозні й жартівливі по черзі).",
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

        if action == "ideas":
            if parts[1] in STREAMS:
                self.close_buttons(mid, f"✅ {stream_label(parts[1])}")
                self.request_ideas(parts[1])
            else:
                self.close_buttons(mid, "Добре, іншим разом")
        elif action == "go":
            self.close_buttons(mid, "➡️")
            self.menu_texts() if parts[1] == "texts" else self.menu_publish()
        elif action == "idea":
            idea = self.ideas.get(parts[1])
            if idea and idea.get("status") == "proposed":
                idea["status"] = "approved" if parts[2] == "yes" else "rejected"
                history(idea, f"idea {idea['status']} in telegram")
            self.close_buttons(mid, "✅ Погоджено" if parts[2] == "yes" else "❌ Відхилено")
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
            if previous.get("posted") or previous.get("posted_ids"):
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
            idea["thread"]["status"] = "queued"
            history(idea, "thread approved for Threads, awaiting time")
            self.close_buttons(mid, "✅ Текст і фото обрано")
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
            if parts[2] == "now":
                item["publish_at"] = now()
                label = "⚡ Публікую зараз"
            else:
                item["publish_at"] = datetime.fromtimestamp(int(parts[2]), KYIV).isoformat()
                label = f"🕒 {fmt_slot(parse_time(item['publish_at']))}"
            idea["thread"]["status"] = "scheduled"
            history(idea, f"thread scheduled: {item['publish_at']}")
            self.close_buttons(mid, label)
            if parts[2] != "now":
                self.send(f"🕒 «{idea['title']}» заплановано на {label[2:]}. Змінити чи зняти — у 📋 Черга.")
        elif action == "unq":
            idea, item = self.ideas.get(parts[1]), self.state["approved_threads"].get(parts[1])
            if not idea or not item or item.get("posted") or item.get("posted_ids"):
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
        elif action == "photokw":
            idea = self.ideas.get(parts[1])
            if idea:
                self.send(f"🔎 Напишіть ключові слова для пошуку фото до «{idea['title']}» — "
                          "відповіддю на це повідомлення (напр. «метрична книга 1850 Київська губернія»).",
                          None, {"kind": "photokw", "idea": idea["id"]})
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
        elif action == "cancel":
            self.close_buttons(mid, "Скасовано")

    def on_message(self, msg: dict) -> None:
        if msg.get("chat", {}).get("id") != CHAT_ID:
            return
        text = (msg.get("text") or "").strip()
        text = MENU_BUTTONS.get(text, text)
        command = text.split()[0].split("@")[0] if text.startswith("/") else ""
        if command == "/ideas":
            return self.ask_ideas()
        if command in ("/texts", "/text"):
            return self.menu_texts()
        if command == "/publish":
            return self.menu_publish()
        if command == "/queue":
            return self.show_queue()
        if command == "/published":
            return self.show_published()
        if command in ("/start", "/help"):
            return self.send(HELP, MAIN_KEYBOARD)
        if command:
            return

        reply_id = (msg.get("reply_to_message") or {}).get("message_id")
        record = self.state["messages"].get(str(reply_id)) if reply_id else None
        if text and record and record.get("kind") in ("photos", "photokw"):
            idea = self.ideas.get(record["idea"])
            if idea:
                self.request_photos(idea, keywords=text[:200])
            return
        if text and record and record.get("kind") == "preview":
            topic = text.lstrip("#").strip()
            if not (1 <= len(topic) <= 50) or "." in topic or "&" in topic:
                self.send("⚠️ Тема має бути до 50 символів, без крапок і «&».", reply_to=msg["message_id"])
                return
            idea = self.ideas.get(record["idea"])
            if idea and idea.get("thread", {}).get("status") == "approved":
                self.state["last_topic"] = topic
                self.close_buttons(reply_id, f"🏷 Тему змінено на «{topic}»")
                self.send_preview(idea, record["text"], topic)
            return
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

    # ------------------------------------------------------------ вихідні

    def send_new_ideas(self):
        fresh = [i for i in self.ideas.values() if i.get("status") == "proposed" and f"idea:{i['id']}" not in self.state["sent"]]
        if not fresh:
            return
        self.send(f"💡 Нові ідеї: {len(fresh)}. Позначте ✅ ті, що подобаються.")
        for idea in fresh:
            sources = "\n".join(f"• {s}" for s in idea.get("sources", [])[:4])
            text = f"{stream_label(idea.get('stream'))}\n💡 {idea['title']}\n\n{idea.get('summary', '')}"
            if idea.get("why_now"):
                text += f"\n\nЧому зараз: {idea['why_now']}"
            if sources:
                text += f"\n\nДжерела:\n{sources}"
            mid = self.send(text, kb([("✅ Беру", f"idea:{idea['id']}:yes"), ("❌ Ні", f"idea:{idea['id']}:no")]),
                            {"kind": "idea", "idea": idea["id"]})
            self.state["sent"][f"idea:{idea['id']}"] = mid
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
        return self.send(
            f"{stream_label(idea.get('stream'))} · тред — «{idea['title']}»\n\n{body}{warning}\n\n"
            "↩️ Правки — відповіддю на це повідомлення.",
            kb([("✅ Готово", f"done:{idea['id']}"), ("🗑 Скасувати тему", f"drop:{idea['id']}")]),
            {"kind": "thread", "idea": idea["id"], "sha": sha(body)})

    def publish_queued(self):
        """Публікує один тред за запуск; прогрес зберігає після кожного поста."""
        if not (THREADS_TOKEN and THREADS_USER_ID):
            return
        current = datetime.now(timezone.utc)
        for idea_id, item in self.state["approved_threads"].items():
            if item.get("posted") or item.get("failed"):
                continue
            at = item.get("publish_at")
            if at == "pending" or (at and parse_time(at) > current):
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
                    for _ in range(20):  # Threads радить дочекатися обробки контейнера
                        status = threads_retry("GET", container, fields="status,error_message")
                        if status.get("status") in ("FINISHED", "PUBLISHED"):
                            break
                        if status.get("status") in ("ERROR", "EXPIRED"):
                            raise ThreadsError(status.get("error_message") or status["status"])
                        time.sleep(3)
                    posted.append(threads_retry("POST", f"{THREADS_USER_ID}/threads_publish", creation_id=container)["id"])
                    save_json(STATE_FILE, self.state)
            except ThreadsError as exc:
                item["attempts"] = item.get("attempts", 0) + 1
                if item["attempts"] >= 3:
                    item["failed"] = str(exc)
                self.send(f"⚠️ Публікація «{idea['title']}» зупинилась на пості {len(posted) + 1}/{len(posts)}: {exc}."
                          + (" Більше не пробую." if item.get("failed") else " Спробую наступного запуску."))
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
        """Раз на годину: список постів акаунта й статистика; зрізи через 24 год, 3 і 7 днів."""
        if not (THREADS_TOKEN and THREADS_USER_ID):
            return
        last = self.state.get("analytics_at")
        if last and datetime.now(timezone.utc) - parse_time(last) < timedelta(minutes=50):
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

    def weekly_report(self) -> None:
        local = datetime.now(KYIV)
        week = local.strftime("%G-%V")
        if (local.weekday(), local.hour) >= REPORT_AT and local.weekday() == REPORT_AT[0] \
                and self.state.get("report_week") != week:
            self.state["report_week"] = week
            self.fire.setdefault("analytics", set()).add(week)
            self.send("📊 Готую тижневий звіт про ефективність тредів — надішлю, щойно буде готовий.")

    def send_reports(self) -> None:
        if not REPORTS_DIR.is_dir():
            return
        for path in sorted(REPORTS_DIR.glob("*.md")):
            key = f"report:{path.name}"
            if key not in self.state["sent"]:
                self.state["sent"][key] = self.send(path.read_text(encoding="utf-8").strip())

    def slot_reminders(self) -> None:
        current = datetime.now(KYIV)
        reminded = self.state.setdefault("reminded_slots", [])
        busy = [moment for moment, _, _ in self.scheduled()]
        for slot in self.recommended_slots(days=1):
            if not (timedelta(0) < slot - current <= REMIND_BEFORE + timedelta(minutes=5)):
                continue
            key = slot.isoformat()
            if key in reminded or any(abs(slot - b) < SLOT_GAP / 2 for b in busy):
                continue
            reminded.append(key)
            day_cats = [c for moment, c in self.bot_publications() if moment.astimezone(KYIV).date() == slot.date()]
            serious = "" if "S" in day_cats else " Серйозного треду на сьогодні теж немає."
            if self.grouped(lambda i: i.get("thread", {}).get("status") == "approved"):
                self.send(f"⏰ {slot:%H:%M} — рекомендований час публікації, але нічого не заплановано.{serious}",
                          kb([("📣 Обрати тред", "go:publish")]), {"kind": "nav"})
            else:
                self.send(f"⏰ {slot:%H:%M} — рекомендований час публікації, але готових тредів немає.{serious}",
                          kb([("✍️ Написати тред", "go:texts")]), {"kind": "nav"})
        self.state["reminded_slots"] = reminded[-50:]

    def next_due(self) -> int:
        """Найближчий момент, коли воркер має розбудити диспетчер: публікація чи нагадування."""
        current = datetime.now(KYIV)
        busy = [m for m, _, _ in self.scheduled()]
        moments = [m for m in busy if m > current]
        reminded = set(self.state.get("reminded_slots", []))
        for slot in self.recommended_slots(days=2):
            if any(abs(slot - b) < SLOT_GAP / 2 for b in busy):
                continue
            if slot.isoformat() not in reminded and slot - REMIND_BEFORE > current:
                moments.append(slot - REMIND_BEFORE)
        return int(min(moments).timestamp()) if moments else 0

    def send_photo_results(self) -> None:
        """Повідомляє про завершений пошук фото, запитаний кнопкою."""
        for idea in self.ideas.values():
            requested = idea.get("photo_requested_at")
            if not requested or idea.get("photo_request"):
                continue
            key = f"photos:{idea['id']}:{requested}"
            if key in self.state["sent"]:
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
            rows.append([("🔄 Знайти інші фото", f"photos:{idea['id']}:more"), ("🔎 Фото за словами", f"photokw:{idea['id']}")])
            text += "\n↩️ Або відповідайте на це повідомлення ключовими словами для нового пошуку."
            self.state["sent"][key] = self.send(text, kb(*rows), {"kind": "photos", "idea": idea["id"]})

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

    # ------------------------------------------------------------ запуск

    def ensure_menu(self) -> None:
        if self.state.get("menu_version") == MENU_VERSION:
            return
        tg("setMyCommands", commands=[
            {"command": "ideas", "description": "Нові ідеї"},
            {"command": "texts", "description": "Написати тред"},
            {"command": "publish", "description": "Публікація"},
            {"command": "queue", "description": "Стан тем"},
            {"command": "published", "description": "Опубліковані треди"},
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
        save_json(ACK_FILE, sorted(found))
        return [found[i] for i in sorted(found) if i not in done]

    def run(self) -> int:
        updates = self.collect_updates()
        for update in updates:
            self.state.setdefault("processed", []).append(update["update_id"])
            self.state["processed"] = self.state["processed"][-500:]
            if "callback_query" in update:
                self.on_callback(update["callback_query"])
            elif "message" in update:
                self.on_message(update["message"])

        self.ensure_menu()
        self.publish_queued()
        self.collect_analytics()
        self.send_new_ideas()
        self.send_ready_threads()
        self.send_reports()
        self.send_photo_results()
        self.token_reminder()
        self.scheduled_prompts()
        self.slot_reminders()
        self.weekly_report()

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
        print(f"[dispatch] updates: {len(updates)}, to fire: {sorted(self.fire)}")
        return 0


def fire_pending() -> int:
    """Окремий крок після пушу: задачі клонують репозиторій уже з новими запитами,
    а черга вебхука очищується лише від оновлень, які вже збережено в репозиторії."""
    acked = load_json(ACK_FILE, [])
    if acked and QUEUE_URL and QUEUE_SECRET:
        queue_call("POST", "/ack", {"ids": acked})
    pending = load_json(FIRE_FILE, {})
    for name, ids in pending.items():
        error = fire_routine(name, "Нові запити: " + ", ".join(ids))
        if error:
            tg("sendMessage", chat_id=CHAT_ID, text=f"⚠️ {error}.")
    return 0


if __name__ == "__main__":
    sys.exit(fire_pending() if "--fire" in sys.argv else Bot().run())
