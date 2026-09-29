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
3. Публікація (18:00): готові треди за потоками → бот ще раз показує текст →
   після підтвердження тред стає в чергу на Threads.

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
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[2]
IDEAS_FILE = ROOT / "data" / "ideas.json"
REQUESTS_FILE = ROOT / "data" / "requests.json"
STRESS_FILE = ROOT / "data" / "stress_requests.json"
STATE_FILE = Path(__file__).resolve().parent / "state.json"
FIRE_FILE = Path(os.environ.get("RUNNER_TEMP", "/tmp")) / "genshorts_fire.json"

TOKEN = os.environ["TELEGRAM_BOT_TOKEN"].strip()
CHAT_ID = int(os.environ["TELEGRAM_CHAT_ID"].strip())
ROUTINES = {
    "ideas": (os.environ.get("ROUTINE_IDEAS_URL", "").strip(), os.environ.get("ROUTINE_IDEAS_TOKEN", "").strip()),
    "texts": (os.environ.get("ROUTINE_TEXTS_URL", "").strip(), os.environ.get("ROUTINE_TEXTS_TOKEN", "").strip()),
}

THREADS_TOKEN = os.environ.get("THREADS_ACCESS_TOKEN", "").strip()
THREADS_USER_ID = os.environ.get("THREADS_USER_ID", "").strip()
THREADS_TOKEN_UPDATED = os.environ.get("THREADS_TOKEN_UPDATED", "").strip()  # YYYY-MM-DD
THREADS_API = "https://graph.threads.net/v1.0"
THREADS_POST_LIMIT = 500

KYIV = ZoneInfo("Europe/Kyiv")
ASK_AT = {"ideas": 9, "texts": 12, "publish": 18}  # година за Києвом
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
        if method in ("answerCallbackQuery", "editMessageReplyMarkup"):
            print(f"[tg] {method}: {description}")
            return None
        raise SystemExit(f"[tg] {method} failed: {description}")


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
        lines = ["📋 Погоджені теми"]
        for stream, ideas in self.grouped(lambda i: i.get("status") == "approved"):
            lines.append(f"\n{stream_label(stream)}")
            for idea in ideas:
                lines.append(f"• {idea['title']} — тред: {idea.get('thread', {}).get('status', 'none')}")
        self.send("\n".join(lines) if len(lines) > 1 else "📋 Погоджених тем немає.")

    def request_ideas(self, stream: str):
        pending = [r for r in self.requests["ideas"] if r.get("status") == "pending"]
        if pending:
            self.send(f"🔎 Уже шукаю ідеї ({stream_label(pending[-1]['stream'])}) — надішлю, щойно будуть готові.")
            return
        self.requests["ideas"].append({"stream": stream, "requested_at": now(), "status": "pending"})
        self.fire.setdefault("ideas", set()).add(stream)
        self.send(f"🔎 Шукаю 10 ідей: {stream_label(stream)}. Це займе близько 10 хвилин.")

    def send_preview(self, idea: dict, body: str, topic: str) -> None:
        digest = sha(body)
        topic_line = (f"🏷 Тема: {topic}" if topic else "🏷 Тема не задана — без неї публікація недоступна.")
        buttons = [("✅ Публікувати в Threads", f"confirm:{idea['id']}:{digest}")] if topic else []
        self.send(f"{stream_label(idea.get('stream'))} · фінальна перевірка — «{idea['title']}»\n\n{body}"
                  f"\n\nℹ️ Блок «Джерела» в Threads не публікується.\n{topic_line}\n"
                  "↩️ Щоб задати чи змінити тему — відповідайте на це повідомлення її назвою (без #).",
                  kb(buttons + [("❌ Скасувати", "cancel")]),
                  {"kind": "preview", "idea": idea["id"], "sha": digest, "text": body, "topic": topic})

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
            self.send("Що далі?", kb([("➡️ До публікації", "go:publish"), ("✍️ Ще тред", "go:texts")]), {"kind": "nav"})
        elif action == "pub":
            idea = self.ideas.get(parts[1])
            body = self.thread_text(idea) if idea else None
            if body is None or idea.get("thread", {}).get("status") != "approved":
                return
            if unverified(body):
                idea["thread"]["status"] = "ready"
                self.send(f"⚠️ «{idea['title']}»: у треді є [ПЕРЕВІРИТИ], не публікую. {UNVERIFIED_HINT}")
                return
            self.send_preview(idea, body, self.state.get("last_topic", ""))
        elif action == "confirm":
            idea, digest = self.ideas.get(parts[1]), parts[2]
            if not idea or record.get("sha") != digest or idea.get("thread", {}).get("status") != "approved":
                return
            # У чергу йде саме той текст, який користувач щойно бачив.
            self.state["approved_threads"][idea["id"]] = {"sha": digest, "text": record["text"],
                                                          "topic": record.get("topic", ""),
                                                          "approved_at": now(), "posted": False}
            idea["thread"]["status"] = "queued"
            history(idea, "thread queued for Threads")
            self.close_buttons(mid, "✅ У черзі на Threads")
            self.send("🧵 Тред у черзі — публікую в Threads." if THREADS_TOKEN and THREADS_USER_ID
                      else "🧵 Тред у черзі. Threads ще не підключено (немає секретів THREADS_*).",
                      kb([("📣 Ще публікація", "go:publish")]), {"kind": "nav"})
        elif action == "cancel":
            self.close_buttons(mid, "Скасовано")

    def on_message(self, msg: dict) -> None:
        if msg.get("chat", {}).get("id") != CHAT_ID:
            return
        text = (msg.get("text") or "").strip()
        command = text.split()[0].split("@")[0] if text.startswith("/") else ""
        if command == "/ideas":
            return self.ask_ideas()
        if command in ("/texts", "/text"):
            return self.menu_texts()
        if command == "/publish":
            return self.menu_publish()
        if command == "/queue":
            return self.show_queue()
        if command in ("/start", "/help"):
            return self.send("Команди: /ideas — нові ідеї, /texts — написати тред, /publish — публікація, /queue — стан тем.")

        reply_id = (msg.get("reply_to_message") or {}).get("message_id")
        record = self.state["messages"].get(str(reply_id)) if reply_id else None
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
            marks = unverified(body)
            warning = f"\n\n⚠️ Є позначки [ПЕРЕВІРИТИ] ({marks}). {UNVERIFIED_HINT}" if marks else ""
            mid = self.send(
                f"{stream_label(idea.get('stream'))} · тред — «{idea['title']}»\n\n{body}{warning}\n\n"
                "↩️ Правки — відповіддю на це повідомлення.",
                kb([("✅ Готово", f"done:{idea['id']}")]),
                {"kind": "thread", "idea": idea["id"], "sha": digest})
            self.state["sent"][key] = mid

    def publish_queued(self):
        """Публікує один тред за запуск; прогрес зберігає після кожного поста."""
        if not (THREADS_TOKEN and THREADS_USER_ID):
            return
        for idea_id, item in self.state["approved_threads"].items():
            if item.get("posted") or item.get("failed"):
                continue
            idea = self.ideas.get(idea_id, {"title": idea_id})
            if unverified(item["text"]):
                item["failed"] = "у тексті є [ПЕРЕВІРИТИ]"
                self.send(f"⚠️ Не публікую «{idea['title']}»: у тексті є [ПЕРЕВІРИТИ].")
                return
            posts = thread_posts(item["text"])
            too_long = [n for n, p in enumerate(posts, 1) if threads_length(p) > THREADS_POST_LIMIT]
            if not posts or too_long:
                item["failed"] = f"пости {too_long} довші за {THREADS_POST_LIMIT} символів" if too_long else "порожній тред"
                self.send(f"⚠️ Не публікую «{idea['title']}»: {item['failed']}.")
                return
            posted = item.setdefault("posted_ids", [])
            try:
                for text in posts[len(posted):]:
                    params = {"media_type": "TEXT", "text": text}
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
            else:
                self.menu_publish(quiet=True)

    # ------------------------------------------------------------ запуск

    def run(self) -> int:
        updates = tg("getUpdates", offset=self.state["offset"], timeout=0,
                     allowed_updates=["message", "callback_query"]) or []
        for update in updates:
            self.state["offset"] = update["update_id"] + 1
            if "callback_query" in update:
                self.on_callback(update["callback_query"])
            elif "message" in update:
                self.on_message(update["message"])

        self.publish_queued()
        self.send_new_ideas()
        self.send_ready_threads()
        self.token_reminder()
        self.scheduled_prompts()

        save_json(IDEAS_FILE, self.ideas_doc)
        save_json(REQUESTS_FILE, self.requests)
        save_json(STRESS_FILE, self.stress)
        save_json(STATE_FILE, self.state)
        save_json(FIRE_FILE, {name: sorted(ids) for name, ids in self.fire.items()})
        print(f"[dispatch] updates: {len(updates)}, to fire: {sorted(self.fire)}")
        return 0


def fire_pending() -> int:
    """Окремий крок після пушу: задачі клонують репозиторій уже з новими запитами."""
    pending = load_json(FIRE_FILE, {})
    for name, ids in pending.items():
        error = fire_routine(name, "Нові запити: " + ", ".join(ids))
        if error:
            tg("sendMessage", chat_id=CHAT_ID, text=f"⚠️ {error}.")
    return 0


if __name__ == "__main__":
    sys.exit(fire_pending() if "--fire" in sys.argv else Bot().run())
