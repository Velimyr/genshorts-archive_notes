"""Диспетчер Telegram для пайплайна «Записки диванного архівіста».

Запускається з GitHub Actions (див. .github/workflows/dispatch.yml) раз на 20 хвилин,
після змін у data/ і вручну. Рішень не приймає — веде діалог з користувачем кнопками
й переносить його вибір у data/ideas.json, а також запускає хмарні задачі через API:

1. Ідеї: о 09:00 питає «Готовий до ідей?» → запускає задачу «Ідеї» → надсилає 10 ідей.
2. Тексти: о 12:00 показує погоджені невикористані теми → користувач обирає тему й
   формат (тред / сценарій) → запускає задачу «Тексти» → надсилає текст на погодження.
3. Публікація: о 18:00 показує готові тексти → користувач обирає → бот ще раз показує
   текст → після підтвердження тред стає в чергу на Threads, сценарій — на відео.

Команди в будь-який момент: /ideas, /texts, /publish, /queue.
Службовий стан — у .github/bot/state.json. Токени — лише зі змінних середовища.
"""

import hashlib
import json
import os
import re
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[2]
IDEAS_FILE = ROOT / "data" / "ideas.json"
STRESS_FILE = ROOT / "data" / "stress_requests.json"
STATE_FILE = Path(__file__).resolve().parent / "state.json"

TOKEN = os.environ["TELEGRAM_BOT_TOKEN"].strip()
CHAT_ID = int(os.environ["TELEGRAM_CHAT_ID"].strip())
ROUTINES = {
    "ideas": (os.environ.get("ROUTINE_IDEAS_URL", "").strip(), os.environ.get("ROUTINE_IDEAS_TOKEN", "").strip()),
    "texts": (os.environ.get("ROUTINE_TEXTS_URL", "").strip(), os.environ.get("ROUTINE_TEXTS_TOKEN", "").strip()),
}

KYIV = ZoneInfo("Europe/Kyiv")
ASK_AT = {"ideas": 9, "texts": 12, "publish": 18}  # година за Києвом
TG_LIMIT = 4000
PART_NAMES = {"thread": "тред", "script": "сценарій"}
PART_ICONS = {"thread": "🧵", "script": "🎬"}


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


def history(idea: dict, event: str) -> None:
    idea.setdefault("history", []).append({"at": now(), "event": event})


class Bot:
    def __init__(self):
        self.ideas_doc = load_json(IDEAS_FILE, {"ideas": []})
        self.ideas = {i["id"]: i for i in self.ideas_doc["ideas"]}
        self.stress = load_json(STRESS_FILE, {"requests": []})
        self.state = load_json(STATE_FILE, {})
        for key, default in (("offset", 0), ("sent", {}), ("messages", {}), ("approved_threads", {}), ("asked", {})):
            self.state.setdefault(key, default)
        self.fire = {}  # routine -> set(ids)

    # ------------------------------------------------------------ відправка

    def send(self, text: str, markup=None, record: dict | None = None, reply_to: int | None = None):
        if len(text) > TG_LIMIT:
            text = text[: TG_LIMIT - 40] + "\n\n⚠️ Текст обрізано для Telegram."
        params = {"chat_id": CHAT_ID, "text": text, "link_preview_options": {"is_disabled": True}}
        if markup:
            params["reply_markup"] = markup
        if reply_to:
            params["reply_to_message_id"] = reply_to
        result = tg("sendMessage", **params)
        if record is not None:
            self.state["messages"][str(result["message_id"])] = record
        return result["message_id"]

    def close_buttons(self, message_id: int, label: str) -> None:
        tg("editMessageReplyMarkup", chat_id=CHAT_ID, message_id=message_id, reply_markup=kb([(label, "noop")]))

    def part_text(self, idea: dict, part: str) -> str | None:
        path = idea.get(part, {}).get("file")
        if path and (ROOT / path).is_file():
            return (ROOT / path).read_text(encoding="utf-8").strip()
        return None

    # ------------------------------------------------------------ списки тем

    def writable(self):
        """Погоджені теми, для яких ще немає треду чи сценарію."""
        rows = []
        for idea in self.ideas.values():
            if idea.get("status") != "approved" or idea.get("test"):
                continue
            parts = [p for p in ("thread", "script") if idea.get(p, {}).get("status", "none") == "none"]
            if parts:
                rows.append((idea, parts))
        return rows

    def publishable(self):
        """Готові (погоджені вами) тексти, які ще не опубліковано й не пущено на відео."""
        rows = []
        for idea in self.ideas.values():
            if idea.get("test"):
                continue
            parts = []
            if idea.get("thread", {}).get("status") == "approved":
                parts.append("thread")
            if idea.get("script", {}).get("status") == "approved" and idea.get("video", {}).get("status", "none") == "none":
                parts.append("script")
            if parts:
                rows.append((idea, parts))
        return rows

    # ------------------------------------------------------------ меню

    def ask_ideas(self):
        self.send("☀️ Готовий до нових ідей?", kb([("✅ Так, шукай 10 ідей", "ideas:go"), ("Не сьогодні", "ideas:skip")]),
                  {"kind": "ask"})

    def menu_texts(self, quiet=False):
        rows = self.writable()
        if not rows:
            if not quiet:
                self.send("✍️ Немає погоджених тем без текстів. Спершу оберіть ідеї: /ideas")
            return
        lines, buttons = [], []
        for n, (idea, parts) in enumerate(rows, 1):
            lines.append(f"{n}. {idea['title']}")
            buttons.append([(f"{n} · {PART_ICONS[p]} {PART_NAMES[p].capitalize()}", f"write:{idea['id']}:{p}") for p in parts])
        self.send("✍️ Пишемо текст? Оберіть тему й формат:\n\n" + "\n".join(lines), kb(*buttons), {"kind": "menu"})

    def menu_publish(self, quiet=False):
        rows = self.publishable()
        if not rows:
            if not quiet:
                self.send("📣 Немає готових текстів для публікації. Написати: /texts")
            return
        lines, buttons = [], []
        for n, (idea, parts) in enumerate(rows, 1):
            lines.append(f"{n}. {idea['title']}")
            buttons.append([(f"{n} · {'🧵 Threads' if p == 'thread' else '🎬 Відео'}", f"pub:{idea['id']}:{p}") for p in parts])
        self.send("📣 Публікуємо? Готові тексти:\n\n" + "\n".join(lines), kb(*buttons), {"kind": "menu"})

    def show_queue(self):
        lines = []
        for idea in self.ideas.values():
            if idea.get("status") != "approved" or idea.get("test"):
                continue
            t = idea.get("thread", {}).get("status", "none")
            s = idea.get("script", {}).get("status", "none")
            v = idea.get("video", {}).get("status", "none")
            lines.append(f"• {idea['title']}\n  тред: {t} · сценарій: {s} · відео: {v}")
        self.send("📋 Погоджені теми\n\n" + ("\n".join(lines) or "порожньо"))

    def request_ideas(self):
        pending = self.state.get("ideas_requested_at")
        if pending and (datetime.now(timezone.utc) - datetime.fromisoformat(pending)).total_seconds() < 40 * 60:
            self.send("🔎 Ідеї вже шукаються — надішлю, щойно будуть готові.")
            return
        if self.start_routine("ideas", "запит нових ідей"):
            self.state["ideas_requested_at"] = now()
            self.send("🔎 Шукаю 10 ідей, це займе близько 10 хвилин.")

    def start_routine(self, name: str, note: str) -> bool:
        url, token = ROUTINES[name]
        if not (url and token):
            self.send(f"⚠️ Задача «{name}» не підключена (немає секретів ROUTINE_{name.upper()}_URL/TOKEN).")
            return False
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
                return True
        except urllib.error.HTTPError as exc:
            self.send(f"⚠️ Не вдалося запустити задачу «{name}»: HTTP {exc.code}.")
            return False

    # ------------------------------------------------------------ вхідні

    def on_callback(self, cq: dict) -> None:
        message = cq.get("message") or {}
        mid = message.get("message_id")
        if message.get("chat", {}).get("id") != CHAT_ID or cq.get("from", {}).get("id") != CHAT_ID:
            return
        data = cq.get("data", "")
        tg("answerCallbackQuery", callback_query_id=cq["id"])
        if data == "noop":
            return
        record = self.state["messages"].get(str(mid))
        video_owner = next((i for i in self.ideas.values() if i.get("video", {}).get("telegram_message_id") == mid), None)
        if record is None and video_owner is None:
            return
        parts = data.split(":")
        action = parts[0]

        if action == "ideas":
            self.close_buttons(mid, "✅ Шукаю" if parts[1] == "go" else "Добре, завтра")
            if parts[1] == "go":
                self.request_ideas()
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
            idea, part = self.ideas.get(parts[1]), parts[2]
            if idea and idea.get(part, {}).get("status", "none") == "none":
                idea.setdefault(part, {})["status"] = "requested"
                history(idea, f"{part} requested in telegram")
                self.fire.setdefault("texts", set()).add(idea["id"])
                self.send(f"✍️ Пишу {PART_NAMES[part]}: «{idea['title']}». Надішлю, щойно буде готовий.")
        elif action in ("thread", "script", "done"):
            # «✅ Готово» під текстом (а також кнопки старого формату).
            part = parts[2] if action == "done" else action
            idea = self.ideas.get(parts[1])
            if idea and record and idea.get(part, {}).get("status") in ("ready", "revise"):
                idea[part]["status"] = "approved"
                idea[part]["approved_sha"] = record.get("sha")
                history(idea, f"{part} text approved in telegram")
            self.close_buttons(mid, "✅ Текст готовий")
            self.send("Що далі?", kb([("➡️ До публікації", "go:publish"), ("✍️ Ще текст", "go:texts")]), {"kind": "nav"})
        elif action == "pub":
            idea, part = self.ideas.get(parts[1]), parts[2]
            body = self.part_text(idea, part) if idea else None
            if body is None:
                return
            digest = sha(body)
            confirm = ("✅ Публікувати в Threads" if part == "thread" else "🎬 Генерувати відео")
            self.send(f"{PART_ICONS[part]} Фінальна перевірка — «{idea['title']}»\n\n{body}",
                      kb([(confirm, f"confirm:{idea['id']}:{part}:{digest}"), ("❌ Скасувати", "cancel")]),
                      {"kind": "preview", "idea": idea["id"], "part": part, "sha": digest, "text": body})
        elif action == "confirm":
            idea, part, digest = self.ideas.get(parts[1]), parts[2], parts[3]
            if not idea or not record or record.get("sha") != digest:
                return
            if part == "thread" and idea.get("thread", {}).get("status") == "approved":
                # У чергу йде саме той текст, який користувач щойно бачив.
                self.state["approved_threads"][idea["id"]] = {"sha": digest, "text": record["text"],
                                                              "approved_at": now(), "posted": False}
                idea["thread"]["status"] = "queued"
                history(idea, "thread queued for Threads")
                self.close_buttons(mid, "✅ У черзі на Threads")
                self.send("🧵 Тред у черзі на публікацію. (Автопостинг у Threads ще підключаємо.)")
            elif part == "script" and idea.get("video", {}).get("status", "none") == "none":
                idea.setdefault("video", {})["status"] = "requested"
                history(idea, "video requested in telegram")
                self.close_buttons(mid, "✅ Відео в черзі")
                self.send("🎬 Сценарій у черзі на відео — Мак згенерує його, коли буде онлайн.")
        elif action == "cancel":
            self.close_buttons(mid, "Скасовано")
        elif action == "video" and video_owner is not None:
            video_owner["video"]["status"] = "accepted"
            history(video_owner, "video accepted in telegram")
            self.close_buttons(mid, "✅ Наголоси ок")

    def on_message(self, msg: dict) -> None:
        if msg.get("chat", {}).get("id") != CHAT_ID:
            return
        text = (msg.get("text") or "").strip()
        command = text.split()[0].split("@")[0] if text.startswith("/") else ""
        if command in ("/ideas",):
            return self.request_ideas()
        if command in ("/texts", "/text"):
            return self.menu_texts()
        if command in ("/publish",):
            return self.menu_publish()
        if command in ("/queue", "/start", "/help"):
            return self.show_queue() if command == "/queue" else self.send(
                "Команди: /ideas — нові ідеї, /texts — написати текст, /publish — публікація, /queue — стан тем.")

        reply_id = (msg.get("reply_to_message") or {}).get("message_id")
        if not text or not reply_id:
            return
        record = self.state["messages"].get(str(reply_id)) or {}
        video_owner = next((i for i in self.ideas.values() if i.get("video", {}).get("telegram_message_id") == reply_id), None)

        if video_owner is not None:
            pairs = []
            for line in text.splitlines():
                p = re.split(r"\s*(?:→|->|=>)\s*", line.strip(), maxsplit=1)
                if len(p) == 2 and p[0] and p[1]:
                    pairs.append((p[0].strip(" ,.;"), p[1].strip(" ,.;")))
            if not pairs:
                self.send("Не бачу правок. Формат: слово → сло́во (кожна з нового рядка).", reply_to=msg["message_id"])
                return
            for word, form in pairs:
                self.stress["requests"].append({"idea": video_owner["id"], "word": word, "form": form, "at": now()})
            video_owner["video"]["status"] = "regen"
            history(video_owner, f"stress corrections: {len(pairs)}")
            self.send(f"✏️ Прийнято правок: {len(pairs)}. Відео перегенерується, щойно Мак буде онлайн.",
                      reply_to=msg["message_id"])
            return

        if record.get("kind") in ("thread", "script"):
            idea, part = self.ideas.get(record["idea"]), record["kind"]
            if idea and idea.get(part, {}).get("status") in ("ready", "revise"):
                idea.setdefault("revisions", []).append({"kind": part, "text": text, "at": now()})
                idea[part]["status"] = "revise"
                history(idea, f"{part} revision requested")
                self.fire.setdefault("texts", set()).add(idea["id"])
                self.send("✏️ Правки прийнято, нова версія прийде сюди.", reply_to=msg["message_id"])

    # ------------------------------------------------------------ вихідні

    def send_new_ideas(self):
        fresh = [i for i in self.ideas.values() if i.get("status") == "proposed" and f"idea:{i['id']}" not in self.state["sent"]]
        if not fresh:
            return
        self.state.pop("ideas_requested_at", None)
        self.send(f"💡 Нові ідеї: {len(fresh)}. Позначте ✅ ті, що подобаються.")
        for idea in fresh:
            sources = "\n".join(f"• {s}" for s in idea.get("sources", [])[:4])
            text = f"💡 {idea['title']}\n\n{idea.get('summary', '')}"
            if idea.get("why_now"):
                text += f"\n\nЧому зараз: {idea['why_now']}"
            if sources:
                text += f"\n\nДжерела:\n{sources}"
            mid = self.send(text, kb([("✅ Беру", f"idea:{idea['id']}:yes"), ("❌ Ні", f"idea:{idea['id']}:no")]),
                            {"kind": "idea", "idea": idea["id"]})
            self.state["sent"][f"idea:{idea['id']}"] = mid
        self.send("Коли оберете — переходьте до текстів.", kb([("➡️ До текстів", "go:texts")]), {"kind": "nav"})

    def send_ready_texts(self):
        for idea in self.ideas.values():
            for part in ("thread", "script"):
                if idea.get(part, {}).get("status") != "ready":
                    continue
                body = self.part_text(idea, part)
                if body is None:
                    continue
                digest = sha(body)
                key = f"{part}:{idea['id']}:{digest}"
                if key in self.state["sent"]:
                    continue
                title = "🧵 Тред для Threads" if part == "thread" else "🎬 Сценарій відео"
                mid = self.send(f"{title} — «{idea['title']}»\n\n{body}\n\n↩️ Правки — відповіддю на це повідомлення.",
                                kb([("✅ Готово", f"done:{idea['id']}:{part}")]),
                                {"kind": part, "idea": idea["id"], "sha": digest})
                self.state["sent"][key] = mid

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

        self.send_new_ideas()
        self.send_ready_texts()
        self.scheduled_prompts()
        for name, ids in self.fire.items():
            self.start_routine(name, "Змінилися ідеї: " + ", ".join(sorted(ids)))

        save_json(IDEAS_FILE, self.ideas_doc)
        save_json(STRESS_FILE, self.stress)
        save_json(STATE_FILE, self.state)
        print(f"[dispatch] updates: {len(updates)}, fired: {sorted(self.fire)}")
        return 0


if __name__ == "__main__":
    sys.exit(Bot().run())
