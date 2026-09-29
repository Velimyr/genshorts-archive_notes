"""Диспетчер Telegram для пайплайна «Записки диванного архівіста».

Запускається з GitHub Actions (див. .github/workflows/dispatch.yml). Не приймає
рішень — лише переносить дані між репозиторієм і Telegram:

1. Читає нові натискання кнопок і відповіді з Telegram (тільки з TELEGRAM_CHAT_ID)
   і записує їх у data/ideas.json та data/stress_requests.json.
2. Надсилає в Telegram нові ідеї, треди й сценарії, які з'явилися в data/.

Службовий стан бота (offset, які повідомлення вже надіслано, погоджені треди)
лежить у .github/bot/state.json. Токени приходять лише зі змінних середовища.
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

ROOT = Path(__file__).resolve().parents[2]
IDEAS_FILE = ROOT / "data" / "ideas.json"
STRESS_FILE = ROOT / "data" / "stress_requests.json"
STATE_FILE = Path(__file__).resolve().parent / "state.json"

TOKEN = os.environ["TELEGRAM_BOT_TOKEN"].strip()
CHAT_ID = int(os.environ["TELEGRAM_CHAT_ID"].strip())
ROUTINE_TEXTS_URL = os.environ.get("ROUTINE_TEXTS_URL", "").strip()
ROUTINE_TEXTS_TOKEN = os.environ.get("ROUTINE_TEXTS_TOKEN", "").strip()

TG_LIMIT = 4000
DONE_LABELS = {
    ("idea", "yes"): "✅ Погоджено",
    ("idea", "no"): "❌ Відхилено",
    ("thread", "yes"): "✅ Тред погоджено",
    ("script", "yes"): "✅ Сценарій погоджено",
    ("video", "ok"): "✅ Наголоси ок",
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
    body = json.dumps(params).encode("utf-8")
    request = urllib.request.Request(
        f"https://api.telegram.org/bot{TOKEN}/{method}",
        data=body,
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


def buttons(*rows):
    return {"inline_keyboard": [[{"text": t, "callback_data": d} for t, d in row] for row in rows]}


def history(idea: dict, event: str) -> None:
    idea.setdefault("history", []).append({"at": now(), "event": event})


# ---------------------------------------------------------------- вхідні


def handle_callback(cq: dict, ideas: dict, state: dict) -> bool:
    message = cq.get("message") or {}
    if message.get("chat", {}).get("id") != CHAT_ID or cq.get("from", {}).get("id") != CHAT_ID:
        return False
    try:
        kind, idea_id, decision = cq.get("data", "").split(":", 2)
    except ValueError:
        return False
    if kind == "noop":
        tg("answerCallbackQuery", callback_query_id=cq["id"])
        return False
    idea = ideas.get(idea_id)
    record = state["messages"].get(str(message.get("message_id")), {})
    is_video_message = (
        kind == "video"
        and idea is not None
        and idea.get("video", {}).get("telegram_message_id") == message.get("message_id")
    )
    if idea is None or not (record.get("kind") == kind and record.get("idea") == idea_id or is_video_message):
        tg("answerCallbackQuery", callback_query_id=cq["id"], text="Невідоме повідомлення")
        return False

    if kind == "idea" and idea.get("status") == "proposed":
        idea["status"] = "approved" if decision == "yes" else "rejected"
        history(idea, f"idea {idea['status']} in telegram")
    elif kind == "thread" and decision == "yes":
        # Публікуємо лише той текст, який користувач бачив у цьому повідомленні.
        state["approved_threads"][idea_id] = {
            "sha": record["sha"],
            "text": record["text"],
            "approved_at": now(),
            "posted": False,
        }
        idea.setdefault("thread", {})["status"] = "approved"
        history(idea, "thread approved in telegram")
    elif kind == "script" and decision == "yes":
        idea.setdefault("script", {})["status"] = "approved"
        history(idea, "script approved in telegram")
    elif kind == "video" and decision == "ok":
        idea.setdefault("video", {})["status"] = "accepted"
        history(idea, "video accepted in telegram")

    tg("answerCallbackQuery", callback_query_id=cq["id"], text="Прийнято")
    label = DONE_LABELS.get((kind, decision), "✅")
    tg(
        "editMessageReplyMarkup",
        chat_id=CHAT_ID,
        message_id=message["message_id"],
        reply_markup=buttons([(label, "noop:-:-")]),
    )
    return kind == "idea" and decision == "yes"


def parse_stress(text: str):
    pairs = []
    for line in text.splitlines():
        parts = re.split(r"\s*(?:→|->|=>)\s*", line.strip(), maxsplit=1)
        if len(parts) == 2 and parts[0] and parts[1]:
            pairs.append((parts[0].strip(" ,.;"), parts[1].strip(" ,.;")))
    return pairs


def handle_message(msg: dict, ideas: dict, state: dict, stress: dict) -> bool:
    if msg.get("chat", {}).get("id") != CHAT_ID:
        return False
    text = (msg.get("text") or "").strip()
    reply_id = (msg.get("reply_to_message") or {}).get("message_id")
    if not text or not reply_id:
        if text == "/queue":
            send_queue(ideas)
        return False

    record = state["messages"].get(str(reply_id))
    if record is None:
        for idea in ideas.values():
            if idea.get("video", {}).get("telegram_message_id") == reply_id:
                record = {"kind": "video", "idea": idea["id"]}
    if record is None or record["idea"] not in ideas:
        return False
    idea = ideas[record["idea"]]

    if record["kind"] == "video":
        pairs = parse_stress(text)
        if not pairs:
            tg("sendMessage", chat_id=CHAT_ID, reply_to_message_id=msg["message_id"],
               text="Не бачу правок. Формат: слово → сло́во (кожна правка з нового рядка).")
            return False
        for word, form in pairs:
            stress["requests"].append({"idea": idea["id"], "word": word, "form": form, "at": now()})
        idea.setdefault("video", {})["status"] = "regen"
        history(idea, f"stress corrections: {len(pairs)}")
        tg("sendMessage", chat_id=CHAT_ID, reply_to_message_id=msg["message_id"],
           text=f"✏️ Прийнято правок: {len(pairs)}. Відео перегенерується, щойно Мак буде онлайн.")
        return False

    if record["kind"] in ("idea", "thread", "script"):
        kind = "script" if record["kind"] == "script" else "thread" if record["kind"] == "thread" else "both"
        idea.setdefault("revisions", []).append({"kind": kind, "text": text, "at": now()})
        for part in (["thread", "script"] if kind == "both" else [kind]):
            if idea.get(part, {}).get("status") not in ("approved", "posted"):
                idea.setdefault(part, {})["status"] = "revise"
        history(idea, f"revision requested: {kind}")
        tg("sendMessage", chat_id=CHAT_ID, reply_to_message_id=msg["message_id"],
           text="✏️ Правки прийнято, нова версія прийде сюди.")
        return True
    return False


def send_queue(ideas: dict) -> None:
    lines = []
    for idea in ideas.values():
        if idea.get("status") != "approved":
            continue
        t = idea.get("thread", {}).get("status", "none")
        s = idea.get("script", {}).get("status", "none")
        v = idea.get("video", {}).get("status", "none")
        lines.append(f"• {idea['title']}\n  тред: {t} · сценарій: {s} · відео: {v}")
    tg("sendMessage", chat_id=CHAT_ID, text="📋 Черга\n\n" + ("\n".join(lines) or "порожньо"))


# ---------------------------------------------------------------- вихідні


def send_tracked(state: dict, key: str, text: str, markup, record: dict) -> None:
    if len(text) > TG_LIMIT:
        tg("sendMessage", chat_id=CHAT_ID, text=f"⚠️ {key}: текст задовгий для Telegram ({len(text)} символів), не надіслано.")
        state["sent"][key] = None
        return
    result = tg("sendMessage", chat_id=CHAT_ID, text=text, reply_markup=markup,
                link_preview_options={"is_disabled": True})
    state["sent"][key] = result["message_id"]
    state["messages"][str(result["message_id"])] = record


def send_pending(ideas: dict, state: dict) -> None:
    fresh = [i for i in ideas.values() if i.get("status") == "proposed" and f"idea:{i['id']}" not in state["sent"]]
    if fresh:
        tg("sendMessage", chat_id=CHAT_ID, text=f"💡 Нові ідеї: {len(fresh)}. Погоджуйте кнопками; правки до ідеї — відповіддю на неї.")
    for idea in fresh:
        sources = "\n".join(f"• {s}" for s in idea.get("sources", [])[:4])
        text = f"💡 {idea['title']}\n\n{idea.get('summary', '')}"
        if idea.get("why_now"):
            text += f"\n\nЧому зараз: {idea['why_now']}"
        if sources:
            text += f"\n\nДжерела:\n{sources}"
        send_tracked(state, f"idea:{idea['id']}", text,
                     buttons([("✅ Беру", f"idea:{idea['id']}:yes"), ("❌ Ні", f"idea:{idea['id']}:no")]),
                     {"kind": "idea", "idea": idea["id"]})

    for idea in ideas.values():
        for kind, icon, label, action in (
            ("thread", "🧵 Тред для Threads", "✅ Публікувати", "yes"),
            ("script", "🎬 Сценарій відео", "✅ Генерувати відео", "yes"),
        ):
            part = idea.get(kind, {})
            path = ROOT / part.get("file", "") if part.get("file") else None
            if part.get("status") != "ready" or path is None or not path.is_file():
                continue
            body = path.read_text(encoding="utf-8").strip()
            digest = sha(body)
            key = f"{kind}:{idea['id']}:{digest}"
            if key in state["sent"]:
                continue
            text = f"{icon} — {idea['title']}\n\n{body}\n\n↩️ Правки — відповіддю на це повідомлення."
            send_tracked(state, key, text, buttons([(label, f"{kind}:{idea['id']}:{action}")]),
                         {"kind": kind, "idea": idea["id"], "sha": digest, "text": body})


def fire_texts_routine(ids) -> None:
    if not (ROUTINE_TEXTS_URL and ROUTINE_TEXTS_TOKEN):
        print("[routine] ROUTINE_TEXTS_URL/TOKEN не задано — пропускаю запуск")
        return
    request = urllib.request.Request(
        ROUTINE_TEXTS_URL,
        data=json.dumps({"text": "Є погоджені ідеї або правки: " + ", ".join(sorted(ids))}).encode(),
        headers={
            "Authorization": f"Bearer {ROUTINE_TEXTS_TOKEN}",
            "anthropic-beta": "experimental-cc-routine-2026-04-01",
            "anthropic-version": "2023-06-01",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=30):
            print("[routine] texts routine fired")
    except urllib.error.HTTPError as exc:
        print(f"[routine] fire failed: HTTP {exc.code}")


def main() -> int:
    ideas_doc = load_json(IDEAS_FILE, {"ideas": []})
    ideas = {i["id"]: i for i in ideas_doc["ideas"]}
    stress = load_json(STRESS_FILE, {"requests": []})
    state = load_json(STATE_FILE, {})
    for key, default in (("offset", 0), ("sent", {}), ("messages", {}), ("approved_threads", {})):
        state.setdefault(key, default)

    need_texts = set()
    updates = tg("getUpdates", offset=state["offset"], timeout=0,
                 allowed_updates=["message", "callback_query"]) or []
    for update in updates:
        state["offset"] = update["update_id"] + 1
        if "callback_query" in update:
            cq = update["callback_query"]
            if handle_callback(cq, ideas, state):
                need_texts.add(cq["data"].split(":")[1])
        elif "message" in update:
            msg = update["message"]
            reply_id = (msg.get("reply_to_message") or {}).get("message_id")
            if handle_message(msg, ideas, state, stress):
                need_texts.add(state["messages"].get(str(reply_id), {}).get("idea", "?"))

    send_pending(ideas, state)
    if need_texts:
        fire_texts_routine(need_texts)

    save_json(IDEAS_FILE, ideas_doc)
    save_json(STRESS_FILE, stress)
    save_json(STATE_FILE, state)
    print(f"[dispatch] updates: {len(updates)}, texts needed: {len(need_texts)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
