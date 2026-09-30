# genshorts-archive_notes

Службова пам'ять пайплайна «Записки диванного архівіста» (https://t.me/archivist_notes).
Відео й матеріали лежать поза репозиторієм, токени — лише в секретах GitHub Actions.

## Хто що змінює

| Хто | Може змінювати | Не чіпає |
|---|---|---|
| Хмарний Claude (routines) | `data/ideas.json` (нові ідеї; поле `thread` своїх текстів), `data/texts/**`, статус запитів у `data/requests.json` | `.github/**`, `data/stress_*.json`, статуси погодження |
| Диспетчер Telegram (`.github/bot/dispatch.py`, GitHub Actions) | статуси погодження, `revisions`, нові запити в `data/requests.json`, `data/stress_requests.json`, `.github/bot/state.json` | тексти |
| Мак (генерація відео) | поле `video`, `data/stress_dict.json`, `data/stress_requests.json` (позначає оброблені) | `.github/**` |

**Папку `.github/` змінює лише власник репозиторію вручну.**

## data/ideas.json

```json
{
  "ideas": [
    {
      "id": "2026-09-29-01",
      "proposed_at": "2026-09-29",
      "stream": "big | small | fun",
      "title": "Коротка назва теми",
      "summary": "2–3 речення: про що ролик і що корисного дізнається глядач",
      "why_now": "річниця / нове оцифрування / обговорення — чому саме зараз",
      "origin": "routine | author",
      "sources": ["https://..."],
      "images": [{"url": "пряме посилання на JPEG/PNG", "page": "сторінка файлу", "credit": "підпис", "license": "..."}],
      "status": "proposed | approved | rejected | cancelled",
      "thread": {"status": "none | requested | ready | revise | approved | queued | posted | cancelled", "file": "data/texts/<id>/thread.md"},
      "script": {"status": "none | requested | ready | revise | approved", "file": "data/texts/<id>/script.md"},
      "video":  {"status": "none | requested | rendered | regen | accepted", "telegram_message_id": null},
      "revisions": [{"kind": "thread | script | both", "text": "правки користувача", "at": "..."}],
      "history": [{"at": "...", "event": "..."}]
    }
  ]
}
```

- Нові ідеї додаються зі `status: "proposed"`, `thread/script/video.status: "none"`.
- Автор обирає в Telegram тему й формат → `requested`; задача «Тексти» пише файл і ставить `ready`; диспетчер надсилає текст.
- «✅ Готово» під текстом → `approved` (текст фінальний). Далі в меню «Публікуємо?»: тред → `queued` → `posted`; сценарій → `video.status: requested` → Мак генерує відео.
- `revise` означає: є нові правки в `revisions`, текст треба переписати й знову поставити `ready`.
- Ідеї ніколи не видаляються: весь список — це історія, з якою звіряються нові ідеї, щоб теми не повторювалися.

## Потоки й запити

- `stream`: `big` — великий серйозний тред (6–10 постів), `small` — малий серйозний (2–3), `fun` — жартівливий, іронічно-саркастичний (3–5). Ідея належить одному потоку.
- `data/requests.json` — запити на ідеї з Telegram: `{"ideas": [{"stream", "requested_at", "status": "pending | done", "done_at", "idea_ids"}]}`.
- У `thread.md` пости розділені рядком `---`; останній блок `Джерела:` — лише для автора, у Threads не публікується.
- Сценарії відео тимчасово не пишуться.
- `images` — до 3 фото з відкритою ліцензією для першого поста треду; зберігаються лише посилання (Threads забирає фото за публічною адресою). Автор обирає фото на фінальній перевірці; підпис `credit` у Threads не публікується.
- `origin: "author"` — тема, яку автор написав боту сам; джерела й фото знаходить задача «Тексти».

## Публікація за розкладом і аналітика

- Після фінальної перевірки автор обирає час: один із найближчих рекомендованих слотів (`data/recommendations.json` → `slots`, за Києвом) або «зараз». `approved_threads[id].publish_at` у `.github/bot/state.json`; `thread.status`: `queued` (час не обрано) → `scheduled` → `posted`.
- Воркер (cron кожні 5 хв) будить диспетчер у момент публікації чи нагадування (`/due`). Якщо на рекомендований слот нічого не заплановано — нагадування за годину.
- `data/analytics.json` — усі кореневі пости акаунта (бот і вручну, поле `origin`) і їхня статистика зрізами 24 год / 3 / 7 днів; оновлюється щогодини.
- Щонеділі о 18:00 задача «Аналітика» пише `data/reports/<дата>.md` (надсилається в Telegram) і оновлює рекомендації.

## Наголоси

- `data/stress_requests.json` — правки, які користувач надіслав відповіддю на відео (`слово → форма`).
- `data/stress_dict.json` — перевірений словник: слово → форма, яку синтезатор вимовляє правильно.
