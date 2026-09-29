# genshorts-archive_notes

Службова пам'ять пайплайна «Записки диванного архівіста» (https://t.me/archivist_notes).
Відео й матеріали лежать поза репозиторієм, токени — лише в секретах GitHub Actions.

## Хто що змінює

| Хто | Може змінювати | Не чіпає |
|---|---|---|
| Хмарний Claude (routines) | `data/ideas.json` (нові ідеї; поля `thread`, `script` своїх текстів), `data/texts/**` | `.github/**`, `data/stress_*.json`, статуси погодження |
| Диспетчер Telegram (`.github/bot/dispatch.py`, GitHub Actions) | статуси погодження, `revisions`, `data/stress_requests.json`, `.github/bot/state.json` | тексти |
| Мак (генерація відео) | поле `video`, `data/stress_dict.json`, `data/stress_requests.json` (позначає оброблені) | `.github/**` |

**Папку `.github/` змінює лише власник репозиторію вручну.**

## data/ideas.json

```json
{
  "ideas": [
    {
      "id": "2026-09-29-01",
      "proposed_at": "2026-09-29",
      "title": "Коротка назва теми",
      "summary": "2–3 речення: про що ролик і що корисного дізнається глядач",
      "why_now": "річниця / нове оцифрування / обговорення — чому саме зараз",
      "sources": ["https://..."],
      "status": "proposed | approved | rejected",
      "thread": {"status": "none | requested | ready | revise | approved | queued | posted", "file": "data/texts/<id>/thread.md"},
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

## Наголоси

- `data/stress_requests.json` — правки, які користувач надіслав відповіддю на відео (`слово → форма`).
- `data/stress_dict.json` — перевірений словник: слово → форма, яку синтезатор вимовляє правильно.
