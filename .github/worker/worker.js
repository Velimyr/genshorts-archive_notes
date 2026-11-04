// Cloudflare Worker для пайплайна «Записки диванного архівіста».
//
// Telegram надсилає сюди кожне оновлення (вебхук). Воркер кладе його в чергу (KV) і
// одразу запускає диспетчер у GitHub Actions. Диспетчер забирає чергу через /pending і
// після обробки підтверджує через /ack. Cron кожні 5 хвилин: запуск у момент, який
// диспетчер попросив через /due (публікація, нагадування, ранкові питання),
// і двічі на добу — запобіжний запуск.
//
// Поки диспетчер чекає нових натискань до кінця оплаченої хвилини Actions, він тримає
// ключ "live" (до якого моменту опитує чергу). Тоді нове натискання лише кладеться в чергу,
// без нового запуску. Якщо диспетчер міг його не побачити — cron запускає диспетчер.
//
// Токена бота тут немає. Секрети воркера: TG_WEBHOOK_SECRET (перевірка, що запит від
// Telegram), QUEUE_SECRET (доступ диспетчера до черги), GITHUB_TOKEN (fine-grained,
// лише Actions: write для одного репозиторію).

const JSON_HEADERS = { "Content-Type": "application/json" };
const LIVE_MARGIN = 20;  // секунд: стільки щонайменше диспетчер ще має опитувати чергу
const SAFETY_HOURS_UTC = [5, 17];  // запобіжні запуски (≈08:00 і 20:00 за Києвом)

async function triggerDispatcher(env, update) {
  const response = await fetch(
    `https://api.github.com/repos/${env.GITHUB_REPO}/actions/workflows/dispatch.yml/dispatches`,
    {
      method: "POST",
      headers: {
        Authorization: `Bearer ${env.GITHUB_TOKEN}`,
        Accept: "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "genshorts-telegram-worker",
      },
      body: JSON.stringify({ ref: "main", inputs: { update } }),
    },
  );
  return response.status === 204;
}

function authorized(request, env) {
  return request.headers.get("Authorization") === `Bearer ${env.QUEUE_SECRET}`;
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (request.method === "POST" && url.pathname === "/telegram") {
      if (request.headers.get("X-Telegram-Bot-Api-Secret-Token") !== env.TG_WEBHOOK_SECRET) {
        return new Response("forbidden", { status: 403 });
      }
      const update = await request.json();
      if (typeof update.update_id !== "number") return new Response("ok");
      const key = `u:${String(update.update_id).padStart(12, "0")}`;
      await env.QUEUE.put(key, JSON.stringify(update), { expirationTtl: 7 * 86400 });
      // Диспетчер ще щонайменше LIVE_MARGIN секунд опитуватиме чергу — він і обробить.
      const nowSec = Date.now() / 1000;
      const live = Number((await env.QUEUE.get("live")) || 0);
      if (live - nowSec >= LIVE_MARGIN) {
        const skipped = Number((await env.QUEUE.get("skipped")) || 0);
        if (update.update_id > skipped) {
          await env.QUEUE.put("skipped", String(update.update_id), { expirationTtl: 86400 });
        }
        return new Response("ok");
      }
      // Якщо GitHub не прийняв запуск, відповідаємо помилкою — Telegram повторить доставку.
      const ok = await triggerDispatcher(env, JSON.stringify(update));
      return new Response(ok ? "ok" : "dispatch failed", { status: ok ? 200 : 502 });
    }

    if (request.method === "GET" && url.pathname === "/pending") {
      if (!authorized(request, env)) return new Response("forbidden", { status: 403 });
      const list = await env.QUEUE.list({ prefix: "u:" });
      const updates = [];
      for (const { name } of list.keys) {
        const value = await env.QUEUE.get(name);
        if (value) updates.push(JSON.parse(value));
      }
      return new Response(JSON.stringify(updates), { headers: JSON_HEADERS });
    }

    if (request.method === "POST" && url.pathname === "/ack") {
      if (!authorized(request, env)) return new Response("forbidden", { status: 403 });
      const { ids = [] } = await request.json();
      await Promise.all(ids.map((id) => env.QUEUE.delete(`u:${String(id).padStart(12, "0")}`)));
      return new Response(JSON.stringify({ deleted: ids.length }), { headers: JSON_HEADERS });
    }

    if (request.method === "POST" && url.pathname === "/live") {
      // Диспетчер: до якого моменту (unix-секунди) він опитує чергу; 0 — уже не опитує,
      // seen — найбільший update_id, який він обробив.
      if (!authorized(request, env)) return new Response("forbidden", { status: 403 });
      const { until = 0, seen = 0 } = await request.json();
      await env.QUEUE.put("live", String(until), { expirationTtl: 300 });
      if (seen > 0) await env.QUEUE.put("seen", String(seen), { expirationTtl: 86400 });
      return new Response(JSON.stringify({ live: until }), { headers: JSON_HEADERS });
    }

    if (request.method === "POST" && url.pathname === "/due") {
      // Диспетчер повідомляє найближчий момент публікації чи нагадування (unix-секунди).
      if (!authorized(request, env)) return new Response("forbidden", { status: 403 });
      const { at = 0 } = await request.json();
      if (at > 0) await env.QUEUE.put("due", String(at));
      else await env.QUEUE.delete("due");
      return new Response(JSON.stringify({ due: at }), { headers: JSON_HEADERS });
    }

    return new Response("not found", { status: 404 });
  },

  // Кожні 5 хвилин (робота воркера безкоштовна, рахуються лише запуски Actions):
  // - настав час, який диспетчер попросив через /due;
  // - запобіжний запуск двічі на добу (якщо /due загубився);
  // - натискання прийшло, поки диспетчер чекав, але він міг його не побачити.
  async scheduled(event, env, ctx) {
    const moment = new Date(event.scheduledTime);
    const nowSec = moment.getTime() / 1000;
    const safety = moment.getUTCMinutes() < 5 && SAFETY_HOURS_UTC.includes(moment.getUTCHours());
    const due = Number((await env.QUEUE.get("due")) || 0);
    const dueNow = due > 0 && nowSec >= due;
    if (dueNow) await env.QUEUE.delete("due");
    let missed = false;
    const skipped = Number((await env.QUEUE.get("skipped")) || 0);
    const live = Number((await env.QUEUE.get("live")) || 0);
    if (skipped > 0 && nowSec > live + 5) {
      const seen = Number((await env.QUEUE.get("seen")) || 0);
      missed = seen < skipped;  // диспетчер завершив чекати, не побачивши це натискання
      await env.QUEUE.delete("skipped");
    }
    if (safety || dueNow || missed) ctx.waitUntil(triggerDispatcher(env, ""));
  },
};
