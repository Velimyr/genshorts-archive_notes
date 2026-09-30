// Cloudflare Worker для пайплайна «Записки диванного архівіста».
//
// Telegram надсилає сюди кожне оновлення (вебхук). Воркер кладе його в чергу (KV) і
// одразу запускає диспетчер у GitHub Actions. Диспетчер забирає чергу через /pending і
// після обробки підтверджує через /ack. Cron кожні 5 хвилин: щогодинний запуск і
// точний запуск у момент запланованої публікації чи нагадування (/due).
//
// Токена бота тут немає. Секрети воркера: TG_WEBHOOK_SECRET (перевірка, що запит від
// Telegram), QUEUE_SECRET (доступ диспетчера до черги), GITHUB_TOKEN (fine-grained,
// лише Actions: write для одного репозиторію).

const JSON_HEADERS = { "Content-Type": "application/json" };

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

  // Кожні 5 хвилин: щогодинний запуск (о :00, 07:00–23:00 за Києвом) або запуск,
  // коли настав час запланованої публікації чи нагадування.
  async scheduled(event, env, ctx) {
    const moment = new Date(event.scheduledTime);
    const hourly = moment.getUTCMinutes() < 5 && moment.getUTCHours() >= 4 && moment.getUTCHours() <= 20;
    const due = Number((await env.QUEUE.get("due")) || 0);
    const dueNow = due > 0 && moment.getTime() >= due * 1000;
    if (dueNow) await env.QUEUE.delete("due");
    if (hourly || dueNow) ctx.waitUntil(triggerDispatcher(env, ""));
  },
};
