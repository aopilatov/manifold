// Клиент admin-API. credentials:include — сессия в httpOnly-cookie.
// В dev Vite проксирует /admin → :8003; в prod статику раздаёт сам admin-сервер (один origin).

async function j<T = any>(path: string, opts?: RequestInit): Promise<T> {
  const r = await fetch(path, {
    credentials: "include",
    headers: { "content-type": "application/json" },
    ...opts,
  });
  if (!r.ok) throw new Error((await r.text()) || `HTTP ${r.status}`);
  return r.json();
}

export interface Info {
  node: string;
  num_connections: number;
  num_channels: number;
  messages_published: number;
  subscriptions: number;
  connections_opened: number;
  connections_closed: number;
}

export const api = {
  me: () => j<{ authenticated: boolean }>("/admin/me"),
  login: (password: string) => j("/admin/login", { method: "POST", body: JSON.stringify({ password }) }),
  info: () => j<Info>("/admin/info"),
  channels: () => j<{ channels: string[] }>("/admin/channels"),
  presence: (channel: string) =>
    j<{ users: string[] }>("/admin/presence", { method: "POST", body: JSON.stringify({ channel }) }),
  publish: (channel: string, data: string) =>
    j<{ offset: number; epoch: string }>("/admin/publish", {
      method: "POST",
      body: JSON.stringify({ channel, data }),
    }),
  disconnect: (user: string) =>
    j("/admin/disconnect", { method: "POST", body: JSON.stringify({ user }) }),
};
