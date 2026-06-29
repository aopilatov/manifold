// Admin API client. credentials:include — session stored in an httpOnly cookie.
// In dev, Vite proxies /admin → :8003; in prod the admin server serves the static assets itself (same origin).

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
