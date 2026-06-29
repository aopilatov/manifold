// E2E события жизненного цикла + метрики.
import http from "node:http";
import crypto from "node:crypto";
import { SocketClient } from "../dist/index.js";

const SECRET = "dev-secret";
const WS_URL = "ws://127.0.0.1:18000/connection/websocket";
const METRICS = "http://127.0.0.1:18004/metrics";

const fail = (m) => {
  console.error("E2E EVENTS FAIL:", m);
  process.exit(1);
};

// приёмник вебхуков
const received = [];
const receiver = http.createServer((req, res) => {
  let body = "";
  req.on("data", (c) => (body += c));
  req.on("end", () => {
    try {
      received.push(JSON.parse(body));
    } catch {}
    res.statusCode = 200;
    res.end("ok");
  });
});
await new Promise((r) => receiver.listen(9999, r));
console.log("webhook receiver up on :9999");

const b64url = (s) => Buffer.from(s).toString("base64url");
function mintJwt(payload, secret) {
  const head = b64url(JSON.stringify({ alg: "HS256", typ: "JWT" }));
  const body = b64url(JSON.stringify(payload));
  const data = `${head}.${body}`;
  return `${data}.${crypto.createHmac("sha256", secret).update(data).digest("base64url")}`;
}
const token = mintJwt(
  { sub: "u-ev", aud: "socket", channels: [{ match: "chat:room:*", allow: ["sub", "presence"] }] },
  SECRET,
);

const client = new SocketClient({ url: WS_URL, getToken: async () => token });
await client.connect().catch((e) => fail("connect: " + e.message));
const sub = client.newSubscription("chat:room:1");
await sub.subscribe().catch((e) => fail("subscribe: " + e.message));
await sub.unsubscribe().catch((e) => fail("unsubscribe: " + e.message));
client.disconnect();

await new Promise((r) => setTimeout(r, 1000)); // дать вебхукам долететь

const kinds = received.map((e) => e.type);
console.log("получены события:", kinds);
for (const k of ["connected", "subscribed", "unsubscribed", "disconnected"]) {
  if (!kinds.includes(k)) fail("нет события: " + k);
}

const metrics = await fetch(METRICS).then((r) => r.text());
const lines = metrics.split("\n").filter((l) => l.startsWith("socket_") && !l.startsWith("#"));
console.log("метрики:\n" + lines.join("\n"));
if (!metrics.includes("socket_subscriptions_total")) fail("нет метрики subscriptions");
if (!metrics.includes("socket_connections_opened_total")) fail("нет метрики connections_opened");

receiver.close();
console.log("E2E EVENTS OK");
process.exit(0);
